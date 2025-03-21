import logging
import multiprocessing
import time
from typing import List, Tuple, Callable, Dict, Any

import schedule

from ..utils import setup_logger


class SchedulerClass:
    """
    A class to handle scheduling and process management for services.
    """

    def __init__(self, lifecycle_callback: List[Tuple[str, Callable]] = None):
        """Initialize the scheduler."""
        self.scheduler = schedule.Scheduler()
        self.processes = []
        self.running = False
        self.logger = setup_logger(
            f"{self.__class__.__module__}.{self.__class__.__name__}"
        )
        self.lifecycle_callback = lifecycle_callback or []

    def add_schedules(self, schedules: List[Tuple[int, Callable, str, Dict[str, Any]]]):
        """
        Add schedules to the scheduler.

        Args:
            schedules: List of tuples containing (schedule job, function, arguments)
        """
        for seconds, func, name, args in schedules:
            self.scheduler.every(seconds).seconds.do(
                self._run_job_in_process, func=func, **args
            )
            self.logger.info(f"Added schedule for {name} every {seconds} seconds")

    def _run_job_in_process(self, func: Callable, **kwargs):
        """
        Run a job in a separate process.

        Args:
            func: Function to run
            **kwargs: Arguments to pass to the function
        """
        # Clean up completed processes before starting a new one
        self._cleanup_processes()

        # Create a new process for the job
        process = multiprocessing.Process(target=func, kwargs=kwargs)
        process.start()

        # Add to the list of processes
        self.processes.append(process)

        logging.info(f"Launched process for {func.__name__} with args {kwargs}")

    def _cleanup_processes(self):
        """Clean up completed processes."""
        self.processes = [p for p in self.processes if p.is_alive()]

    def tick(self):
        """
        Run pending jobs and clean up completed processes.
        This method should be called periodically to execute scheduled jobs.
        """
        self.scheduler.run_pending()
        self._cleanup_processes()

    def start(self):
        """Start the scheduler."""
        self.running = True
        try:
            while self.running:
                self.tick()
                time.sleep(1)
                for name, callback in self.lifecycle_callback:
                    self.logger.info(f"Running lifecycle callback {name}")
                    should_continue = callback()
                    if not should_continue:
                        self.stop()
                        break

        except KeyboardInterrupt:
            print("Shutting down scheduler...")
            self.stop()

    def stop(self):
        """Stop the scheduler."""
        self.running = False

        # Clear all scheduled jobs
        self.scheduler.clear()

        # Terminate all processes
        for process in self.processes:
            if process.is_alive():
                process.terminate()
                process.join(timeout=5)

        # Clean up the process list
        self.processes = []
