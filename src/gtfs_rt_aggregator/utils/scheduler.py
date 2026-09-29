import logging
import multiprocessing
import os
import signal
import sys
import time
from typing import List, Tuple, Callable, Dict, Any, Optional

import schedule

from ..utils import setup_logger


def _peak_memory_mb() -> float:
    """Peak resident memory of this process, in MB."""
    import resource

    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    # Kilobytes on Linux, bytes on macOS
    return peak / (1024 * 1024 if sys.platform == "darwin" else 1024)


def _run_job(func: Callable, label: str, kwargs: Dict[str, Any]):
    """Job process body: run func, then log how long it took and its peak memory."""
    start = time.monotonic()
    try:
        func(**kwargs)
    finally:
        try:
            logging.getLogger(__name__).info(
                f"Job done: {label} in {time.monotonic() - start:.1f}s, "
                f"peak memory {_peak_memory_mb():.0f} MB"
            )
        except Exception:
            pass


class SchedulerClass:
    """
    A class to handle scheduling and process management for services.
    """

    def __init__(self, lifecycle_callback: List[Tuple[str, Callable]] = None):
        """Initialize the scheduler."""
        self.scheduler = schedule.Scheduler()
        self.processes = []
        self.exclusive_processes = {}
        self.running = False
        self.logger = setup_logger(
            f"{self.__class__.__module__}.{self.__class__.__name__}"
        )
        self.lifecycle_callback = lifecycle_callback or []

    def add_schedules(self, schedules: List[Tuple[int, Callable, str, Dict[str, Any]]]):
        """
        Add schedules to the scheduler.

        Args:
            schedules: List of (interval in seconds, function, name, arguments) tuples,
                optionally followed by True for a job that must not overlap itself
        """
        for seconds, func, name, args, *options in schedules:
            exclusive = bool(options and options[0])
            self.scheduler.every(seconds).seconds.do(
                self._run_job_in_process,
                func=func,
                job_name=name if exclusive else None,
                job_label=name,
                **args,
            )
            self.logger.info(f"Added schedule for {name} every {seconds} seconds")

    def _run_job_in_process(
        self,
        func: Callable,
        job_name: Optional[str] = None,
        job_label: Optional[str] = None,
        **kwargs,
    ):
        """
        Run a job in a separate process.

        Args:
            func: Function to run
            job_name: Set for exclusive jobs: skipped while their previous run is alive
            job_label: Name of the job in logs (kind, provider, feed)
            **kwargs: Arguments to pass to the function
        """
        # Clean up completed processes before starting a new one
        self._cleanup_processes()

        if job_name is not None:
            previous = self.exclusive_processes.get(job_name)
            if previous is not None and previous.is_alive():
                self.logger.warning(f"Skipping {job_name}: previous run still running")
                return

        # Create a new process for the job
        process = multiprocessing.Process(
            target=_run_job,
            args=(func, job_label or getattr(func, "__name__", "job"), kwargs),
        )
        process.start()

        # Add to the list of processes
        self.processes.append(process)
        if job_name is not None:
            self.exclusive_processes[job_name] = process

        # Headers often carry an API key: keep them out of the logs
        logged = {
            k: (
                "***"
                if k == "headers" and v
                # A query string may hold an API key too
                else v.split("?")[0] + "?***" if k == "url" and v and "?" in v else v
            )
            for k, v in kwargs.items()
        }
        logging.info(f"Launched process for {func.__name__} with args {logged}")

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
        """Start the scheduler. Stops cleanly on Ctrl+C and on SIGTERM."""
        self.running = True
        previous_handler = self._handle_sigterm()
        try:
            # Run every job once right away instead of waiting a full interval
            # (a static feed checked daily would otherwise first run after a day)
            self.scheduler.run_all()
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
        finally:
            if previous_handler is not None:
                signal.signal(signal.SIGTERM, previous_handler)

    @staticmethod
    def _handle_sigterm():
        """
        Make SIGTERM (sent by systemd, Docker, Kubernetes...) stop the scheduler
        like Ctrl+C, so buffered data is written. Job processes forked later
        inherit the handler: in them, SIGTERM keeps its default effect (exit).

        Returns the previous handler, or None if it could not be changed (not
        the main thread).
        """
        main_pid = os.getpid()

        def handler(signum, frame):
            if os.getpid() != main_pid:
                signal.signal(signal.SIGTERM, signal.SIG_DFL)
                os.kill(os.getpid(), signal.SIGTERM)
                return
            raise KeyboardInterrupt

        try:
            return signal.signal(signal.SIGTERM, handler)
        except ValueError:
            return None

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
        self.exclusive_processes = {}
