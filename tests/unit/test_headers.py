import unittest
from unittest.mock import MagicMock, patch

from src.gtfs_rt_aggregator.fetcher.gtfs_rt import GtfsRtFetcher
from src.gtfs_rt_aggregator.utils.scheduler import SchedulerClass


class TestHeaders(unittest.TestCase):
    def test_fetch_feed_sends_headers(self):
        response = MagicMock(content=b"data")
        with patch(
            "src.gtfs_rt_aggregator.fetcher.gtfs_rt.requests.get",
            return_value=response,
        ) as get:
            GtfsRtFetcher.fetch_feed("https://example.org/vp.pb", {"x-api-key": "k"})
        self.assertEqual(get.call_args.kwargs["headers"], {"x-api-key": "k"})

    def test_scheduler_does_not_log_headers(self):
        scheduler = SchedulerClass()
        with (
            patch("src.gtfs_rt_aggregator.utils.scheduler.multiprocessing.Process"),
            self.assertLogs(level="INFO") as logs,
        ):
            scheduler._run_job_in_process(
                func=print, url="https://example.org", headers={"x-api-key": "secret"}
            )
        self.assertFalse(any("secret" in line for line in logs.output))
        self.assertTrue(any("https://example.org" in line for line in logs.output))


class TestExclusiveJobs(unittest.TestCase):
    def test_exclusive_job_skipped_while_running(self):
        scheduler = SchedulerClass()
        with patch(
            "src.gtfs_rt_aggregator.utils.scheduler.multiprocessing.Process"
        ) as process_class:
            process_class.return_value.is_alive.return_value = True
            scheduler._run_job_in_process(func=print, job_name="static nl")
            scheduler._run_job_in_process(func=print, job_name="static nl")
            self.assertEqual(process_class.call_count, 1)

            # Non-exclusive jobs may overlap
            scheduler._run_job_in_process(func=print)
            scheduler._run_job_in_process(func=print)
            self.assertEqual(process_class.call_count, 3)

            process_class.return_value.is_alive.return_value = False
            scheduler._run_job_in_process(func=print, job_name="static nl")
            self.assertEqual(process_class.call_count, 4)

    def test_add_schedules_accepts_exclusive_flag(self):
        scheduler = SchedulerClass()
        scheduler.add_schedules([(60, print, "a", {}), (60, print, "b", {}, True)])
        job_names = [
            job.job_func.keywords["job_name"] for job in scheduler.scheduler.jobs
        ]
        self.assertEqual(job_names, [None, "b"])


class TestFileSystemWrites(unittest.TestCase):
    def test_no_temporary_file_left(self):
        import os
        import tempfile

        from src.gtfs_rt_aggregator.storage.filesystem import FileSystemStorage

        with tempfile.TemporaryDirectory() as directory:
            storage = FileSystemStorage(directory)
            storage.save_bytes(b"one", "a/b.json")
            storage.save_bytes(b"two", "a/b.json")
            self.assertEqual(os.listdir(os.path.join(directory, "a")), ["b.json"])
            self.assertEqual(storage.read_bytes("a/b.json"), b"two")


if __name__ == "__main__":
    unittest.main()
