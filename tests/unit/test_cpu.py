import unittest
from unittest.mock import mock_open, patch

from src.gtfs_rt_aggregator.utils import cpu


class TestAvailableCpus(unittest.TestCase):
    def _with(self, cpu_max, affinity=20):
        def fake_open(path, *args, **kwargs):
            if path == "/sys/fs/cgroup/cpu.max":
                return mock_open(read_data=cpu_max)()
            raise OSError(path)

        with patch("builtins.open", fake_open), patch.object(
            cpu.os, "sched_getaffinity", return_value=set(range(affinity)), create=True
        ):
            return cpu.available_cpus()

    def test_quota(self):
        self.assertEqual(self._with("200000 100000\n"), 2)
        self.assertEqual(self._with("150000 100000\n"), 2)  # 1.5 CPUs
        self.assertEqual(self._with("50000 100000\n"), 1)

    def test_unlimited(self):
        self.assertEqual(self._with("max 100000\n"), 20)
        self.assertEqual(self._with("max 100000\n", affinity=4), 4)
