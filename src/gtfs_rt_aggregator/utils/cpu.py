import math
import os


def available_cpus() -> int:
    """
    CPUs this process may use: the CPU affinity, and the cgroup quota set by
    containers (docker --cpus, Kubernetes limits), which os.cpu_count() ignores.
    """
    try:
        count = len(os.sched_getaffinity(0))
    except AttributeError:  # not Linux
        count = os.cpu_count() or 1
    quota = _cgroup_quota()
    if quota is not None:
        count = min(count, max(1, math.ceil(quota)))
    return max(1, count)


def _cgroup_quota():
    """CPU quota in CPUs, or None if unlimited or unknown."""
    try:  # cgroup v2
        with open("/sys/fs/cgroup/cpu.max") as f:
            quota, period = f.read().split()[:2]
        return None if quota == "max" else int(quota) / int(period)
    except (OSError, ValueError):
        pass
    try:  # cgroup v1
        with open("/sys/fs/cgroup/cpu/cpu.cfs_quota_us") as f:
            quota = int(f.read())
        with open("/sys/fs/cgroup/cpu/cpu.cfs_period_us") as f:
            period = int(f.read())
        return None if quota <= 0 else quota / period
    except (OSError, ValueError):
        return None
