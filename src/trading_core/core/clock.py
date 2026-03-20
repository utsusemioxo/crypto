from __future__ import annotations
import time


def monotonic_ns() -> int:
    """Monotonic clock in ns (for latency measurements)"""
    return time.monotonic_ns()


def wall_ns() -> int:
    """Wall clock in ns (for correlating with logs)"""
    return time.time_ns()
