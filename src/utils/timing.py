"""Performance timing utilities for measuring bottlenecks.

Provides a context manager for timing operations and logging results.
"""

import time
from contextlib import contextmanager
from typing import Optional

from prefect import get_run_logger


@contextmanager
def timing(operation: str, log_threshold_ms: Optional[float] = None):
    """Context manager for timing operations.

    Usage:
        with timing("BigQuery MERGE"):
            execute_dml(query, params)

        # Only log if operation takes >100ms
        with timing("Fast operation", log_threshold_ms=100):
            quick_task()

    Args:
        operation: Human-readable description of the operation
        log_threshold_ms: Only log if operation takes longer than this (ms)
                         If None, always log

    Yields:
        Dict with elapsed_ms key (updated after context exits)
    """
    logger = get_run_logger()
    start_time = time.time()
    timing_data = {"elapsed_ms": 0}

    try:
        yield timing_data
    finally:
        elapsed_seconds = time.time() - start_time
        elapsed_ms = elapsed_seconds * 1000
        timing_data["elapsed_ms"] = elapsed_ms

        # Decide whether to log based on threshold
        should_log = (log_threshold_ms is None) or (elapsed_ms >= log_threshold_ms)

        if should_log:
            if elapsed_ms < 1000:
                logger.info(f"⏱️  {operation}: {elapsed_ms:.1f}ms")
            else:
                logger.info(f"⏱️  {operation}: {elapsed_seconds:.2f}s")
