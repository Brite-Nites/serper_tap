"""BigQuery operations - public API.

This module re-exports all operations from the split modules to maintain
backward compatibility. Import from here for the public API.

Operations are organized internally into three modules:
- job_ops: Job lifecycle (create, status, stats, completion)
- query_ops: Query queue management (enqueue, dequeue, update)
- place_ops: Place storage (MERGE-based persistence)
"""

# Job operations
from src.operations.job_ops import (
    create_job,
    get_job_stats,
    get_job_status,
    get_running_jobs,
    get_zips_for_state,
    mark_job_done,
    update_job_stats,
)

# Place operations
from src.operations.place_ops import store_places

# Query operations
from src.operations.query_ops import (
    batch_skip_remaining_pages,
    batch_update_query_statuses,
    dequeue_batch,
    enqueue_queries,
    reset_batch_to_queued,
    skip_remaining_pages,
    update_query_status,
)

__all__ = [
    # Job operations
    "create_job",
    "get_job_stats",
    "get_job_status",
    "get_running_jobs",
    "get_zips_for_state",
    "mark_job_done",
    "update_job_stats",
    # Query operations
    "batch_skip_remaining_pages",
    "batch_update_query_statuses",
    "dequeue_batch",
    "enqueue_queries",
    "reset_batch_to_queued",
    "skip_remaining_pages",
    "update_query_status",
    # Place operations
    "store_places",
]
