"""DEPRECATED: Compatibility shim for bigquery_ops.

⚠️  DEPRECATION NOTICE ⚠️
This module is deprecated and will be removed on 2026-02-01.
Migrate all imports to use the new focused modules.

This module has been split into focused modules for better maintainability:
- job_ops.py: Job lifecycle operations (create_job, get_job_status, mark_job_done, etc.)
- query_ops.py: Query queue management (enqueue_queries, dequeue_batch, update_query_status, etc.)
- place_ops.py: Place storage operations (store_places)

MIGRATION GUIDE:
Old (deprecated):
    from src.operations.bigquery_ops import create_job, dequeue_batch, store_places

New (preferred):
    from src.operations import create_job, dequeue_batch, store_places

All functions are re-exported from src.operations.__init__ for backward compatibility.

REMOVAL DATE: 2026-02-01
GitHub Issue: See issue #1 or CONTRIBUTING.md for migration guidance
"""

import warnings

# Re-export everything from the public API
from src.operations import (
    batch_skip_remaining_pages,
    batch_update_query_statuses,
    create_job,
    dequeue_batch,
    enqueue_queries,
    get_job_stats,
    get_job_status,
    get_running_jobs,
    get_zips_for_state,
    mark_job_done,
    reset_batch_to_queued,
    skip_remaining_pages,
    store_places,
    update_job_stats,
    update_query_status,
)

# Emit deprecation warning when this module is imported
warnings.warn(
    "Direct import from src.operations.bigquery_ops is deprecated. "
    "Use 'from src.operations import <function>' instead. "
    "This compatibility shim will be removed in a future version.",
    DeprecationWarning,
    stacklevel=2
)

__all__ = [
    "batch_skip_remaining_pages",
    "batch_update_query_statuses",
    "create_job",
    "dequeue_batch",
    "enqueue_queries",
    "get_job_stats",
    "get_job_status",
    "get_running_jobs",
    "get_zips_for_state",
    "mark_job_done",
    "reset_batch_to_queued",
    "skip_remaining_pages",
    "store_places",
    "update_job_stats",
    "update_query_status",
]
