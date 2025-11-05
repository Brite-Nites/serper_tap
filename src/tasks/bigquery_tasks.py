"""Prefect task wrappers for BigQuery operations.

These tasks are thin wrappers around operations layer functions that add
Prefect's retry logic and orchestration capabilities. All business logic
remains in the operations layer (job_ops, query_ops, place_ops).
"""

from typing import Any

from prefect import task

from src.models.schemas import JobParams
from src.operations.job_ops import (
    create_job,
    get_job_status,
    get_running_jobs,
    get_zips_for_state,
    mark_job_done,
    update_job_stats,
)
from src.operations.place_ops import store_places
from src.operations.query_ops import (
    batch_skip_remaining_pages,
    batch_update_query_statuses,
    dequeue_batch,
    enqueue_queries,
    reset_batch_to_queued,
    skip_remaining_pages,
    update_query_status,
)


@task(retries=3, retry_delay_seconds=5)
def create_job_task(job_id: str, params: JobParams) -> dict[str, Any]:
    """Create a new scraping job in BigQuery.

    Args:
        job_id: Unique job identifier
        params: Validated job parameters

    Returns:
        Dict with job_id, status, and created_at
    """
    return create_job(job_id, params)


@task(retries=3, retry_delay_seconds=5)
def get_zips_for_state_task(state: str) -> list[str]:
    """Retrieve zip codes for a state from reference table.

    Args:
        state: Two-letter state code

    Returns:
        List of zip code strings
    """
    return get_zips_for_state(state)


@task(retries=3, retry_delay_seconds=5)
def enqueue_queries_task(job_id: str, queries: list[dict[str, Any]]) -> int:
    """Enqueue queries for a job using idempotent MERGE.

    Args:
        job_id: Job identifier
        queries: List of query dicts

    Returns:
        Number of new queries inserted
    """
    return enqueue_queries(job_id, queries)


@task(retries=3, retry_delay_seconds=5)
def dequeue_batch_task(job_id: str, batch_size: int) -> list[dict[str, Any]]:
    """Atomically dequeue a batch of queries using claim_id pattern.

    Args:
        job_id: Job identifier
        batch_size: Maximum queries to claim

    Returns:
        List of query dicts with keys: zip, page, q
    """
    return dequeue_batch(job_id, batch_size)


@task(retries=3, retry_delay_seconds=5)
def store_places_task(job_id: str, places: list[dict[str, Any]]) -> int:
    """Store scraped places using idempotent MERGE.

    Args:
        job_id: Job identifier
        places: List of place dicts

    Returns:
        Number of new places inserted
    """
    return store_places(job_id, places)


@task(retries=3, retry_delay_seconds=5)
def update_query_status_task(
    job_id: str,
    zip_code: str,
    page: int,
    status: str,
    api_status: int | None = None,
    results_count: int | None = None,
    credits: int | None = None,
    error: str | None = None
) -> None:
    """Update status and metadata for a specific query.

    Args:
        job_id: Job identifier
        zip_code: Zip code
        page: Page number
        status: New status (success, failed, etc.)
        api_status: HTTP status code
        results_count: Number of results
        credits: API credits consumed
        error: Error message if failed
    """
    update_query_status(
        job_id=job_id,
        zip_code=zip_code,
        page=page,
        status=status,
        api_status=api_status,
        results_count=results_count,
        credits=credits,
        error=error
    )


@task(retries=3, retry_delay_seconds=5)
def update_job_stats_task(job_id: str) -> dict[str, int]:
    """Recalculate and update rollup statistics for a job.

    Args:
        job_id: Job identifier

    Returns:
        Dict containing updated statistics
    """
    return update_job_stats(job_id)


@task(retries=3, retry_delay_seconds=5)
def skip_remaining_pages_task(
    job_id: str,
    zip_code: str,
    page: int,
    results_count: int
) -> int:
    """Mark remaining pages as skipped for sparse zip codes.

    Only executes when page==1 AND results_count<10.

    Args:
        job_id: Job identifier
        zip_code: Zip code
        page: Page number just processed
        results_count: Number of results returned

    Returns:
        Number of queries marked as skipped
    """
    return skip_remaining_pages(
        job_id=job_id,
        zip_code=zip_code,
        page=page,
        results_count=results_count
    )


@task(retries=3, retry_delay_seconds=5)
def get_job_status_task(job_id: str) -> dict[str, Any]:
    """Retrieve complete status information for a job.

    Useful for debugging and monitoring during flow execution.

    Args:
        job_id: Job identifier

    Returns:
        Dict containing job metadata and statistics
    """
    return get_job_status(job_id)


@task(retries=3, retry_delay_seconds=5)
def get_running_jobs_task() -> list[dict[str, Any]]:
    """Retrieve all jobs with status='running'.

    Returns job metadata needed for batch processing.

    Returns:
        List of job dicts with keys:
        - job_id, keyword, state, pages, batch_size
    """
    return get_running_jobs()


@task(retries=3, retry_delay_seconds=5)
def mark_job_done_task(job_id: str) -> None:
    """Mark a job as completed.

    Updates job status to 'done' and sets finished_at timestamp.

    Args:
        job_id: Job identifier
    """
    mark_job_done(job_id)


@task(retries=3, retry_delay_seconds=5)
def reset_batch_to_queued_task(claim_id: str) -> int:
    """Reset queries from 'processing' back to 'queued' after batch failure.

    Args:
        claim_id: Claim ID of the failed batch

    Returns:
        Number of queries reset to 'queued' status
    """
    return reset_batch_to_queued(claim_id)


@task(retries=3, retry_delay_seconds=5)
def batch_update_query_statuses_task(
    job_id: str,
    updates: list[dict[str, Any]]
) -> int:
    """Update status and metadata for multiple queries in a single batch operation.

    Batched version of update_query_status_task() for dramatically improved performance.
    Uses MERGE + UNNEST to update all queries in a single database call.

    Args:
        job_id: Job identifier
        updates: List of update dicts, each with keys:
            - zip: Zip code (STRING)
            - page: Page number (INT)
            - status: New status (STRING)
            - api_status: HTTP status code (INT, optional)
            - results_count: Number of results (INT, optional)
            - credits: API credits consumed (INT, optional)
            - error: Error message (STRING, optional)

    Returns:
        Number of rows updated

    Example:
        updates = [
            {"zip": "85001", "page": 1, "status": "success", "api_status": 200,
             "results_count": 10, "credits": 1, "error": None},
            {"zip": "85002", "page": 1, "status": "success", "api_status": 200,
             "results_count": 8, "credits": 1, "error": None},
        ]
        count = batch_update_query_statuses_task(job_id, updates)
    """
    return batch_update_query_statuses(job_id, updates)


@task(retries=3, retry_delay_seconds=5)
def batch_skip_remaining_pages_task(
    job_id: str,
    zips_to_skip: list[str]
) -> int:
    """Skip pages 2-3 for multiple zip codes in a single batch operation.

    Batched version of skip_remaining_pages_task() for improved performance.
    Used when page 1 returns <10 results (early exit optimization).

    Args:
        job_id: Job identifier
        zips_to_skip: List of zip codes where pages 2-3 should be skipped

    Returns:
        Number of rows updated (0-2 per zip code)

    Example:
        zips_to_skip = ["85001", "85002", "85003"]
        count = batch_skip_remaining_pages_task(job_id, zips_to_skip)
    """
    return batch_skip_remaining_pages(job_id, zips_to_skip)
