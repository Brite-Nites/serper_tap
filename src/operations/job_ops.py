"""Job lifecycle operations for BigQuery.

This module handles job CRUD operations:
- create_job: Insert new jobs
- get_job_status: Retrieve job metadata
- get_job_stats: Retrieve rollup statistics
- update_job_stats: Recalculate aggregated statistics
- mark_job_done: Mark job as completed
- get_running_jobs: List active jobs
- get_zips_for_state: Reference data for job planning
"""

from datetime import UTC, datetime
from typing import Any

from google.cloud import bigquery

from src.models.schemas import JobParams
from src.utils.bigquery_client import execute_dml, execute_query
from src.utils.config import settings


def create_job(job_id: str, params: JobParams) -> dict[str, Any]:
    """Create a new scraping job in the serper_jobs table.

    Args:
        job_id: Unique identifier for the job (UUID recommended)
        params: Validated job parameters

    Returns:
        Dict containing job_id and created_at timestamp

    Raises:
        google.cloud.exceptions.GoogleCloudError: If insert fails
    """
    query = f"""
    INSERT INTO `{settings.bigquery_project_id}.{settings.bigquery_dataset}.serper_jobs`
    (job_id, keyword, state, pages, dry_run, batch_size, concurrency, status, created_at, started_at, totals)
    VALUES (
        @job_id,
        @keyword,
        @state,
        @pages,
        @dry_run,
        @batch_size,
        @concurrency,
        'running',
        CURRENT_TIMESTAMP(),
        CURRENT_TIMESTAMP(),
        STRUCT(0 AS zips, 0 AS queries, 0 AS successes, 0 AS failures, 0 AS places, 0 AS credits)
    )
    """

    parameters = [
        bigquery.ScalarQueryParameter("job_id", "STRING", job_id),
        bigquery.ScalarQueryParameter("keyword", "STRING", params.keyword),
        bigquery.ScalarQueryParameter("state", "STRING", params.state),
        bigquery.ScalarQueryParameter("pages", "INT64", params.pages),
        bigquery.ScalarQueryParameter("dry_run", "BOOL", params.dry_run),
        bigquery.ScalarQueryParameter("batch_size", "INT64", params.batch_size),
        bigquery.ScalarQueryParameter("concurrency", "INT64", params.concurrency),
    ]

    execute_dml(query, parameters)

    return {
        "job_id": job_id,
        "status": "running",
        "created_at": datetime.now(UTC).isoformat()
    }


def get_zips_for_state(state: str) -> list[str]:
    """Retrieve all zip codes for a given state from reference table.

    Args:
        state: Two-letter state code (e.g., 'AZ', 'CA')

    Returns:
        List of zip code strings

    Raises:
        google.cloud.exceptions.GoogleCloudError: If query fails
    """
    query = f"""
    SELECT DISTINCT zip
    FROM `{settings.bigquery_project_id}.reference.geo_zip_all`
    WHERE state = @state
    ORDER BY zip
    """

    parameters = [
        bigquery.ScalarQueryParameter("state", "STRING", state.upper())
    ]

    results = execute_query(query, parameters)
    return [row.zip for row in results]


def update_job_stats(job_id: str) -> dict[str, int]:
    """Recalculate and update rollup statistics for a job.

    Aggregates data from serper_queries and serper_places tables to update
    the totals STRUCT in serper_jobs.

    Args:
        job_id: Job identifier

    Returns:
        Dict containing updated statistics

    Raises:
        google.cloud.exceptions.GoogleCloudError: If update fails
    """
    update_query = f"""
    UPDATE `{settings.bigquery_project_id}.{settings.bigquery_dataset}.serper_jobs`
    SET totals = STRUCT(
        (SELECT COUNT(DISTINCT zip) FROM `{settings.bigquery_project_id}.{settings.bigquery_dataset}.serper_queries` WHERE job_id = @job_id) AS zips,
        (SELECT COUNT(*) FROM `{settings.bigquery_project_id}.{settings.bigquery_dataset}.serper_queries` WHERE job_id = @job_id) AS queries,
        (SELECT COUNT(*) FROM `{settings.bigquery_project_id}.{settings.bigquery_dataset}.serper_queries` WHERE job_id = @job_id AND status = 'success') AS successes,
        (SELECT COUNT(*) FROM `{settings.bigquery_project_id}.{settings.bigquery_dataset}.serper_queries` WHERE job_id = @job_id AND status = 'failed') AS failures,
        (SELECT COUNT(*) FROM `{settings.bigquery_project_id}.{settings.bigquery_dataset}.serper_places` WHERE job_id = @job_id) AS places,
        (SELECT COALESCE(SUM(credits), 0) FROM `{settings.bigquery_project_id}.{settings.bigquery_dataset}.serper_queries` WHERE job_id = @job_id) AS credits
    )
    WHERE job_id = @job_id
    """

    parameters = [
        bigquery.ScalarQueryParameter("job_id", "STRING", job_id)
    ]

    execute_dml(update_query, parameters)

    # Return the updated stats
    return get_job_stats(job_id)


def get_job_stats(job_id: str) -> dict[str, int]:
    """Retrieve current statistics for a job.

    Args:
        job_id: Job identifier

    Returns:
        Dict containing job statistics

    Raises:
        google.cloud.exceptions.GoogleCloudError: If query fails
    """
    query = f"""
    SELECT totals
    FROM `{settings.bigquery_project_id}.{settings.bigquery_dataset}.serper_jobs`
    WHERE job_id = @job_id
    """

    parameters = [
        bigquery.ScalarQueryParameter("job_id", "STRING", job_id)
    ]

    results = execute_query(query, parameters)
    row = next(iter(results), None)

    if not row:
        raise ValueError(f"Job not found: {job_id}")

    totals = row.totals
    return {
        "zips": totals["zips"] if totals else 0,
        "queries": totals["queries"] if totals else 0,
        "successes": totals["successes"] if totals else 0,
        "failures": totals["failures"] if totals else 0,
        "places": totals["places"] if totals else 0,
        "credits": totals["credits"] if totals else 0,
    }


def get_job_status(job_id: str) -> dict[str, Any]:
    """Retrieve complete status information for a job.

    Args:
        job_id: Job identifier

    Returns:
        Dict containing job metadata and statistics

    Raises:
        google.cloud.exceptions.GoogleCloudError: If query fails
        ValueError: If job not found
    """
    query = f"""
    SELECT
        job_id,
        keyword,
        state,
        pages,
        dry_run,
        concurrency,
        status,
        created_at,
        started_at,
        finished_at,
        totals
    FROM `{settings.bigquery_project_id}.{settings.bigquery_dataset}.serper_jobs`
    WHERE job_id = @job_id
    """

    parameters = [
        bigquery.ScalarQueryParameter("job_id", "STRING", job_id)
    ]

    results = execute_query(query, parameters)
    row = next(iter(results), None)

    if not row:
        raise ValueError(f"Job not found: {job_id}")

    totals = row.totals
    return {
        "job_id": row.job_id,
        "keyword": row.keyword,
        "state": row.state,
        "pages": row.pages,
        "dry_run": row.dry_run,
        "concurrency": row.concurrency,
        "status": row.status,
        "created_at": row.created_at.isoformat() if row.created_at else None,
        "started_at": row.started_at.isoformat() if row.started_at else None,
        "finished_at": row.finished_at.isoformat() if row.finished_at else None,
        "totals": {
            "zips": totals["zips"] if totals else 0,
            "queries": totals["queries"] if totals else 0,
            "successes": totals["successes"] if totals else 0,
            "failures": totals["failures"] if totals else 0,
            "places": totals["places"] if totals else 0,
            "credits": totals["credits"] if totals else 0,
        }
    }


def get_running_jobs() -> list[dict[str, Any]]:
    """Retrieve all jobs with status='running'.

    Returns job metadata needed for batch processing.

    Returns:
        List of job dicts with keys:
        - job_id, keyword, state, pages, batch_size, concurrency

    Raises:
        google.cloud.exceptions.GoogleCloudError: If query fails
    """
    query = f"""
    SELECT
        job_id,
        keyword,
        state,
        pages,
        batch_size,
        concurrency
    FROM `{settings.bigquery_project_id}.{settings.bigquery_dataset}.serper_jobs`
    WHERE status = 'running'
    ORDER BY created_at ASC
    """

    results = execute_query(query, parameters=None)

    jobs = []
    for row in results:
        jobs.append({
            "job_id": row.job_id,
            "keyword": row.keyword,
            "state": row.state,
            "pages": row.pages,
            "batch_size": row.batch_size,
            "concurrency": row.concurrency,
        })

    return jobs


def mark_job_done(job_id: str) -> None:
    """Mark a job as completed.

    Updates job status to 'done' and sets finished_at timestamp.

    Args:
        job_id: Job identifier

    Raises:
        google.cloud.exceptions.GoogleCloudError: If update fails
    """
    update_query = f"""
    UPDATE `{settings.bigquery_project_id}.{settings.bigquery_dataset}.serper_jobs`
    SET
        status = 'done',
        finished_at = CURRENT_TIMESTAMP()
    WHERE job_id = @job_id
    """

    parameters = [
        bigquery.ScalarQueryParameter("job_id", "STRING", job_id)
    ]

    execute_dml(update_query, parameters)
