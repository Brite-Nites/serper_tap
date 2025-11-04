"""BigQuery operations for Serper scraping pipeline.

This module contains plain Python functions (not Prefect tasks) that execute
SQL operations against BigQuery. All operations are designed to be idempotent
using MERGE statements and atomic updates.
"""

import json
import time
import uuid
from datetime import datetime
from typing import Any

from google.cloud import bigquery

from src.models.schemas import JobParams, JobRecord, JobStats
from src.utils.bigquery_client import execute_dml, execute_query, get_bigquery_client
from src.utils.config import settings
from src.utils.timing import timing

# BigQuery MERGE operation limits
# Safe chunk size to avoid hitting parameter limits (10000 params max)
# With ~10 params per row, 500 rows = ~5000 params (50% safety margin)
MERGE_CHUNK_SIZE = 500


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
        "created_at": datetime.utcnow().isoformat()
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


def _enqueue_queries_chunk(job_id: str, queries: list[dict[str, Any]]) -> int:
    """Internal helper: enqueue a single chunk of queries (<=500 rows).

    Args:
        job_id: Job identifier
        queries: List of query dicts (max 500 rows)

    Returns:
        Number of new queries inserted

    Raises:
        google.cloud.exceptions.GoogleCloudError: If MERGE fails
    """
    if not queries:
        return 0

    # Build VALUES clause for all queries
    values_clauses = []
    for i, q in enumerate(queries):
        values_clauses.append(
            f"(@job_id, @zip_{i}, @page_{i}, @q_{i}, 'queued', NULL, NULL, NULL, NULL, NULL, NULL, NULL)"
        )

    values_sql = ",\n        ".join(values_clauses)

    query = f"""
    MERGE `{settings.bigquery_project_id}.{settings.bigquery_dataset}.serper_queries` AS target
    USING (
        SELECT * FROM UNNEST([
            STRUCT<job_id STRING, zip STRING, page INT64, q STRING, status STRING,
                   claim_id STRING, claimed_at TIMESTAMP, api_status INT64,
                   results_count INT64, credits INT64, error STRING, ran_at TIMESTAMP>
            {values_sql}
        ])
    ) AS source
    ON target.job_id = source.job_id
       AND target.zip = source.zip
       AND target.page = source.page
    WHEN NOT MATCHED THEN
        INSERT (job_id, zip, page, q, status, claim_id, claimed_at, api_status, results_count, credits, error, ran_at)
        VALUES (source.job_id, source.zip, source.page, source.q, source.status,
                source.claim_id, source.claimed_at, source.api_status, source.results_count,
                source.credits, source.error, source.ran_at)
    """

    # Build parameters list
    parameters = [bigquery.ScalarQueryParameter("job_id", "STRING", job_id)]
    for i, q in enumerate(queries):
        parameters.extend([
            bigquery.ScalarQueryParameter(f"zip_{i}", "STRING", q["zip"]),
            bigquery.ScalarQueryParameter(f"page_{i}", "INT64", q["page"]),
            bigquery.ScalarQueryParameter(f"q_{i}", "STRING", q["q"]),
        ])

    with timing(f"MERGE enqueue {len(queries)} queries"):
        rows_affected = execute_dml(query, parameters)
    return rows_affected


def enqueue_queries(job_id: str, queries: list[dict[str, Any]]) -> int:
    """Enqueue queries for a job using idempotent MERGE operation.

    This function uses MERGE to ensure that re-running enqueue with the same
    queries won't create duplicates. Primary key is (job_id, zip, page).

    For large query sets (e.g., California: 5,301 queries), this automatically
    chunks the operation into batches of 500 rows to avoid BigQuery parameter
    limits (10,000 params max).

    Args:
        job_id: Job identifier
        queries: List of query dicts with keys: zip, page, q

    Returns:
        Number of new queries inserted (not total queries)

    Raises:
        google.cloud.exceptions.GoogleCloudError: If MERGE fails
    """
    if not queries:
        return 0

    # Chunk large query sets to avoid BigQuery parameter limits
    if len(queries) > MERGE_CHUNK_SIZE:
        total_inserted = 0
        for i in range(0, len(queries), MERGE_CHUNK_SIZE):
            chunk = queries[i:i + MERGE_CHUNK_SIZE]
            inserted = _enqueue_queries_chunk(job_id, chunk)
            total_inserted += inserted
        return total_inserted
    else:
        return _enqueue_queries_chunk(job_id, queries)


def dequeue_batch(job_id: str, batch_size: int) -> list[dict[str, Any]]:
    """Atomically dequeue a batch of queries for processing using claim_id pattern.

    This implements the atomic dequeue pattern from the specification:
    1. Generate unique claim_id
    2. UPDATE queries to 'processing' status with claim_id (atomic)
    3. SELECT only queries with this claim_id

    Even if multiple workers run concurrently, each will claim a different batch
    because BigQuery executes UPDATEs serially.

    Args:
        job_id: Job identifier
        batch_size: Maximum number of queries to claim

    Returns:
        List of query dicts with keys: zip, page, q, claim_id

    Raises:
        google.cloud.exceptions.GoogleCloudError: If operation fails
    """
    # Generate unique claim_id for this dequeue operation
    claim_id = f"claim-{int(time.time())}-{uuid.uuid4().hex[:9]}"

    # Step 1: Atomically claim a batch of queued queries
    update_query = f"""
    UPDATE `{settings.bigquery_project_id}.{settings.bigquery_dataset}.serper_queries`
    SET
        status = 'processing',
        claim_id = @claim_id,
        claimed_at = CURRENT_TIMESTAMP()
    WHERE job_id = @job_id
      AND status = 'queued'
      AND CONCAT(zip, '-', CAST(page AS STRING)) IN (
          SELECT CONCAT(zip, '-', CAST(page AS STRING))
          FROM (
              SELECT zip, page, ROW_NUMBER() OVER (ORDER BY zip, page) AS rn
              FROM `{settings.bigquery_project_id}.{settings.bigquery_dataset}.serper_queries`
              WHERE job_id = @job_id AND status = 'queued'
          )
          WHERE rn <= @batch_size
      )
    """

    update_params = [
        bigquery.ScalarQueryParameter("job_id", "STRING", job_id),
        bigquery.ScalarQueryParameter("claim_id", "STRING", claim_id),
        bigquery.ScalarQueryParameter("batch_size", "INT64", batch_size),
    ]

    with timing(f"Atomic claim batch (size={batch_size})"):
        claimed_count = execute_dml(update_query, update_params)

    if claimed_count == 0:
        return []  # No queued queries found

    # Step 2: SELECT only queries with our claim_id
    select_query = f"""
    SELECT zip, page, q, claim_id
    FROM `{settings.bigquery_project_id}.{settings.bigquery_dataset}.serper_queries`
    WHERE job_id = @job_id AND claim_id = @claim_id
    ORDER BY zip, page
    """

    select_params = [
        bigquery.ScalarQueryParameter("job_id", "STRING", job_id),
        bigquery.ScalarQueryParameter("claim_id", "STRING", claim_id),
    ]

    with timing(f"SELECT claimed queries (expected={claimed_count})"):
        results = execute_query(select_query, select_params)

    return [
        {
            "zip": row.zip,
            "page": row.page,
            "q": row.q,
            "claim_id": row.claim_id,
        }
        for row in results
    ]


def _store_places_chunk(job_id: str, places: list[dict[str, Any]]) -> int:
    """Internal helper: store a single chunk of places (<=500 rows).

    Args:
        job_id: Job identifier
        places: List of place dicts (max 500 rows)

    Returns:
        Number of new places inserted

    Raises:
        google.cloud.exceptions.GoogleCloudError: If MERGE fails
    """
    if not places:
        return 0

    # Build rows for MERGE operation
    rows_to_insert = []
    for place in places:
        row = {
            "ingest_id": place.get("ingest_id", f"{job_id}-{place['place_uid']}-{int(time.time())}"),
            "job_id": job_id,
            "source": "serper_places",
            "source_version": "v1",
            "ingest_ts": place.get("ingest_ts", datetime.utcnow().isoformat()),
            "keyword": place["keyword"],
            "state": place["state"],
            "zip": place["zip"],
            "page": place["page"],
            "place_uid": place["place_uid"],
            "payload": place["payload"],
            "api_status": place.get("api_status"),
            "api_ms": place.get("api_ms"),
            "results_count": place.get("results_count"),
            "credits": place.get("credits"),
            "error": place.get("error"),
        }
        rows_to_insert.append(row)

    # Always use MERGE for guaranteed idempotency (works for all batch sizes)
    values_clauses = []
    for i, _ in enumerate(places):
        values_clauses.append(
            f"(@ingest_id_{i}, @job_id, @source, @source_version, @ingest_ts_{i}, "
            f"@keyword_{i}, @state_{i}, @zip_{i}, @page_{i}, @place_uid_{i}, "
            f"PARSE_JSON(@payload_{i}), @api_status_{i}, @api_ms_{i}, "
            f"@results_count_{i}, @credits_{i}, @error_{i})"
        )

    values_sql = ",\n        ".join(values_clauses)

    merge_query = f"""
    MERGE `{settings.bigquery_project_id}.{settings.bigquery_dataset}.serper_places` AS target
    USING (
        SELECT * FROM UNNEST([
            STRUCT<ingest_id STRING, job_id STRING, source STRING, source_version STRING,
                   ingest_ts TIMESTAMP, keyword STRING, state STRING, zip STRING, page INT64,
                   place_uid STRING, payload JSON, api_status INT64, api_ms INT64,
                   results_count INT64, credits INT64, error STRING>
            {values_sql}
        ])
    ) AS source
    ON target.job_id = source.job_id AND target.place_uid = source.place_uid
    WHEN NOT MATCHED THEN
        INSERT (ingest_id, job_id, source, source_version, ingest_ts, keyword, state,
                zip, page, place_uid, payload, api_status, api_ms, results_count, credits, error)
        VALUES (source.ingest_id, source.job_id, source.source, source.source_version,
                source.ingest_ts, source.keyword, source.state, source.zip, source.page,
                source.place_uid, source.payload, source.api_status, source.api_ms,
                source.results_count, source.credits, source.error)
    """

    # Build parameters
    parameters = [
        bigquery.ScalarQueryParameter("job_id", "STRING", job_id),
        bigquery.ScalarQueryParameter("source", "STRING", "serper_places"),
        bigquery.ScalarQueryParameter("source_version", "STRING", "v1"),
    ]

    for i, place in enumerate(places):
        row = rows_to_insert[i]
        parameters.extend([
            bigquery.ScalarQueryParameter(f"ingest_id_{i}", "STRING", row["ingest_id"]),
            bigquery.ScalarQueryParameter(f"ingest_ts_{i}", "TIMESTAMP", row["ingest_ts"]),
            bigquery.ScalarQueryParameter(f"keyword_{i}", "STRING", row["keyword"]),
            bigquery.ScalarQueryParameter(f"state_{i}", "STRING", row["state"]),
            bigquery.ScalarQueryParameter(f"zip_{i}", "STRING", row["zip"]),
            bigquery.ScalarQueryParameter(f"page_{i}", "INT64", row["page"]),
            bigquery.ScalarQueryParameter(f"place_uid_{i}", "STRING", row["place_uid"]),
            bigquery.ScalarQueryParameter(f"payload_{i}", "STRING", json.dumps(row["payload"])),
            bigquery.ScalarQueryParameter(f"api_status_{i}", "INT64", row.get("api_status")),
            bigquery.ScalarQueryParameter(f"api_ms_{i}", "INT64", row.get("api_ms")),
            bigquery.ScalarQueryParameter(f"results_count_{i}", "INT64", row.get("results_count")),
            bigquery.ScalarQueryParameter(f"credits_{i}", "INT64", row.get("credits")),
            bigquery.ScalarQueryParameter(f"error_{i}", "STRING", row.get("error")),
        ])

    with timing(f"MERGE store {len(places)} places"):
        rows_affected = execute_dml(merge_query, parameters)
    return rows_affected


def store_places(job_id: str, places: list[dict[str, Any]]) -> int:
    """Store scraped places using idempotent MERGE operation.

    MERGE prevents duplicate places based on (job_id, place_uid). If the same
    place is scraped multiple times (e.g., due to retries), only the first
    instance is stored.

    For large place sets, this automatically chunks the operation into batches
    of 500 rows to avoid BigQuery parameter limits (10,000 params max).

    Args:
        job_id: Job identifier
        places: List of place dicts with keys matching PlaceRecord schema

    Returns:
        Number of new places inserted

    Raises:
        google.cloud.exceptions.GoogleCloudError: If MERGE fails
    """
    if not places:
        return 0

    # Chunk large place sets to avoid BigQuery parameter limits
    if len(places) > MERGE_CHUNK_SIZE:
        total_inserted = 0
        for i in range(0, len(places), MERGE_CHUNK_SIZE):
            chunk = places[i:i + MERGE_CHUNK_SIZE]
            inserted = _store_places_chunk(job_id, chunk)
            total_inserted += inserted
        return total_inserted
    else:
        return _store_places_chunk(job_id, places)


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


def skip_remaining_pages(
    job_id: str,
    zip_code: str,
    page: int,
    results_count: int
) -> int:
    """Mark remaining pages (2-3) as skipped when page 1 has sparse results.

    This is a defensive implementation of the early exit optimization.
    It only marks pages as skipped when ALL conditions are met:
    - page == 1 (we just processed the first page)
    - results_count < 10 (sparse results indicate no more pages needed)

    For any other combination of inputs, this is a no-op returning 0.

    Args:
        job_id: Job identifier
        zip_code: Zip code being processed
        page: Page number that was just processed
        results_count: Number of results returned for this page

    Returns:
        Number of queries marked as skipped (0, 1, or 2)

    Raises:
        google.cloud.exceptions.GoogleCloudError: If update fails
    """
    # Defensive check: only skip if page 1 had <10 results
    if page != 1 or results_count >= 10:
        return 0  # No-op for any other case

    # Mark pages 2 and 3 as skipped
    update_query = f"""
    UPDATE `{settings.bigquery_project_id}.{settings.bigquery_dataset}.serper_queries`
    SET
        status = 'skipped',
        error = 'early_exit_page1_lt10',
        ran_at = CURRENT_TIMESTAMP()
    WHERE job_id = @job_id
      AND zip = @zip
      AND page IN (2, 3)
      AND status = 'queued'
    """

    parameters = [
        bigquery.ScalarQueryParameter("job_id", "STRING", job_id),
        bigquery.ScalarQueryParameter("zip", "STRING", zip_code),
    ]

    rows_skipped = execute_dml(update_query, parameters)
    return rows_skipped


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


def update_query_status(
    job_id: str,
    zip_code: str,
    page: int,
    status: str,
    api_status: int | None = None,
    results_count: int | None = None,
    credits: int | None = None,
    error: str | None = None
) -> None:
    """Update the status and metadata for a specific query.

    Args:
        job_id: Job identifier
        zip_code: Zip code
        page: Page number
        status: New status ('success', 'failed', etc.)
        api_status: HTTP status code from API
        results_count: Number of results returned
        credits: API credits consumed
        error: Error message if failed

    Raises:
        google.cloud.exceptions.GoogleCloudError: If update fails
    """
    update_query = f"""
    UPDATE `{settings.bigquery_project_id}.{settings.bigquery_dataset}.serper_queries`
    SET
        status = @status,
        api_status = @api_status,
        results_count = @results_count,
        credits = @credits,
        error = @error,
        ran_at = CURRENT_TIMESTAMP()
    WHERE job_id = @job_id
      AND zip = @zip
      AND page = @page
    """

    parameters = [
        bigquery.ScalarQueryParameter("job_id", "STRING", job_id),
        bigquery.ScalarQueryParameter("zip", "STRING", zip_code),
        bigquery.ScalarQueryParameter("page", "INT64", page),
        bigquery.ScalarQueryParameter("status", "STRING", status),
        bigquery.ScalarQueryParameter("api_status", "INT64", api_status),
        bigquery.ScalarQueryParameter("results_count", "INT64", results_count),
        bigquery.ScalarQueryParameter("credits", "INT64", credits),
        bigquery.ScalarQueryParameter("error", "STRING", error),
    ]

    execute_dml(update_query, parameters)


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


def reset_batch_to_queued(claim_id: str) -> int:
    """Reset queries from 'processing' back to 'queued' after batch failure.

    When a batch fails (e.g., network error, API error), this function allows
    the queries to be retried by resetting them back to 'queued' status and
    clearing the claim_id.

    Args:
        claim_id: Claim ID of the failed batch

    Returns:
        Number of queries reset to 'queued' status

    Raises:
        google.cloud.exceptions.GoogleCloudError: If update fails
    """
    update_query = f"""
    UPDATE `{settings.bigquery_project_id}.{settings.bigquery_dataset}.serper_queries`
    SET
        status = 'queued',
        claim_id = NULL,
        claimed_at = NULL
    WHERE claim_id = @claim_id
      AND status = 'processing'
    """

    parameters = [
        bigquery.ScalarQueryParameter("claim_id", "STRING", claim_id)
    ]

    reset_count = execute_dml(update_query, parameters)
    return reset_count


def batch_update_query_statuses(
    job_id: str,
    updates: list[dict[str, Any]]
) -> int:
    """Update status and metadata for multiple queries in a single batch operation.

    This is a batched version of update_query_status() that uses MERGE + UNNEST
    to update multiple queries in a single database call, dramatically improving
    performance for batch processing.

    Args:
        job_id: Job identifier (same for all updates)
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

    Raises:
        google.cloud.exceptions.GoogleCloudError: If MERGE fails
        ValueError: If updates list is empty

    Example:
        updates = [
            {"zip": "85001", "page": 1, "status": "success", "api_status": 200,
             "results_count": 10, "credits": 1, "error": None},
            {"zip": "85002", "page": 1, "status": "success", "api_status": 200,
             "results_count": 8, "credits": 1, "error": None},
        ]
        count = batch_update_query_statuses(job_id, updates)
    """
    if not updates:
        raise ValueError("updates list cannot be empty")

    # Build values clauses for UNNEST
    values_clauses = []
    for i in range(len(updates)):
        values_clauses.append(
            f"(@job_id, @zip_{i}, @page_{i}, @status_{i}, @api_status_{i}, "
            f"@results_count_{i}, @credits_{i}, @error_{i})"
        )

    values_sql = ",\n            ".join(values_clauses)

    # MERGE query using UNNEST pattern
    merge_query = f"""
    MERGE `{settings.bigquery_project_id}.{settings.bigquery_dataset}.serper_queries` AS target
    USING (
        SELECT * FROM UNNEST([
            STRUCT<job_id STRING, zip STRING, page INT64, status STRING,
                   api_status INT64, results_count INT64, credits INT64, error STRING>
            {values_sql}
        ])
    ) AS source
    ON target.job_id = source.job_id
       AND target.zip = source.zip
       AND target.page = source.page
    WHEN MATCHED THEN
        UPDATE SET
            status = source.status,
            api_status = source.api_status,
            results_count = source.results_count,
            credits = source.credits,
            error = source.error,
            ran_at = CURRENT_TIMESTAMP()
    """

    # Build parameters
    parameters = [
        bigquery.ScalarQueryParameter("job_id", "STRING", job_id)
    ]

    for i, update in enumerate(updates):
        parameters.extend([
            bigquery.ScalarQueryParameter(f"zip_{i}", "STRING", update["zip"]),
            bigquery.ScalarQueryParameter(f"page_{i}", "INT64", update["page"]),
            bigquery.ScalarQueryParameter(f"status_{i}", "STRING", update["status"]),
            bigquery.ScalarQueryParameter(f"api_status_{i}", "INT64", update.get("api_status")),
            bigquery.ScalarQueryParameter(f"results_count_{i}", "INT64", update.get("results_count")),
            bigquery.ScalarQueryParameter(f"credits_{i}", "INT64", update.get("credits")),
            bigquery.ScalarQueryParameter(f"error_{i}", "STRING", update.get("error")),
        ])

    with timing(f"MERGE batch update {len(updates)} query statuses"):
        rows_updated = execute_dml(merge_query, parameters)
    return rows_updated


def batch_skip_remaining_pages(
    job_id: str,
    zips_to_skip: list[str]
) -> int:
    """Skip pages 2-3 for multiple zip codes in a single batch operation.

    This is a batched version of skip_remaining_pages() that uses MERGE + UNNEST
    to update multiple queries in a single database call. Used when page 1 returns
    <10 results, indicating sparse area (early exit optimization).

    Args:
        job_id: Job identifier
        zips_to_skip: List of zip codes where pages 2-3 should be skipped

    Returns:
        Number of rows updated (0-2 per zip code)

    Raises:
        google.cloud.exceptions.GoogleCloudError: If MERGE fails
        ValueError: If zips_to_skip list is empty

    Example:
        zips_to_skip = ["85001", "85002", "85003"]
        count = batch_skip_remaining_pages(job_id, zips_to_skip)
        # Updates up to 6 rows (2 pages × 3 zips)
    """
    if not zips_to_skip:
        raise ValueError("zips_to_skip list cannot be empty")

    # Build values clauses for UNNEST - need entries for pages 2 and 3 for each zip
    values_clauses = []
    for i, zip_code in enumerate(zips_to_skip):
        # Add entries for page 2 and page 3
        values_clauses.append(f"(@job_id, @zip_{i}, 2)")
        values_clauses.append(f"(@job_id, @zip_{i}, 3)")

    values_sql = ",\n            ".join(values_clauses)

    # MERGE query using UNNEST pattern
    merge_query = f"""
    MERGE `{settings.bigquery_project_id}.{settings.bigquery_dataset}.serper_queries` AS target
    USING (
        SELECT * FROM UNNEST([
            STRUCT<job_id STRING, zip STRING, page INT64>
            {values_sql}
        ])
    ) AS source
    ON target.job_id = source.job_id
       AND target.zip = source.zip
       AND target.page = source.page
       AND target.status = 'queued'
    WHEN MATCHED THEN
        UPDATE SET
            status = 'skipped',
            error = 'early_exit_page1_lt10',
            ran_at = CURRENT_TIMESTAMP()
    """

    # Build parameters
    parameters = [
        bigquery.ScalarQueryParameter("job_id", "STRING", job_id)
    ]

    for i, zip_code in enumerate(zips_to_skip):
        parameters.append(
            bigquery.ScalarQueryParameter(f"zip_{i}", "STRING", zip_code)
        )

    with timing(f"MERGE batch skip {len(zips_to_skip)} zips (pages 2-3)"):
        rows_updated = execute_dml(merge_query, parameters)
    return rows_updated
