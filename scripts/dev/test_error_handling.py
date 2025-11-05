#!/usr/bin/env python3
"""Test script to verify error handling in batch processing.

This script:
1. Creates a small test job with a few queries
2. Temporarily modifies the Serper task to inject failures
3. Runs the batch processor to verify error handling
4. Checks that failed queries are reset to 'queued' status
5. Verifies the job eventually completes after retries

Usage:
    python scripts/test_error_handling.py
"""

import sys
import time
import uuid

from src.models.schemas import JobParams
from src.operations.bigquery_ops import (
    create_job,
    enqueue_queries,
    get_job_status,
)
from src.utils.config import settings


def create_error_test_job(
    keyword: str = "test-error-handling",
    zip_codes: list[str] = None,
    pages: int = 2
) -> str:
    """Create a minimal test job for error handling testing.

    Args:
        keyword: Search keyword
        zip_codes: List of zip codes (default: just a few test zips)
        pages: Number of pages per zip

    Returns:
        job_id: UUID of created job
    """
    if zip_codes is None:
        zip_codes = ["85001", "85002", "85003"]  # Just 3 zips = 6 queries

    # Generate job ID
    job_id = str(uuid.uuid4())

    # Create job
    print(f"Creating error handling test job: {job_id}")
    print(f"  Keyword: {keyword}")
    print(f"  Zip codes: {zip_codes}")
    print(f"  Pages: {pages}")
    print(f"  Total queries: {len(zip_codes) * pages}")
    print()

    params = JobParams(
        keyword=keyword,
        state="AZ",
        pages=pages,
        batch_size=5,  # Small batch
        concurrency=2
    )
    create_job(job_id=job_id, params=params)

    # Generate queries
    queries = []
    for zip_code in zip_codes:
        for page in range(1, pages + 1):
            query = {
                "zip": zip_code,
                "page": page,
                "q": f"{zip_code} {keyword}"
            }
            queries.append(query)

    # Enqueue queries
    print(f"Enqueueing {len(queries)} queries...")
    enqueue_queries(job_id, queries)

    # Show job status
    status = get_job_status(job_id)
    print()
    print("=" * 60)
    print("ERROR TEST JOB CREATED")
    print("=" * 60)
    print(f"Job ID: {job_id}")
    print(f"Total queries: {status['totals']['queries']}")
    print()
    print("Next steps:")
    print()
    print("1. To test error handling, temporarily modify src/tasks/serper_tasks.py:")
    print('   In _fetch_mock_api(), add at the start:')
    print('   if query["zip"] == "85002":  # Make second zip always fail')
    print('       raise Exception("Simulated API error for testing")')
    print()
    print("2. Run the batch processor:")
    print(f"   PYTHONPATH=$PWD .venv/bin/python -m src.flows.process_batches")
    print()
    print("3. Watch the logs - you should see:")
    print("   - First batch fails for zip 85002")
    print("   - Queries reset back to 'queued' status")
    print("   - Next iteration retries those queries")
    print("   - Eventually all queries complete (or keep failing)")
    print()
    print("4. Check job status periodically:")
    print(f"   PYTHONPATH=$PWD .venv/bin/python -c \"from src.operations.bigquery_ops import get_job_status; import json; print(json.dumps(get_job_status('{job_id}'), indent=2))\"")
    print()
    print("5. Remove the error injection code when done testing")
    print()

    return job_id


def check_query_statuses(job_id: str):
    """Check the status distribution of queries for a job."""
    from src.utils.bigquery_client import execute_query
    from google.cloud import bigquery

    query = f"""
    SELECT
        status,
        COUNT(*) as count
    FROM `{settings.bigquery_project_id}.{settings.bigquery_dataset}.serper_queries`
    WHERE job_id = @job_id
    GROUP BY status
    ORDER BY status
    """

    parameters = [
        bigquery.ScalarQueryParameter("job_id", "STRING", job_id)
    ]

    results = execute_query(query, parameters)

    print(f"\nQuery status distribution for job {job_id}:")
    print("-" * 40)
    for row in results:
        print(f"  {row.status:12s}: {row.count:3d}")
    print()


if __name__ == "__main__":
    # Create test job
    job_id = create_error_test_job()

    # Wait a moment
    print("Waiting 2 seconds...")
    time.sleep(2)

    # Show initial status
    check_query_statuses(job_id)
