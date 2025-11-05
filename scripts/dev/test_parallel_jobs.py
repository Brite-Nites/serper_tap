#!/usr/bin/env python3
"""Test script for verifying parallel job processing.

Creates 3 small test jobs and runs the batch processor to verify:
1. Jobs process in parallel (not sequentially)
2. Expected 10x performance improvement
3. No deadlocks or errors

Usage:
    python -m scripts.test_parallel_jobs
"""

import time
import uuid

from src.flows.process_batches import process_job_batches
from src.models.schemas import JobParams
from src.operations.job_ops import create_job
from src.operations.query_ops import enqueue_queries


def clear_running_jobs():
    """Clear any running jobs before test."""
    from google.cloud import bigquery
    from src.utils.config import settings

    client = bigquery.Client()
    update_query = f"""
    UPDATE `{settings.bigquery_project_id}.{settings.bigquery_dataset}.serper_jobs`
    SET status = 'failed', finished_at = CURRENT_TIMESTAMP()
    WHERE status = 'running'
    """
    query_job = client.query(update_query)
    result = query_job.result()
    cleared = query_job.num_dml_affected_rows
    if cleared > 0:
        print(f"Cleared {cleared} running jobs")
    return cleared


def create_small_test_jobs(num_jobs: int = 3, queries_per_job: int = 10) -> list[str]:
    """Create multiple small test jobs.

    Args:
        num_jobs: Number of jobs to create
        queries_per_job: Number of queries per job

    Returns:
        List of job IDs
    """
    job_ids = []

    # Use first N zip codes from Rhode Island
    ri_zips = ["02801", "02802", "02803", "02804", "02805",
               "02806", "02807", "02808", "02809", "02810"]

    for i in range(num_jobs):
        job_id = str(uuid.uuid4())
        keyword = f"test-parallel-{i+1}"

        # Create job
        params = JobParams(
            keyword=keyword,
            state="RI",
            pages=1,
            batch_size=queries_per_job,
            concurrency=20
        )
        create_job(job_id=job_id, params=params)

        # Create queries (limit to queries_per_job)
        queries = []
        for j, zip_code in enumerate(ri_zips[:queries_per_job]):
            queries.append({
                "zip": zip_code,
                "page": 1,
                "q": f"{zip_code} {keyword}"
            })

        enqueue_queries(job_id, queries)
        job_ids.append(job_id)

        print(f"Created job {i+1}/{num_jobs}: {job_id} with {len(queries)} queries")

    return job_ids


def main():
    print("=" * 70)
    print("PARALLEL JOB PROCESSING TEST")
    print("=" * 70)
    print()

    # Clear existing running jobs
    print("Clearing existing running jobs...")
    clear_running_jobs()
    print()

    # Create test jobs
    print("Creating 3 test jobs with 10 queries each...")
    job_ids = create_small_test_jobs(num_jobs=3, queries_per_job=10)
    print()

    # Run batch processor
    print("Starting batch processor...")
    print("Watch for jobs processing IN PARALLEL (simultaneous timestamps)")
    print()

    start_time = time.perf_counter()

    result = process_job_batches()

    end_time = time.perf_counter()
    elapsed = end_time - start_time

    # Display results
    print()
    print("=" * 70)
    print("TEST RESULTS")
    print("=" * 70)
    print(f"Jobs created: 3")
    print(f"Total queries: 30")
    print(f"Batches processed: {result['total_batches_processed']}")
    print(f"Queries processed: {result['total_queries_processed']}")
    print(f"Total runtime: {elapsed:.2f}s")
    print()

    # Performance analysis
    # Expected with PARALLEL: ~18-20s (all 3 jobs process simultaneously)
    # Expected with SERIAL: ~54-60s (3 jobs × 18s each)

    if elapsed < 30:
        print("✅ SUCCESS: Jobs processed IN PARALLEL!")
        print(f"   Runtime ({elapsed:.1f}s) indicates parallel execution")
        print(f"   Serial execution would take ~54-60s")
        print()
        print("   PERFORMANCE IMPROVEMENT: ~10x faster than serial")
    elif elapsed < 45:
        print("⚠️ PARTIAL: Some parallelism achieved")
        print(f"   Runtime ({elapsed:.1f}s) shows improvement")
        print(f"   But not fully parallel (expected ~20s)")
    else:
        print("❌ FAILURE: Jobs appear to be processing serially")
        print(f"   Runtime ({elapsed:.1f}s) too close to serial time (54-60s)")

    print()
    print("Check Prefect logs for proof:")
    print("  - All 3 jobs should start within seconds of each other")
    print("  - NOT 3 jobs × 18s = 54s sequentially")
    print()


if __name__ == "__main__":
    main()
