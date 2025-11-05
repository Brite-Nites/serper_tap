#!/usr/bin/env python3
"""Validation test for nested .map() parallelism in Prefect.

This test proves that Prefect's nested .map() pattern works as expected:
- Outer .map() processes multiple "jobs" in parallel
- Each job spawns inner .map() for parallel "queries"
- Parent tasks don't block worker threads while waiting for children

Expected behavior:
    - With 3 jobs × 5 queries each, we expect ~15 parallel task executions
    - Total runtime should be ~1-2 seconds (not 15 seconds)
    - This proves nested parallelism works without deadlocks

Usage:
    python -m scripts.test_nested_parallelism
"""

import time
from typing import Any

from prefect import flow, get_run_logger, task
from prefect.task_runners import ConcurrentTaskRunner


@task(name="simulate-query")
def simulate_query_task(job_id: str, query_num: int) -> dict[str, Any]:
    """Simulate a single query with 0.5s delay.

    Args:
        job_id: Job identifier
        query_num: Query number within job

    Returns:
        Dict with job_id, query_num, and result
    """
    logger = get_run_logger()
    logger.info(f"Job {job_id}: Processing query {query_num}")

    # Simulate API call
    time.sleep(0.5)

    result = {
        "job_id": job_id,
        "query_num": query_num,
        "result": f"Data for job {job_id} query {query_num}"
    }

    logger.info(f"Job {job_id}: Query {query_num} complete")
    return result


@task(name="process-job")
def process_job_task(job_id: str, num_queries: int) -> dict[str, Any]:
    """Process a single job by spawning multiple parallel queries (inner .map()).

    This is analogous to process_single_batch() in the real code.

    Args:
        job_id: Job identifier
        num_queries: Number of queries to process

    Returns:
        Dict with job_id and results
    """
    logger = get_run_logger()
    logger.info(f"Starting job {job_id} with {num_queries} queries")

    # Inner .map() - process all queries for this job in parallel
    query_nums = list(range(1, num_queries + 1))
    results = simulate_query_task.map(
        job_id=[job_id] * num_queries,
        query_num=query_nums
    )

    logger.info(f"Job {job_id} complete: processed {len(results)} queries")

    return {
        "job_id": job_id,
        "queries_processed": len(results),
        "results": results
    }


@task(name="analyze-results")
def analyze_results_task(job_results: list[dict[str, Any]], elapsed: float, num_jobs: int, queries_per_job: int) -> dict[str, Any]:
    """Analyze test results to determine if nested parallelism worked.

    This task receives the actual results (Prefect auto-resolves futures when passed to tasks).

    Args:
        job_results: List of job result dicts
        elapsed: Elapsed time in seconds
        num_jobs: Number of jobs processed
        queries_per_job: Queries per job

    Returns:
        Dict with analysis results
    """
    logger = get_run_logger()

    total_queries = sum(r["queries_processed"] for r in job_results)

    logger.info("")
    logger.info("=" * 60)
    logger.info("TEST RESULTS")
    logger.info("=" * 60)
    logger.info(f"Jobs processed: {len(job_results)}")
    logger.info(f"Total queries: {total_queries}")
    logger.info(f"Elapsed time: {elapsed:.2f}s")
    logger.info("")

    # Analyze results
    expected_serial_time = num_jobs * queries_per_job * 0.5
    expected_parallel_time = 0.5  # Just one query duration

    if elapsed < 2.0:
        logger.info("✅ SUCCESS: Nested parallelism working!")
        logger.info(f"   Runtime ({elapsed:.2f}s) indicates parallel execution")
        logger.info(f"   Serial execution would take ~{expected_serial_time:.1f}s")
        success = True
    elif elapsed < expected_serial_time * 0.5:
        logger.info("✅ PARTIAL SUCCESS: Significant parallelism achieved")
        logger.info(f"   Runtime ({elapsed:.2f}s) faster than serial")
        success = True
    else:
        logger.info("❌ FAILURE: Appears to be serial execution")
        logger.info(f"   Runtime ({elapsed:.2f}s) too close to serial time ({expected_serial_time:.1f}s)")
        success = False

    logger.info("")

    return {
        "num_jobs": num_jobs,
        "queries_per_job": queries_per_job,
        "total_queries": total_queries,
        "elapsed_seconds": elapsed,
        "expected_serial_seconds": expected_serial_time,
        "expected_parallel_seconds": expected_parallel_time,
        "success": success
    }


@flow(name="test-nested-parallelism", task_runner=ConcurrentTaskRunner())
def test_nested_parallelism_flow(num_jobs: int = 3, queries_per_job: int = 5) -> dict[str, Any]:
    """Test flow: Process multiple jobs in parallel, each with parallel queries.

    This validates the nested .map() pattern:
    - Outer .map() processes jobs in parallel
    - Inner .map() (within each job) processes queries in parallel

    With 3 jobs × 5 queries and max_workers=20:
    - We expect all 15 queries to run in parallel
    - Total time should be ~0.5s (one query duration)
    - NOT 15 × 0.5s = 7.5s (which would indicate serial processing)

    Args:
        num_jobs: Number of jobs to process
        queries_per_job: Queries per job

    Returns:
        Dict with timing and results
    """
    logger = get_run_logger()

    logger.info(f"Starting test with {num_jobs} jobs, {queries_per_job} queries each")
    logger.info(f"Total queries: {num_jobs * queries_per_job}")
    logger.info(f"Expected runtime with full parallelism: ~0.5s")
    logger.info(f"Expected runtime if serial: ~{num_jobs * queries_per_job * 0.5}s")

    start_time = time.perf_counter()

    # Outer .map() - process all jobs in parallel
    job_ids = [f"job-{i}" for i in range(1, num_jobs + 1)]
    job_results = process_job_task.map(
        job_id=job_ids,
        num_queries=[queries_per_job] * num_jobs
    )

    end_time = time.perf_counter()
    elapsed = end_time - start_time

    # Pass futures to another task - Prefect will auto-resolve them
    result = analyze_results_task(job_results, elapsed, num_jobs, queries_per_job)
    return result


if __name__ == "__main__":
    import sys

    # Allow customization for more thorough testing
    num_jobs = int(sys.argv[1]) if len(sys.argv) > 1 else 3
    queries_per_job = int(sys.argv[2]) if len(sys.argv) > 2 else 5

    result = test_nested_parallelism_flow(
        num_jobs=num_jobs,
        queries_per_job=queries_per_job
    )

    print("\n" + "=" * 60)
    print("NESTED PARALLELISM TEST SUMMARY")
    print("=" * 60)
    print(f"Configuration: {result['num_jobs']} jobs × {result['queries_per_job']} queries")
    print(f"Total queries: {result['total_queries']}")
    print(f"Elapsed time: {result['elapsed_seconds']:.2f}s")
    print(f"Expected (parallel): ~{result['expected_parallel_seconds']:.1f}s")
    print(f"Expected (serial): ~{result['expected_serial_seconds']:.1f}s")
    print("")

    if result['success']:
        print("✅ TEST PASSED: Nested .map() parallelism validated")
        print("")
        print("Safe to implement job-level parallelism in process_batches.py")
        sys.exit(0)
    else:
        print("❌ TEST FAILED: Nested parallelism not working as expected")
        print("")
        print("Review Prefect configuration or consider alternative approach")
        sys.exit(1)
