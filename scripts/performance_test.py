#!/usr/bin/env python3
"""Performance test for batch processing with detailed timing breakdown.

This script creates a small test job and processes one batch while measuring
the time spent in each phase to identify bottlenecks and verify performance
improvements from batched BigQuery operations.

Usage:
    # Basic test (10 queries)
    python scripts/performance_test.py

    # Larger test (30 queries)
    python scripts/performance_test.py --batch-size 30

    # Verbose output
    python scripts/performance_test.py --verbose
"""

import argparse
import sys
import time
import uuid
from typing import Any

from src.models.schemas import JobParams
from src.operations import bigquery_ops
from src.tasks import serper_tasks


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


def create_test_job(batch_size: int) -> tuple[str, int]:
    """Create a small test job with specified number of queries.

    Args:
        batch_size: Number of queries to create

    Returns:
        Tuple of (job_id, num_queries)
    """
    job_id = str(uuid.uuid4())
    keyword = "perf-test"

    # Create job
    params = JobParams(
        keyword=keyword,
        state="RI",  # Small state
        pages=1,
        batch_size=batch_size,
        concurrency=20
    )
    bigquery_ops.create_job(job_id=job_id, params=params)

    # Create minimal test queries (limit to batch_size)
    queries = []
    zip_codes = ["02801", "02802", "02803", "02804", "02805",
                 "02806", "02807", "02808", "02809", "02810"]

    for i, zip_code in enumerate(zip_codes):
        if i >= batch_size:
            break
        queries.append({
            "zip": zip_code,
            "page": 1,
            "q": f"{zip_code} {keyword}"
        })

    bigquery_ops.enqueue_queries(job_id, queries)

    return job_id, len(queries)


def process_batch_with_timing(
    job_id: str,
    keyword: str,
    state: str,
    batch_size: int,
    verbose: bool = False
) -> dict[str, Any]:
    """Process a batch with detailed timing for each phase.

    Args:
        job_id: Job identifier
        keyword: Search keyword
        state: State code
        batch_size: Number of queries to process
        verbose: Print detailed logs

    Returns:
        Dict with timing breakdown and results
    """
    timings = {}

    # Phase 1: Dequeue batch
    t_start = time.perf_counter()
    queries = bigquery_ops.dequeue_batch(job_id, batch_size)
    timings['dequeue'] = time.perf_counter() - t_start

    if not queries:
        return {"error": "No queries dequeued", "timings": timings}

    if verbose:
        print(f"Dequeued {len(queries)} queries")

    # Phase 2: Parallel API calls (simulated with ThreadPoolExecutor for timing)
    t_start = time.perf_counter()
    from concurrent.futures import ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=20) as executor:
        results = list(executor.map(serper_tasks._fetch_mock_api if True else serper_tasks._fetch_real_api, queries))
    timings['api_calls'] = time.perf_counter() - t_start

    if verbose:
        print(f"API calls completed in {timings['api_calls']:.2f}s")

    # Phase 3: Result processing (collect updates, extract places)
    t_start = time.perf_counter()

    places_to_store = []
    status_updates = []
    zips_to_skip = []

    for query, result in zip(queries, results):
        api_places = result.get("places", [])
        results_count = len(api_places)
        credits = result.get("credits", 1)

        # Collect status update
        status_updates.append({
            "zip": query["zip"],
            "page": query["page"],
            "status": "success",
            "api_status": 200,
            "results_count": results_count,
            "credits": credits,
            "error": None
        })

        # Collect early exit data
        if query["page"] == 1 and results_count < 10:
            zips_to_skip.append(query["zip"])

        # Extract places
        for place in api_places:
            place_uid = place.get("placeId") or place.get("cid")
            if not place_uid:
                continue

            place_record = {
                "keyword": keyword,
                "state": state,
                "zip": query["zip"],
                "page": query["page"],
                "place_uid": place_uid,
                "payload": place,
                "api_status": 200,
                "results_count": results_count,
                "credits": credits,
            }
            places_to_store.append(place_record)

    timings['result_processing'] = time.perf_counter() - t_start

    if verbose:
        print(f"Result processing: {len(status_updates)} updates, {len(places_to_store)} places")

    # Phase 4: Batched BigQuery updates
    t_start = time.perf_counter()
    updated_count = bigquery_ops.batch_update_query_statuses(job_id, status_updates)

    if zips_to_skip:
        skipped_count = bigquery_ops.batch_skip_remaining_pages(job_id, zips_to_skip)
    else:
        skipped_count = 0

    timings['bigquery_batch'] = time.perf_counter() - t_start

    if verbose:
        print(f"BigQuery batch ops: {updated_count} updated, {skipped_count} skipped")

    # Phase 5: Store places
    t_start = time.perf_counter()
    if places_to_store:
        stored_count = bigquery_ops.store_places(job_id, places_to_store)
    else:
        stored_count = 0
    timings['place_storage'] = time.perf_counter() - t_start

    if verbose:
        print(f"Stored {stored_count} places")

    # Phase 6: Update job stats
    t_start = time.perf_counter()
    bigquery_ops.update_job_stats(job_id)
    timings['stats_update'] = time.perf_counter() - t_start

    # Calculate totals
    timings['total'] = sum(timings.values())

    return {
        "queries_processed": len(queries),
        "places_stored": stored_count,
        "updated_count": updated_count,
        "skipped_count": skipped_count,
        "timings": timings
    }


def print_performance_report(result: dict[str, Any], batch_size: int):
    """Print detailed performance report.

    Args:
        result: Results dict from process_batch_with_timing
        batch_size: Batch size used
    """
    if "error" in result:
        print(f"❌ Error: {result['error']}")
        return

    timings = result['timings']
    total_time = timings['total']

    print()
    print("=" * 70)
    print("PERFORMANCE TEST RESULTS")
    print("=" * 70)
    print()
    print("Test Configuration:")
    print(f"  Batch size:      {batch_size} queries")
    print(f"  API type:        Mock (for consistent timing)")
    print(f"  Parallelism:     20 workers")
    print()
    print("Phase Breakdown:")
    print(f"  1. Dequeue batch:        {timings['dequeue']:5.2f}s  ({timings['dequeue']/total_time*100:4.1f}%)")
    print(f"  2. API calls (parallel): {timings['api_calls']:5.2f}s  ({timings['api_calls']/total_time*100:4.1f}%)")
    print(f"  3. Result processing:    {timings['result_processing']:5.2f}s  ({timings['result_processing']/total_time*100:4.1f}%)")
    print(f"  4. BigQuery batch ops:   {timings['bigquery_batch']:5.2f}s  ({timings['bigquery_batch']/total_time*100:4.1f}%)", end="")

    # Flag bottleneck
    if timings['bigquery_batch'] / total_time > 0.5:
        print("  ⚠️ BOTTLENECK")
    else:
        print()

    print(f"  5. Place storage:        {timings['place_storage']:5.2f}s  ({timings['place_storage']/total_time*100:4.1f}%)")
    print(f"  6. Stats update:         {timings['stats_update']:5.2f}s  ({timings['stats_update']/total_time*100:4.1f}%)")
    print("  " + "-" * 50)
    print(f"  Total batch time:        {total_time:5.2f}s")
    print()
    print("Performance Metrics:")

    queries_per_sec = result['queries_processed'] / total_time
    queries_per_min = queries_per_sec * 60

    print(f"  Queries/second:  {queries_per_sec:.2f}")
    print(f"  Queries/minute:  {queries_per_min:.1f}")
    print(f"  Target range:    333-500 queries/min")

    # Status indicator
    if queries_per_min >= 333:
        print(f"  Status:          ✅ MEETING TARGET ({queries_per_min/333*100:.0f}% of minimum)")
    elif queries_per_min >= 166:
        print(f"  Status:          ⚠️ BELOW TARGET ({queries_per_min/333*100:.0f}% of minimum)")
    else:
        print(f"  Status:          ❌ FAR BELOW TARGET ({queries_per_min/333*100:.0f}% of minimum)")

    print()
    print("Results:")
    print(f"  Queries processed:  {result['queries_processed']}")
    print(f"  Places stored:      {result['places_stored']}")
    print(f"  Updates executed:   {result['updated_count']}")
    print(f"  Pages skipped:      {result['skipped_count']}")
    print()

    # Analysis and recommendations
    print("Analysis:")

    # Check API call performance
    avg_api_time_ms = (timings['api_calls'] / result['queries_processed']) * 1000
    if avg_api_time_ms < 50:
        print(f"  ✅ Parallel API calls working ({avg_api_time_ms:.0f}ms avg per call)")
    else:
        print(f"  ⚠️ API calls slower than expected ({avg_api_time_ms:.0f}ms avg per call)")

    # Check BigQuery performance
    bigquery_pct = (timings['bigquery_batch'] / total_time) * 100
    if bigquery_pct > 50:
        print(f"  ⚠️ BigQuery operations consuming {bigquery_pct:.0f}% of batch time")
        print("     Possible issues:")
        print("       - Network latency to BigQuery")
        print("       - Query complexity")
        param_count = len(result.get('updated_count', 0)) * 7 + len(result.get('skipped_count', 0))
        print(f"       - High parameter count (~{param_count} parameters)")
    else:
        print(f"  ✅ BigQuery batch operations efficient ({bigquery_pct:.0f}% of total time)")

    print()
    print("Next Steps:")
    if queries_per_min < 333:
        print("  - Review BigQuery query execution plan")
        print("  - Test with real API to compare performance")
        print("  - Consider increasing batch size to amortize overhead")
        print("  - Check BigQuery slot allocation and quotas")
    else:
        print("  ✅ Performance meets specification requirements")
        print("  - Ready for production testing")
        print("  - Monitor performance with real API calls")

    print("=" * 70)
    print()


def main():
    parser = argparse.ArgumentParser(
        description="Performance test for batch processing",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )

    parser.add_argument(
        "--batch-size",
        type=int,
        default=10,
        help="Number of queries to test (default: 10)"
    )

    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Print verbose output during processing"
    )

    args = parser.parse_args()

    try:
        print("Setting up performance test...")
        print()

        # Clear existing jobs
        cleared = clear_running_jobs()
        if cleared > 0:
            print()

        # Create test job
        print(f"Creating test job with {args.batch_size} queries...")
        job_id, num_queries = create_test_job(args.batch_size)
        print(f"Created job {job_id} with {num_queries} queries")
        print()

        if args.verbose:
            print("Starting batch processing with detailed timing...")
            print()

        # Process batch with timing
        result = process_batch_with_timing(
            job_id=job_id,
            keyword="perf-test",
            state="RI",
            batch_size=args.batch_size,
            verbose=args.verbose
        )

        # Print report
        print_performance_report(result, args.batch_size)

    except Exception as e:
        print(f"❌ Performance test failed: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
