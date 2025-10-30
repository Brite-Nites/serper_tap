"""Flow 2: Process batches for all running jobs until complete.

This is a self-looping flow that continuously processes batches until all
running jobs are complete. It handles multiple jobs fairly (one batch per job
per iteration) and exits naturally when no work remains.

Usage:
    # Run the processor (blocks until all jobs complete)
    python -m src.flows.process_batches

    # Or call from Python
    from src.flows.process_batches import process_job_batches
    result = process_job_batches()
"""

import time
from datetime import datetime
from typing import Any

from prefect import flow, get_run_logger, task

from src.tasks.bigquery_tasks import (
    dequeue_batch_task,
    get_running_jobs_task,
    mark_job_done_task,
    reset_batch_to_queued_task,
    skip_remaining_pages_task,
    store_places_task,
    update_job_stats_task,
    update_query_status_task,
)
from src.tasks.serper_tasks import fetch_serper_place_task


@task(name="process-single-batch-results")
def process_single_batch_results_task(
    job_id: str,
    keyword: str,
    state: str,
    queries: list[dict[str, Any]],
    results: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    """Process API results: update query statuses, extract places, handle early exit.

    This is the same logic as test_batch.py's process_batch_results_task.

    Args:
        job_id: Job identifier
        keyword: Search keyword
        state: State code
        queries: Original queries that were processed
        results: API responses from Serper (or mock)

    Returns:
        List of place records ready for storage
    """
    logger = get_run_logger()
    places_to_store = []

    for query, result in zip(queries, results):
        # Extract API response metadata
        api_places = result.get("places", [])
        results_count = len(api_places)
        credits = result.get("credits", 1)

        logger.info(
            f"Query {query['zip']} page {query['page']}: "
            f"{results_count} results, {credits} credits"
        )

        # Update query status to 'success'
        update_query_status_task(
            job_id=job_id,
            zip_code=query["zip"],
            page=query["page"],
            status="success",
            api_status=200,
            results_count=results_count,
            credits=credits,
            error=None
        )

        # Check early exit optimization
        if query["page"] == 1 and results_count < 10:
            skipped_count = skip_remaining_pages_task(
                job_id=job_id,
                zip_code=query["zip"],
                page=query["page"],
                results_count=results_count
            )
            if skipped_count > 0:
                logger.info(
                    f"Early exit: skipped {skipped_count} pages for zip {query['zip']} "
                    f"(page 1 had only {results_count} results)"
                )

        # Extract places from API response
        for place in api_places:
            place_uid = place.get("placeId") or place.get("cid")

            if not place_uid:
                logger.warning(
                    f"Place missing both placeId and cid: {place.get('title', 'Unknown')}"
                )
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

    logger.info(f"Processed {len(queries)} queries, extracted {len(places_to_store)} places")
    return places_to_store


def process_single_batch(
    job_id: str,
    keyword: str,
    state: str,
    batch_size: int
) -> dict[str, int]:
    """Process ONE batch for a single job.

    This is the core batch processing logic extracted from test_batch.py.
    If any query fails after all retries, the entire batch is reset back to
    'queued' status and can be retried in the next iteration.

    Args:
        job_id: Job identifier
        keyword: Search keyword
        state: State code
        batch_size: Number of queries to process

    Returns:
        {
            "queries_processed": 10,
            "places_stored": 47,
            "batch_failed": False  # True if batch failed and was reset
        }
    """
    logger = get_run_logger()

    # Step 1: Atomically dequeue a batch
    queries = dequeue_batch_task(job_id, batch_size)

    if not queries:
        logger.info(f"No queued queries for job {job_id}")
        return {
            "queries_processed": 0,
            "places_stored": 0,
            "batch_failed": False
        }

    # Extract claim_id for error recovery (all queries share same claim_id)
    claim_id = queries[0].get("claim_id")
    logger.info(f"Processing {len(queries)} queries for job {job_id} (claim: {claim_id})")

    try:
        # Step 2: Process queries in parallel using .map()
        results = fetch_serper_place_task.map(queries)

        # Step 3: Process results - update statuses, extract places, handle early exit
        places = process_single_batch_results_task(
            job_id=job_id,
            keyword=keyword,
            state=state,
            queries=queries,
            results=results
        )

        # Step 4: Store places in BigQuery
        if places:
            logger.info(f"Storing {len(places)} places for job {job_id}...")
            stored_count = store_places_task(job_id, places)
            logger.info(f"Stored {stored_count} new places")
        else:
            logger.info("No places to store")
            stored_count = 0

        # Step 5: Update job statistics
        update_job_stats_task(job_id)

        return {
            "queries_processed": len(queries),
            "places_stored": stored_count,
            "batch_failed": False
        }

    except Exception as e:
        # Batch failed after all retries - reset queries back to 'queued' for retry
        logger.error(f"Batch failed for job {job_id}, claim {claim_id}: {type(e).__name__}: {e}")

        if claim_id:
            reset_count = reset_batch_to_queued_task(claim_id)
            logger.info(f"Reset {reset_count} queries back to 'queued' status for retry")
        else:
            logger.warning("No claim_id found - cannot reset queries")

        return {
            "queries_processed": 0,
            "places_stored": 0,
            "batch_failed": True,
            "error": str(e)
        }


@flow(name="process-job-batches")
def process_job_batches() -> dict[str, Any]:
    """Process batches for all running jobs until none remain.

    This is a self-looping flow that:
    1. Finds all jobs with status='running'
    2. Processes ONE batch per job per iteration
    3. Marks jobs as 'done' when no queries remain
    4. Sleeps between iterations for rate limiting
    5. Exits when no running jobs remain

    Returns:
        {
            "total_batches_processed": 47,
            "total_queries_processed": 4700,
            "jobs_completed": ["job-id-1", "job-id-2"],
            "runtime_seconds": 523
        }
    """
    logger = get_run_logger()
    start_time = datetime.utcnow()

    total_batches = 0
    total_queries = 0
    completed_jobs = []

    logger.info("Starting batch processor...")

    while True:
        # Step 1: Find all running jobs
        running_jobs = get_running_jobs_task()

        if not running_jobs:
            logger.info("No running jobs found - exiting processor")
            break

        logger.info(f"Found {len(running_jobs)} running job(s)")

        # Step 2: Process one batch for EACH running job
        for job in running_jobs:
            job_id = job["job_id"]
            keyword = job["keyword"]
            state = job["state"]
            batch_size = job["batch_size"]

            logger.info(
                f"Processing batch for job {job_id} "
                f"(keyword={keyword}, state={state}, batch_size={batch_size})"
            )

            # Process one batch
            batch_result = process_single_batch(
                job_id=job_id,
                keyword=keyword,
                state=state,
                batch_size=batch_size
            )

            queries_processed = batch_result["queries_processed"]
            places_stored = batch_result["places_stored"]
            batch_failed = batch_result.get("batch_failed", False)

            # Track statistics
            if queries_processed > 0:
                total_batches += 1
                total_queries += queries_processed
                logger.info(
                    f"Batch complete for job {job_id}: "
                    f"{queries_processed} queries, {places_stored} places"
                )
            elif batch_failed:
                # Batch failed - queries reset to 'queued', will retry next iteration
                logger.warning(
                    f"Batch failed for job {job_id}: {batch_result.get('error', 'Unknown error')}. "
                    f"Queries reset to 'queued' for retry."
                )
            else:
                # No queries found - job is complete
                logger.info(f"Job {job_id} complete - marking as done")
                mark_job_done_task(job_id)
                completed_jobs.append(job_id)

        # Step 3: Rate limiting delay between iterations
        logger.info("Waiting 3 seconds before next iteration...")
        time.sleep(3)

    # Calculate runtime
    end_time = datetime.utcnow()
    runtime_seconds = (end_time - start_time).total_seconds()

    result = {
        "total_batches_processed": total_batches,
        "total_queries_processed": total_queries,
        "jobs_completed": completed_jobs,
        "runtime_seconds": runtime_seconds
    }

    logger.info("=" * 60)
    logger.info("BATCH PROCESSING COMPLETE")
    logger.info("=" * 60)
    logger.info(f"Total batches processed: {total_batches}")
    logger.info(f"Total queries processed: {total_queries}")
    logger.info(f"Jobs completed: {len(completed_jobs)}")
    logger.info(f"Runtime: {runtime_seconds:.1f} seconds")
    logger.info("")

    return result


if __name__ == "__main__":
    # Run the processor
    result = process_job_batches()

    print("\n" + "=" * 60)
    print("PROCESSING COMPLETE")
    print("=" * 60)
    print(f"Batches processed: {result['total_batches_processed']}")
    print(f"Queries processed: {result['total_queries_processed']}")
    print(f"Jobs completed: {len(result['jobs_completed'])}")
    print(f"Runtime: {result['runtime_seconds']:.1f} seconds")
    print("")
