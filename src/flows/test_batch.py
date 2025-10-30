"""Test flow for processing a single batch of queries.

This flow is designed for Phase 2A testing. It assumes a job already exists
with queued queries and processes exactly ONE batch to verify the end-to-end
integration of Prefect + BigQuery + Mock Serper API.

Prerequisites:
    - Job must exist in serper_jobs table
    - Job must have queued queries in serper_queries table
    - Use scripts/setup_test_job.py to create test data

Usage:
    from src.flows.test_batch import test_batch_processing
    test_batch_processing(job_id="your-job-id-here", batch_size=10)
"""

from typing import Any

from prefect import flow, get_run_logger, task

from src.operations import bigquery_ops
from src.tasks.bigquery_tasks import (
    dequeue_batch_task,
    get_job_status_task,
    skip_remaining_pages_task,
    store_places_task,
    update_job_stats_task,
    update_query_status_task,
)
from src.tasks.serper_tasks import fetch_serper_place_task


@task(name="process-batch-results")
def process_batch_results_task(
    job_id: str,
    keyword: str,
    state: str,
    queries: list[dict[str, Any]],
    results: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    """Process API results: update query statuses, extract places, handle early exit.

    This task runs after all parallel API calls complete. It:
    1. Updates query status (success/failed) for each query
    2. Checks early exit condition (page 1 with <10 results)
    3. Extracts places from API responses
    4. Builds place records for storage

    Args:
        job_id: Job identifier
        keyword: Search keyword (e.g., "bars")
        state: Two-letter state code (e.g., "AZ")
        queries: Original queries that were processed
        results: API responses from Serper (or mock)

    Returns:
        List of place records ready for storage in BigQuery
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
        # If page 1 returned <10 results, skip pages 2-3 for this zip
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

        # Extract places from API response and build records
        for place in api_places:
            # Use placeId or cid as unique identifier
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
                "payload": place,  # Store full API response as JSON
                "api_status": 200,
                "results_count": results_count,
                "credits": credits,
            }

            places_to_store.append(place_record)

    logger.info(f"Processed {len(queries)} queries, extracted {len(places_to_store)} places")
    return places_to_store


@flow(name="test-batch-processing")
def test_batch_processing(job_id: str, batch_size: int = 10) -> dict[str, Any]:
    """Test flow: Process one batch of queries for an existing job.

    This is a simplified flow for testing Phase 2A. It processes exactly
    one batch and returns, allowing you to verify the integration works
    before building the full self-looping processor.

    Args:
        job_id: Existing job identifier (use scripts/setup_test_job.py to create)
        batch_size: Number of queries to process in this batch

    Returns:
        Dict containing processing summary:
        {
            "queries_processed": 10,
            "places_stored": 47,
            "job_stats": {...}
        }

    Flow steps:
        1. Get job metadata (keyword, state)
        2. Dequeue batch atomically
        3. Process queries in parallel with .map()
        4. Process results (update statuses, extract places)
        5. Store places in BigQuery
        6. Update job statistics
    """
    logger = get_run_logger()

    # Step 1: Get job metadata (need keyword and state for place records)
    logger.info(f"Starting batch processing for job {job_id}")
    job_status = get_job_status_task(job_id)

    keyword = job_status["keyword"]
    state = job_status["state"]

    logger.info(f"Job details: keyword={keyword}, state={state}")

    # Step 2: Atomically dequeue a batch of queries
    logger.info(f"Dequeuing batch of {batch_size} queries...")
    queries = dequeue_batch_task(job_id, batch_size)

    if not queries:
        logger.info("No queued queries found - batch processing complete")
        return {
            "queries_processed": 0,
            "places_stored": 0,
            "job_stats": get_job_status_task(job_id)
        }

    logger.info(f"Dequeued {len(queries)} queries for processing")

    # Step 3: Process all queries in parallel using .map()
    # Prefect will execute these concurrently up to default limits
    logger.info("Fetching places from Serper API (mock) in parallel...")
    results = fetch_serper_place_task.map(queries)

    # Step 4: Process results - update statuses, extract places, handle early exit
    logger.info("Processing API results...")
    places = process_batch_results_task(
        job_id=job_id,
        keyword=keyword,
        state=state,
        queries=queries,
        results=results
    )

    # Step 5: Store places in BigQuery (idempotent MERGE)
    if places:
        logger.info(f"Storing {len(places)} places in BigQuery...")
        stored_count = store_places_task(job_id, places)
        logger.info(f"Stored {stored_count} new places (duplicates skipped by MERGE)")
    else:
        logger.info("No places to store")
        stored_count = 0

    # Step 6: Update job statistics
    logger.info("Updating job statistics...")
    updated_stats = update_job_stats_task(job_id)

    logger.info(
        f"Batch complete: processed {len(queries)} queries, "
        f"stored {stored_count} places"
    )

    # Return summary
    return {
        "queries_processed": len(queries),
        "places_stored": stored_count,
        "job_stats": updated_stats
    }


if __name__ == "__main__":
    # Example usage for local testing
    # First run: scripts/setup_test_job.py to create a test job
    # Then run this flow with the job_id it prints

    import sys

    if len(sys.argv) < 2:
        print("Usage: python -m src.flows.test_batch <job_id> [batch_size]")
        print("\nFirst create a test job:")
        print("  python scripts/setup_test_job.py")
        sys.exit(1)

    job_id = sys.argv[1]
    batch_size = int(sys.argv[2]) if len(sys.argv) > 2 else 10

    result = test_batch_processing(job_id=job_id, batch_size=batch_size)

    print("\n" + "="*60)
    print("BATCH PROCESSING COMPLETE")
    print("="*60)
    print(f"Queries processed: {result['queries_processed']}")
    print(f"Places stored: {result['places_stored']}")
    print(f"\nJob statistics:")
    for key, value in result['job_stats'].items():
        print(f"  {key}: {value}")
