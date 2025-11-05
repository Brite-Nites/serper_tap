"""Flow 1: Create a new scraping job and enqueue all queries.

This flow initializes a job, enqueues all query combinations, and automatically
triggers the batch processor to handle query execution in the background.

Usage:
    # As a Python function
    from src.flows.create_job import create_scraping_job
    result = create_scraping_job(keyword="bars", state="AZ", pages=3)

    # As a CLI command
    python -m src.flows.create_job --keyword bars --state AZ --pages 3
"""

import argparse
import sys
import uuid
from typing import Any

from prefect import flow, get_run_logger
from prefect.deployments import run_deployment

from src.models.schemas import JobParams
from src.tasks.bigquery_tasks import (
    create_job_task,
    enqueue_queries_task,
    get_zips_for_state_task,
)
from src.utils.config import settings
from src.utils.cost_tracking import validate_budget_for_job


@flow(name="create-scraping-job")
def create_scraping_job(
    keyword: str,
    state: str,
    pages: int | None = None,
    batch_size: int | None = None,
    concurrency: int | None = None,
    dry_run: bool = False
) -> dict[str, Any]:
    """Create a new scraping job and enqueue all queries.

    This flow creates a job, enqueues all queries, and automatically triggers
    the batch processor to handle query execution in the background.

    Args:
        keyword: Search keyword (e.g., "bars", "restaurants")
        state: Two-letter state code (e.g., "AZ", "CA")
        pages: Pages to scrape per zip (defaults to settings.default_pages)
        batch_size: Batch size for processing (defaults to settings.default_batch_size)
        concurrency: Concurrency level (defaults to settings.default_concurrency)
        dry_run: If True, mark job as dry run (no real API calls)

    Returns:
        Dict with job details:
        {
            "job_id": "uuid",
            "keyword": "bars",
            "state": "AZ",
            "total_zips": 416,
            "total_queries": 1248,
            "status": "running",
            "processor_info": "background-pid-12345"  # or "manual-start-required"
        }

    Runtime: <10 seconds (setup only, processing happens in background)
    """
    logger = get_run_logger()

    # Step 1: Apply defaults from settings for missing parameters
    pages = pages if pages is not None else settings.default_pages
    batch_size = batch_size if batch_size is not None else settings.default_batch_size
    concurrency = concurrency if concurrency is not None else settings.default_concurrency

    logger.info(
        f"Creating scraping job: keyword={keyword}, state={state}, "
        f"pages={pages}, batch_size={batch_size}"
    )

    # Step 2: Validate parameters using Pydantic model
    params = JobParams(
        keyword=keyword,
        state=state,
        pages=pages,
        batch_size=batch_size,
        concurrency=concurrency,
        dry_run=dry_run
    )

    # Step 3: Get all zip codes for the state
    # (Do this before creating job to calculate budget impact)
    logger.info(f"Retrieving zip codes for {state}...")
    zips = get_zips_for_state_task(state)

    if not zips:
        logger.warning(f"No zip codes found for state {state}")
        raise ValueError(f"No zip codes found for state {state}")

    logger.info(f"Found {len(zips)} zip codes in {state}")

    # Step 4: Calculate total queries for budget validation
    total_queries = len(zips) * pages
    logger.info(f"Job will create {total_queries} total queries ({len(zips)} zips × {pages} pages)")

    # Step 5: Validate budget (only for real API, not dry runs or mock)
    if not dry_run and not settings.use_mock_api:
        logger.info("Checking daily budget...")
        budget_check = validate_budget_for_job(total_queries)

        if not budget_check["allowed"]:
            logger.error(f"Budget validation failed: {budget_check['message']}")
            raise ValueError(
                f"Job blocked: {budget_check['message']}. "
                f"Estimated cost: ${budget_check['job_estimate']['estimated_cost_usd']}"
            )

        if budget_check["budget_status"]["status"] == "warning":
            logger.warning(f"Budget warning: {budget_check['budget_status']['message']}")

        logger.info(
            f"Budget OK - Estimated cost: ${budget_check['job_estimate']['estimated_cost_usd']} "
            f"({budget_check['budget_status']['remaining_budget_usd']:.2f} remaining)"
        )
    else:
        logger.info("Skipping budget check (dry_run=True or use_mock_api=True)")

    # Step 6: Generate unique job_id and create job in BigQuery
    job_id = str(uuid.uuid4())
    logger.info(f"Generated job_id: {job_id}")

    logger.info("Creating job in serper_jobs table...")
    create_job_task(job_id, params)

    # Step 7: Generate all query combinations (zip × page)
    logger.info("Generating query details...")
    queries = []
    for zip_code in zips:
        for page in range(1, pages + 1):
            query = {
                "zip": zip_code,
                "page": page,
                "q": f"{zip_code} {keyword}"
            }
            queries.append(query)

    # Step 8: Enqueue all queries in BigQuery
    logger.info("Enqueuing queries in serper_queries table...")
    inserted = enqueue_queries_task(job_id, queries)
    logger.info(f"Enqueued {inserted} queries")

    # Step 9: Trigger batch processor via Prefect deployment
    logger.info("Triggering batch processor deployment...")
    try:
        # Trigger the batch processor deployment asynchronously
        # This requires the deployment to be created and a worker to be running:
        #   1. prefect deployment apply deployment.yaml
        #   2. prefect worker start --pool default-pool
        flow_run = run_deployment(
            name=settings.prefect_deployment_name,
            timeout=0,  # Return immediately without waiting for completion
            parameters={}
        )
        logger.info(f"Processor deployment triggered (Flow Run ID: {flow_run.id})")
        processor_info = f"deployment-run-{flow_run.id}"
    except Exception as e:
        logger.warning(f"Could not trigger deployment: {e}")
        logger.info("Deployment may not be set up. To configure:")
        logger.info("  1. prefect deployment apply deployment.yaml")
        logger.info("  2. prefect worker start --pool default-pool")
        logger.info("Alternatively, start processor manually: python -m src.flows.process_batches")
        processor_info = "manual-start-required"

    # Step 9: Return job summary
    result = {
        "job_id": job_id,
        "keyword": keyword,
        "state": state,
        "total_zips": len(zips),
        "total_queries": total_queries,
        "status": "running",
        "processor_info": processor_info
    }

    logger.info("=" * 60)
    logger.info("JOB CREATED SUCCESSFULLY")
    logger.info("=" * 60)
    logger.info(f"Job ID: {job_id}")
    logger.info(f"Keyword: {keyword}")
    logger.info(f"State: {state}")
    logger.info(f"Zip codes: {len(zips)}")
    logger.info(f"Total queries: {total_queries}")
    logger.info(f"Processor: {processor_info}")
    logger.info("")
    logger.info("Batch processor running - queries will be processed automatically.")
    logger.info("")

    return result


def main():
    """Command-line interface for creating scraping jobs."""
    parser = argparse.ArgumentParser(
        description="Create a new Serper scraping job",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Create a job for Arizona bars
  python -m src.flows.create_job --keyword bars --state AZ

  # Create a job with custom pages
  python -m src.flows.create_job --keyword restaurants --state CA --pages 5

  # Create a dry run job (no real API calls)
  python -m src.flows.create_job --keyword cafes --state NY --dry-run
        """
    )

    parser.add_argument(
        "--keyword",
        type=str,
        required=True,
        help="Search keyword (e.g., 'bars', 'restaurants')"
    )

    parser.add_argument(
        "--state",
        type=str,
        required=True,
        help="Two-letter state code (e.g., 'AZ', 'CA')"
    )

    parser.add_argument(
        "--pages",
        type=int,
        default=None,
        help=f"Pages to scrape per zip (default: {settings.default_pages})"
    )

    parser.add_argument(
        "--batch-size",
        type=int,
        default=None,
        help=f"Batch size for processing (default: {settings.default_batch_size})"
    )

    parser.add_argument(
        "--concurrency",
        type=int,
        default=None,
        help=f"Concurrency level (default: {settings.default_concurrency})"
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Mark job as dry run (no real API calls)"
    )

    args = parser.parse_args()

    # Run the flow
    try:
        result = create_scraping_job(
            keyword=args.keyword,
            state=args.state,
            pages=args.pages,
            batch_size=args.batch_size,
            concurrency=args.concurrency,
            dry_run=args.dry_run
        )

        print("\n" + "=" * 60)
        print("SUCCESS")
        print("=" * 60)
        print(f"Job ID: {result['job_id']}")
        print(f"Queries created: {result['total_queries']}")
        print(f"Processor: {result['processor_info']}")
        print("\nBatch processor started automatically in background.")
        print("Queries will be processed shortly.")
        print("")

    except Exception as e:
        print(f"\nError creating job: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
