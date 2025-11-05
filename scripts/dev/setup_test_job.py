#!/usr/bin/env python
"""Helper script to create a test job with queued queries.

This script creates a small test job that can be used to verify the
Prefect + BigQuery integration works end-to-end. It:

1. Creates a new job in serper_jobs table
2. Retrieves zip codes for a specified state
3. Enqueues queries for a subset of zip codes (to keep tests fast)
4. Prints the job_id for use with test_batch_processing flow

Usage:
    python scripts/setup_test_job.py
    python scripts/setup_test_job.py --state CA --keyword restaurants --zips 5 --pages 3

After running, use the printed job_id to test the batch processor:
    python -m src.flows.test_batch <job_id> 10
"""

import argparse
import uuid

from src.models.schemas import JobParams
from src.operations import bigquery_ops


def setup_test_job(
    keyword: str = "bars",
    state: str = "AZ",
    num_zips: int = 5,
    pages: int = 3,
    batch_size: int = 100,
    concurrency: int = 20
) -> dict:
    """Create a test job with queued queries.

    Args:
        keyword: Search keyword
        state: Two-letter state code
        num_zips: Number of zip codes to include (keeps test small)
        pages: Pages to scrape per zip code
        batch_size: Batch size for processing
        concurrency: Concurrency level

    Returns:
        Dict with job details including job_id
    """
    print("="*60)
    print("CREATING TEST JOB")
    print("="*60)

    # Step 1: Generate job_id
    job_id = str(uuid.uuid4())
    print(f"\nGenerated job_id: {job_id}")

    # Step 2: Create job parameters
    params = JobParams(
        keyword=keyword,
        state=state,
        pages=pages,
        batch_size=batch_size,
        concurrency=concurrency,
        dry_run=False  # Not a dry run, we'll use mock API
    )

    print(f"\nJob parameters:")
    print(f"  Keyword: {params.keyword}")
    print(f"  State: {params.state}")
    print(f"  Pages per zip: {params.pages}")
    print(f"  Batch size: {params.batch_size}")

    # Step 3: Create job in BigQuery
    print(f"\nCreating job in serper_jobs table...")
    result = bigquery_ops.create_job(job_id, params)
    print(f"  ✓ Job created at {result['created_at']}")

    # Step 4: Get zip codes for state
    print(f"\nRetrieving zip codes for {state}...")
    all_zips = bigquery_ops.get_zips_for_state(state)

    if not all_zips:
        print(f"  ✗ No zip codes found for state {state}")
        print(f"  Note: Ensure reference.geo_zip_all table exists and has data")
        return {
            "job_id": job_id,
            "error": "No zip codes found"
        }

    print(f"  ✓ Found {len(all_zips)} total zip codes in {state}")

    # Use only a subset for testing (keeps it fast)
    test_zips = all_zips[:num_zips]
    print(f"  Using first {num_zips} zip codes for test: {', '.join(test_zips)}")

    # Step 5: Generate queries for all combinations of (zip, page)
    print(f"\nGenerating queries for {len(test_zips)} zips × {pages} pages...")
    queries = []
    for zip_code in test_zips:
        for page in range(1, pages + 1):
            query = {
                "zip": zip_code,
                "page": page,
                "q": f"{zip_code} {keyword}"
            }
            queries.append(query)

    print(f"  Generated {len(queries)} total queries")

    # Step 6: Enqueue queries in BigQuery
    print(f"\nEnqueuing queries in serper_queries table...")
    inserted = bigquery_ops.enqueue_queries(job_id, queries)
    print(f"  ✓ Inserted {inserted} new queries")

    # Step 7: Update job stats to reflect queued queries
    print(f"\nUpdating job statistics...")
    stats = bigquery_ops.update_job_stats(job_id)
    print(f"  ✓ Job stats updated:")
    print(f"    - Queries: {stats['queries']}")
    print(f"    - Zips: {stats['zips']}")

    # Step 8: Print instructions for next steps
    print("\n" + "="*60)
    print("TEST JOB READY")
    print("="*60)
    print(f"\nJob ID: {job_id}")
    print(f"Queries queued: {len(queries)}")
    print(f"\nTo test batch processing, run:")
    print(f"  python -m src.flows.test_batch {job_id} 10")
    print(f"\nTo check job status in BigQuery:")
    print(f"  SELECT * FROM `{{project}}.raw_data.serper_jobs` WHERE job_id = '{job_id}'")
    print(f"  SELECT * FROM `{{project}}.raw_data.serper_queries` WHERE job_id = '{job_id}'")
    print("")

    return {
        "job_id": job_id,
        "keyword": keyword,
        "state": state,
        "queries_created": len(queries),
        "test_zips": test_zips,
        "stats": stats
    }


def main():
    """Parse arguments and create test job."""
    parser = argparse.ArgumentParser(
        description="Create a test job for Prefect + BigQuery integration testing"
    )

    parser.add_argument(
        "--keyword",
        type=str,
        default="bars",
        help="Search keyword (default: bars)"
    )

    parser.add_argument(
        "--state",
        type=str,
        default="AZ",
        help="Two-letter state code (default: AZ)"
    )

    parser.add_argument(
        "--zips",
        type=int,
        default=5,
        help="Number of zip codes to include in test (default: 5)"
    )

    parser.add_argument(
        "--pages",
        type=int,
        default=3,
        help="Pages to scrape per zip code (default: 3)"
    )

    parser.add_argument(
        "--batch-size",
        type=int,
        default=100,
        help="Batch size for processing (default: 100)"
    )

    parser.add_argument(
        "--concurrency",
        type=int,
        default=20,
        help="Concurrency level (default: 20)"
    )

    args = parser.parse_args()

    try:
        result = setup_test_job(
            keyword=args.keyword,
            state=args.state,
            num_zips=args.zips,
            pages=args.pages,
            batch_size=args.batch_size,
            concurrency=args.concurrency
        )

        if "error" in result:
            exit(1)

    except Exception as e:
        print(f"\n✗ Error creating test job: {e}")
        print(f"\nMake sure:")
        print(f"  1. BigQuery tables are created (run sql/schema.sql)")
        print(f"  2. Environment variables are set in .env")
        print(f"  3. reference.geo_zip_all table exists with data for {args.state}")
        exit(1)


if __name__ == "__main__":
    main()
