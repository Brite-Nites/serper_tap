#!/usr/bin/env python3
"""Small test script for real Serper API.

Creates a minimal test job with a single zip code (3 queries = 3 credits = ~$0.03).
Use this to validate real API integration before scaling up.

Usage:
    # Make sure USE_MOCK_API=false in your .env file
    python scripts/test_real_api.py

    # Or override via environment variable
    USE_MOCK_API=false python scripts/test_real_api.py
"""

import uuid

from src.operations.bigquery_ops import (
    create_job,
    enqueue_queries,
    get_job_status,
)
from src.utils.config import settings


def create_small_test_job(
    keyword: str = "bars",
    zip_code: str = "85001",  # Downtown Phoenix
    pages: int = 3
) -> str:
    """Create a minimal test job with a single zip code.

    Args:
        keyword: Search keyword
        zip_code: Single zip code to test
        pages: Number of pages (default 3 = 3 queries)

    Returns:
        job_id: UUID of created job
    """
    # Check API mode
    if settings.use_mock_api:
        print("⚠️  WARNING: Currently using MOCK API (use_mock_api=True)")
        print("   Set USE_MOCK_API=false in .env to test real API")
        print()

    # Validate API key if using real API
    if not settings.use_mock_api and not settings.serper_api_key:
        raise ValueError(
            "SERPER_API_KEY is required when USE_MOCK_API=false. "
            "Get your API key from https://serper.dev"
        )

    # Generate job ID
    job_id = str(uuid.uuid4())

    # Create job
    print(f"Creating test job: {job_id}")
    print(f"  Keyword: {keyword}")
    print(f"  Zip code: {zip_code}")
    print(f"  Pages: {pages}")
    print(f"  API mode: {'MOCK' if settings.use_mock_api else 'REAL'}")
    print()

    create_job(
        job_id=job_id,
        keyword=keyword,
        state="AZ",  # Not used for single zip test
        pages=pages,
        batch_size=10,  # Small batch
        concurrency=3
    )

    # Generate queries for single zip code
    queries = [(zip_code, page) for page in range(1, pages + 1)]

    # Enqueue queries
    print(f"Enqueueing {len(queries)} queries...")
    enqueue_queries(job_id, queries)

    # Show job status
    status = get_job_status(job_id)
    print()
    print("=" * 60)
    print("TEST JOB CREATED")
    print("=" * 60)
    print(f"Job ID: {job_id}")
    print(f"Total queries: {status['total_queries']}")
    print(f"Estimated cost: ${len(queries) * 0.01:.2f} (if using real API)")
    print()
    print("Next steps:")
    print(f"  1. Process the job:")
    print(f"     python -m src.flows.test_batch {job_id} --batch-size {len(queries)}")
    print()
    print(f"  2. Or process with full processor:")
    print(f"     python -m src.flows.process_batches")
    print()

    return job_id


if __name__ == "__main__":
    import sys

    # Allow keyword and zip as command line args
    keyword = sys.argv[1] if len(sys.argv) > 1 else "bars"
    zip_code = sys.argv[2] if len(sys.argv) > 2 else "85001"

    job_id = create_small_test_job(keyword=keyword, zip_code=zip_code)
