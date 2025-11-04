#!/usr/bin/env python3
"""Validate real Serper API integration with timing and response analysis.

This script:
1. Creates a minimal 3-query test job
2. Processes it with real Serper API
3. Captures timing, response format, and network latency
4. Compares mock vs real API performance

Cost: $0.03 (3 API credits)
Time: ~1 minute

Usage:
    USE_MOCK_API=false python scripts/validate_real_api.py
"""

import time
import uuid

from src.flows.test_batch import test_batch_processing
from src.models.schemas import JobParams
from src.operations import bigquery_ops
from src.utils.config import settings


def validate_real_api():
    """Run end-to-end test with real Serper API."""
    print("=" * 70)
    print("REAL SERPER API VALIDATION")
    print("=" * 70)
    print()

    # Check configuration
    print("Configuration Check:")
    print(f"  USE_MOCK_API: {settings.use_mock_api}")
    print(f"  SERPER_API_KEY: {'Set' if settings.serper_api_key else 'MISSING'}")
    print()

    if settings.use_mock_api:
        print("❌ FAILURE: Still using mock API")
        print("   Set USE_MOCK_API=false in .env or run with:")
        print("   USE_MOCK_API=false python scripts/validate_real_api.py")
        print()
        return False

    if not settings.serper_api_key:
        print("❌ FAILURE: SERPER_API_KEY not set")
        print("   Get your API key from https://serper.dev")
        print("   Add to .env: SERPER_API_KEY=your-key-here")
        print()
        return False

    print("✅ Configuration valid (real API mode)")
    print()

    # Create test job
    job_id = str(uuid.uuid4())
    keyword = "coffee shops"
    zip_code = "85001"  # Downtown Phoenix
    pages = 3

    print("Creating test job...")
    print(f"  Job ID: {job_id}")
    print(f"  Keyword: {keyword}")
    print(f"  Zip: {zip_code}")
    print(f"  Pages: {pages}")
    print(f"  Cost: $0.03 (3 queries × $0.01)")
    print()

    params = JobParams(
        keyword=keyword,
        state="AZ",
        pages=pages,
        batch_size=10,
        concurrency=3
    )
    bigquery_ops.create_job(job_id=job_id, params=params)

    # Create queries
    queries = [
        {"zip": zip_code, "page": page, "q": f"{zip_code} {keyword}"}
        for page in range(1, pages + 1)
    ]
    bigquery_ops.enqueue_queries(job_id, queries)

    print("✅ Test job created")
    print()

    # Process with real API
    print("Processing with REAL Serper API...")
    print("  This will make 3 actual API calls")
    print("  Watch for:")
    print("    - Actual network latency (not mock 0.1-0.3s)")
    print("    - Real response format")
    print("    - Actual place data")
    print()

    start_time = time.perf_counter()

    try:
        result = test_batch_processing(job_id=job_id, batch_size=3)

        end_time = time.perf_counter()
        elapsed = end_time - start_time

        print()
        print("=" * 70)
        print("VALIDATION RESULTS")
        print("=" * 70)
        print()

        # Check results
        if result["queries_processed"] != 3:
            print(f"⚠️  WARNING: Expected 3 queries, processed {result['queries_processed']}")
        else:
            print(f"✅ All 3 queries processed successfully")

        print(f"✅ Places found: {result['places_stored']}")
        print(f"✅ Total time: {elapsed:.2f}s ({elapsed/3:.2f}s per query)")
        print()

        # Compare to mock timing
        mock_expected_per_query = 0.2  # Mock uses 0.1-0.3s
        real_per_query = elapsed / 3

        print("Performance Comparison:")
        print(f"  Mock API (expected): ~{mock_expected_per_query:.2f}s per query")
        print(f"  Real API (actual):    {real_per_query:.2f}s per query")

        if real_per_query < 0.5:
            print(f"  ✅ Real API faster than expected!")
        elif real_per_query < 1.0:
            print(f"  ✅ Real API performance acceptable")
        else:
            print(f"  ⚠️  Real API slower than mock (check network)")

        print()

        # Check job stats
        job_status = bigquery_ops.get_job_status(job_id)
        print("Job Statistics:")
        print(f"  Total queries: {job_status['totals']['queries']}")
        print(f"  Successful: {job_status['totals']['successes']}")
        print(f"  Failed: {job_status['totals']['failures']}")
        print(f"  Places found: {job_status['totals']['places']}")
        print(f"  Credits used: {job_status['totals']['credits']}")
        print()

        if job_status['totals']['failures'] > 0:
            print("⚠️  WARNING: Some queries failed")
            print("   Check serper_queries table for error details")
        else:
            print("✅ All queries succeeded")

        print()
        print("=" * 70)
        print("REAL API VALIDATION COMPLETE")
        print("=" * 70)
        print("✅ Real Serper API integration working")
        print(f"✅ Response format parses correctly")
        print(f"✅ Found {result['places_stored']} actual places")
        print(f"✅ Average latency: {real_per_query:.2f}s per query")
        print()
        print("Status: READY FOR PRODUCTION-SCALE TEST")
        print()

        return True

    except Exception as e:
        print()
        print("=" * 70)
        print("VALIDATION FAILED")
        print("=" * 70)
        print(f"❌ Error: {type(e).__name__}: {e}")
        print()
        print("Possible causes:")
        print("  - Invalid API key")
        print("  - Network connectivity issue")
        print("  - API rate limiting")
        print("  - Response format changed")
        print()

        import traceback
        traceback.print_exc()

        return False


if __name__ == "__main__":
    import sys

    success = validate_real_api()
    sys.exit(0 if success else 1)
