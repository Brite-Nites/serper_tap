#!/usr/bin/env python3
"""Direct unit test for error handling in batch processing.

Directly calls process_single_batch with a job that will trigger an error.
"""

import sys
import uuid

from src.flows.process_batches import process_single_batch
from src.models.schemas import JobParams
from src.operations.bigquery_ops import (
    create_job,
    enqueue_queries,
)


def test_error_handling_directly():
    """Test error handling by directly calling process_single_batch."""

    # Create a minimal test job
    job_id = str(uuid.uuid4())
    keyword = "test-error-unit"

    print(f"Creating test job: {job_id}")
    print()

    params = JobParams(
        keyword=keyword,
        state="AZ",
        pages=1,
        batch_size=5,
        concurrency=2
    )
    create_job(job_id=job_id, params=params)

    # Enqueue queries - including one that will fail (85002)
    queries = [
        {"zip": "85001", "page": 1, "q": f"85001 {keyword}"},
        {"zip": "85002", "page": 1, "q": f"85002 {keyword}"},  # This will fail!
        {"zip": "85003", "page": 1, "q": f"85003 {keyword}"},
    ]
    enqueue_queries(job_id, queries)

    print(f"Enqueued {len(queries)} queries")
    print("Query 85002 will fail due to error injection in serper_tasks.py")
    print()

    # Directly call process_single_batch
    print("=" * 60)
    print("CALLING process_single_batch()")
    print("=" * 60)
    print()

    try:
        result = process_single_batch(
            job_id=job_id,
            keyword=keyword,
            state="AZ",
            batch_size=5
        )

        print()
        print("=" * 60)
        print("RESULT FROM process_single_batch()")
        print("=" * 60)
        print(f"Queries processed: {result['queries_processed']}")
        print(f"Places stored: {result['places_stored']}")
        print(f"Batch failed: {result.get('batch_failed', False)}")
        if result.get('error'):
            print(f"Error: {result['error']}")
        print()

        # Check if error handling worked
        if result.get('batch_failed'):
            print("✅ ERROR HANDLING WORKED!")
            print("   - Batch failed as expected")
            print("   - Queries should be reset to 'queued' status")
            print()

            # Check query statuses
            from src.utils.bigquery_client import execute_query
            from google.cloud import bigquery
            from src.utils.config import settings

            query = f"""
            SELECT status, COUNT(*) as count
            FROM `{settings.bigquery_project_id}.{settings.bigquery_dataset}.serper_queries`
            WHERE job_id = @job_id
            GROUP BY status
            """
            parameters = [bigquery.ScalarQueryParameter("job_id", "STRING", job_id)]
            results = execute_query(query, parameters)

            print("Query statuses after failure:")
            for row in results:
                print(f"  {row.status}: {row.count}")
            print()

        else:
            print("❌ ERROR HANDLING DID NOT WORK")
            print("   - Batch should have failed but didn't")
            print()

    except Exception as e:
        print(f"❌ UNEXPECTED EXCEPTION: {e}")
        print("   - Error handling should have caught this")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    test_error_handling_directly()
