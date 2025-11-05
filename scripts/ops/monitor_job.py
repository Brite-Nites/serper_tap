#!/usr/bin/env python3
"""Monitor a scraping job in real-time.

Usage:
    python scripts/monitor_job.py <job_id>

Example:
    python scripts/monitor_job.py 096c367e-5042-4c4b-a522-1c9cc3776c63
"""

import os
import sys
_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

import time
from datetime import datetime

from src.operations.bigquery_ops import get_job_status
from src.utils.bigquery_client import execute_query
from google.cloud import bigquery
from src.utils.config import settings


def monitor_job(job_id: str, refresh_seconds: int = 10):
    """Monitor job progress with periodic updates.

    Args:
        job_id: Job identifier
        refresh_seconds: Seconds between refreshes
    """
    print(f"Monitoring job: {job_id}")
    print(f"Refreshing every {refresh_seconds} seconds (Ctrl+C to stop)")
    print("=" * 70)
    print()

    iteration = 0
    last_success_count = 0

    try:
        while True:
            iteration += 1
            timestamp = datetime.now().strftime("%H:%M:%S")

            # Get job totals first (needed for total_queries)
            job_status = get_job_status(job_id)
            totals = job_status['totals']
            total_queries = totals['queries']

            # Get query status distribution
            query = f"""
            SELECT
              status,
              COUNT(*) as count,
              ROUND(100.0 * COUNT(*) / @total_queries, 1) as pct
            FROM `{settings.bigquery_project_id}.{settings.bigquery_dataset}.serper_queries`
            WHERE job_id = @job_id
            GROUP BY status
            ORDER BY status
            """

            params = [
                bigquery.ScalarQueryParameter("job_id", "STRING", job_id),
                bigquery.ScalarQueryParameter("total_queries", "INT64", total_queries)
            ]
            results = execute_query(query, params)

            # Convert to dict
            status_counts = {row.status: (row.count, row.pct) for row in results}

            # Calculate progress
            success_count = status_counts.get('success', (0, 0))[0]
            skipped_count = status_counts.get('skipped', (0, 0))[0]
            completed = success_count + skipped_count
            progress_pct = round(100.0 * completed / total_queries, 1) if total_queries > 0 else 0

            # Calculate rate
            queries_since_last = success_count - last_success_count
            rate_per_10s = queries_since_last if iteration > 1 else 0
            est_minutes_remaining = 0
            if rate_per_10s > 0:
                queries_remaining = total_queries - completed
                est_minutes_remaining = round((queries_remaining / rate_per_10s) * (refresh_seconds / 60), 1)

            last_success_count = success_count

            # Print update
            print(f"[{timestamp}] Iteration {iteration}")
            print(f"  Status: {job_status['status']}")
            print(f"  Progress: {completed}/{total_queries} queries ({progress_pct}%)")
            print(f"  Success: {success_count}, Skipped: {skipped_count}, Queued: {status_counts.get('queued', (0, 0))[0]}")

            if totals['places'] > 0:
                print(f"  Places found: {totals['places']:,}")
                print(f"  Credits used: {totals['credits']} (${totals['credits'] * settings.cost_per_credit:.2f})")

            if rate_per_10s > 0 and completed < total_queries:
                print(f"  Rate: {rate_per_10s} queries/{refresh_seconds}s")
                print(f"  Est. time remaining: {est_minutes_remaining} minutes")

            # Check if complete
            if job_status['status'] == 'done':
                print()
                print("=" * 70)
                print("✅ JOB COMPLETE!")
                print("=" * 70)
                print(f"Total queries: {totals['queries']}")
                print(f"Successes: {totals['successes']}")
                print(f"Places found: {totals['places']:,}")
                print(f"Credits used: {totals['credits']} (${totals['credits'] * settings.cost_per_credit:.2f})")
                print()
                break

            print()

            # Wait before next iteration
            time.sleep(refresh_seconds)

    except KeyboardInterrupt:
        print()
        print("Monitoring stopped by user")
        print(f"Final status: {completed}/{total_queries} queries completed ({progress_pct}%)")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python scripts/monitor_job.py <job_id>")
        sys.exit(1)

    job_id = sys.argv[1]
    refresh_seconds = int(sys.argv[2]) if len(sys.argv) > 2 else 10

    monitor_job(job_id, refresh_seconds)
