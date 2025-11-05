#!/usr/bin/env python3
"""Clear all running jobs from the queue.

Use this to clean up test jobs before running production workloads.

Usage:
    # Dry run (shows what would be cleared)
    python scripts/clear_running_jobs.py

    # Actually clear jobs
    python scripts/clear_running_jobs.py --confirm

    # Exclude specific job IDs from clearing
    python scripts/clear_running_jobs.py --confirm --keep 096c367e-5042-4c4b-a522-1c9cc3776c63
"""

import argparse
import sys

from google.cloud import bigquery
from src.operations.bigquery_ops import get_running_jobs
from src.utils.bigquery_client import execute_dml
from src.utils.config import settings


def clear_running_jobs(confirm: bool = False, keep_job_ids: list[str] = None):
    """Mark all running jobs as failed.

    Args:
        confirm: If True, actually execute the update. If False, dry run.
        keep_job_ids: List of job IDs to preserve (not clear)
    """
    keep_job_ids = keep_job_ids or []

    # Get all running jobs
    running_jobs = get_running_jobs()

    if not running_jobs:
        print("No running jobs found. Queue is already clear.")
        return

    # Filter out jobs to keep
    jobs_to_clear = [j for j in running_jobs if j['job_id'] not in keep_job_ids]
    jobs_to_keep = [j for j in running_jobs if j['job_id'] in keep_job_ids]

    print("=" * 70)
    print("RUNNING JOBS FOUND")
    print("=" * 70)
    print(f"Total running: {len(running_jobs)}")
    print(f"To be cleared: {len(jobs_to_clear)}")
    print(f"To be kept: {len(jobs_to_keep)}")
    print()

    if jobs_to_keep:
        print("Jobs that will be KEPT:")
        for job in jobs_to_keep:
            print(f"  ✓ {job['job_id']} - {job['keyword']} × {job['state']}")
        print()

    if not jobs_to_clear:
        print("No jobs to clear after filtering.")
        return

    print("Jobs that will be CLEARED:")
    for job in jobs_to_clear[:10]:  # Show first 10
        print(f"  ✗ {job['job_id']} - {job['keyword']} × {job['state']}")
    if len(jobs_to_clear) > 10:
        print(f"  ... and {len(jobs_to_clear) - 10} more")
    print()

    # Dry run mode
    if not confirm:
        print("=" * 70)
        print("DRY RUN MODE")
        print("=" * 70)
        print("This was a dry run. No changes were made.")
        print(f"Run with --confirm to actually clear {len(jobs_to_clear)} jobs.")
        print()
        return

    # Confirm execution
    print("=" * 70)
    print("⚠️  CONFIRMATION REQUIRED")
    print("=" * 70)
    print(f"About to mark {len(jobs_to_clear)} jobs as 'failed'.")
    print()
    response = input("Type 'yes' to confirm: ")

    if response.lower() != 'yes':
        print("Cancelled. No changes made.")
        return

    # Build list of job IDs to clear
    job_ids_to_clear = [j['job_id'] for j in jobs_to_clear]

    # Update jobs to failed status
    print()
    print("Clearing jobs...")

    update_query = f"""
    UPDATE `{settings.bigquery_project_id}.{settings.bigquery_dataset}.serper_jobs`
    SET
        status = 'failed',
        finished_at = CURRENT_TIMESTAMP(),
        totals.failures = totals.queries
    WHERE job_id IN UNNEST(@job_ids)
      AND status = 'running'
    """

    parameters = [
        bigquery.ArrayQueryParameter("job_ids", "STRING", job_ids_to_clear)
    ]

    rows_updated = execute_dml(update_query, parameters)

    print()
    print("=" * 70)
    print("✅ JOBS CLEARED")
    print("=" * 70)
    print(f"Updated {rows_updated} jobs to status='failed'")
    print(f"Error message: 'manually_cleared'")
    print()

    # Show remaining running jobs
    remaining_jobs = get_running_jobs()
    print(f"Remaining running jobs: {len(remaining_jobs)}")
    if remaining_jobs:
        print()
        print("Still running:")
        for job in remaining_jobs:
            print(f"  {job['job_id']} - {job['keyword']} × {job['state']}")
    else:
        print("Queue is now empty!")
    print()


def main():
    parser = argparse.ArgumentParser(
        description="Clear running jobs from the queue",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Dry run (see what would be cleared)
  python scripts/clear_running_jobs.py

  # Actually clear all running jobs
  python scripts/clear_running_jobs.py --confirm

  # Clear all except specific job
  python scripts/clear_running_jobs.py --confirm --keep 096c367e-5042-4c4b-a522-1c9cc3776c63
        """
    )

    parser.add_argument(
        "--confirm",
        action="store_true",
        help="Actually execute the clear (default is dry run)"
    )

    parser.add_argument(
        "--keep",
        action="append",
        metavar="JOB_ID",
        help="Job ID to keep (can be specified multiple times)"
    )

    args = parser.parse_args()

    try:
        clear_running_jobs(confirm=args.confirm, keep_job_ids=args.keep)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
