#!/usr/bin/env python3
"""Quick job progress check."""
import sys

from src.operations import get_job_status

if __name__ == "__main__":
    job_id = sys.argv[1] if len(sys.argv) > 1 else None
    if not job_id:
        print("Usage: python scripts/check_job_progress.py <job_id>")
        sys.exit(1)

    status = get_job_status(job_id)
    totals = status['totals']

    print(f"\nJob: {job_id}")
    print(f"Status: {status['status']}")
    print(f"Queries: {totals['successes']}/{totals['queries']} complete")
    print(f"Failed: {totals['failures']}")
    print(f"Places: {totals['places']}")
    print(f"Credits: {totals['credits']}")

    if totals['queries'] > 0:
        pct = (totals['successes'] / totals['queries']) * 100
        print(f"Progress: {pct:.1f}%\n")
