"""Command-line interface for serper-tap.

Provides CLI commands for running flows and monitoring jobs.
"""

import argparse
import sys
from typing import Optional

from src.flows.create_job import create_scraping_job
from src.flows.process_batches import process_job_batches
from src.models.schemas import JobParams
from src.utils.health import get_system_health


def create_job_cli():
    """CLI entry point for creating a new scraping job.

    Usage:
        serper-create-job --keyword bars --state AZ --pages 3 --batch-size 100
    """
    parser = argparse.ArgumentParser(
        description="Create a new Serper scraping job",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    parser.add_argument(
        "--keyword",
        required=True,
        help="Search keyword (e.g., 'bars', 'restaurants')"
    )
    parser.add_argument(
        "--state",
        required=True,
        help="Two-letter state code (e.g., 'AZ', 'CA')"
    )
    parser.add_argument(
        "--pages",
        type=int,
        default=3,
        help="Number of pages to scrape per zip code (1-3)"
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=100,
        help="Number of queries to process per batch"
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=20,
        help="Number of concurrent API calls per batch"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate parameters without creating job"
    )

    args = parser.parse_args()

    # Validate state code
    if len(args.state) != 2:
        print(f"Error: State must be 2 characters (got '{args.state}')", file=sys.stderr)
        sys.exit(1)

    # Validate pages (align with JobParams limits)
    if not 1 <= args.pages <= 10:
        print(f"Error: Pages must be 1-10 (got {args.pages})", file=sys.stderr)
        sys.exit(1)

    # Create job parameters
    params = JobParams(
        keyword=args.keyword,
        state=args.state.upper(),
        pages=args.pages,
        batch_size=args.batch_size,
        concurrency=args.concurrency,
        dry_run=args.dry_run
    )

    print(f"\nCreating scraping job:")
    print(f"  Keyword: {params.keyword}")
    print(f"  State: {params.state}")
    print(f"  Pages: {params.pages}")
    print(f"  Batch size: {params.batch_size}")
    print(f"  Concurrency: {params.concurrency}")
    print(f"  Dry run: {params.dry_run}")
    print()

    # Run the flow
    try:
        result = create_scraping_job(**params.model_dump())

        print("=" * 60)
        print("JOB CREATED SUCCESSFULLY")
        print("=" * 60)
        print(f"Job ID: {result['job_id']}")
        print(f"Status: {result['status']}")
        print(f"Total queries: {result.get('total_queries', 'N/A')}")
        print(f"\nTo monitor progress:")
        print(f"  serper-monitor-job {result['job_id']}")
        print()

        return 0

    except Exception as e:
        print(f"\nError creating job: {e}", file=sys.stderr)
        return 1


def process_batches_cli():
    """CLI entry point for running the batch processor.

    Usage:
        serper-process-batches
    """
    parser = argparse.ArgumentParser(
        description="Process batches for all running jobs until complete",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    # No arguments needed - processor finds all running jobs
    parser.parse_args()

    print("\nStarting batch processor...")
    print("This will process all running jobs until complete.")
    print("Press Ctrl+C to stop gracefully.\n")

    try:
        result = process_job_batches()

        print("\n" + "=" * 60)
        print("PROCESSING COMPLETE")
        print("=" * 60)
        print(f"Batches processed: {result['total_batches_processed']}")
        print(f"Queries processed: {result['total_queries_processed']}")
        print(f"Jobs completed: {len(result['jobs_completed'])}")
        print(f"Runtime: {result['runtime_seconds']:.1f} seconds")
        print()

        return 0

    except KeyboardInterrupt:
        print("\n\nGracefully stopping batch processor...")
        print("Jobs will resume from their last state on next run.")
        return 0

    except Exception as e:
        print(f"\nError during processing: {e}", file=sys.stderr)
        return 1


def monitor_job_cli():
    """CLI entry point for monitoring a job's progress.

    Usage:
        serper-monitor-job <job-id> [--interval SECONDS]
    """
    parser = argparse.ArgumentParser(
        description="Monitor a scraping job's progress",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    parser.add_argument(
        "job_id",
        help="Job ID to monitor"
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=5,
        help="Polling interval in seconds"
    )

    args = parser.parse_args()

    # Import and run the monitor script
    # Note: This delegates to the existing scripts/monitor_job.py logic
    from src.operations.bigquery_ops import get_job_status
    import time

    print(f"\nMonitoring job: {args.job_id}")
    print(f"Polling every {args.interval} seconds (Ctrl+C to stop)\n")

    try:
        while True:
            try:
                status = get_job_status(args.job_id)

                # Clear screen (optional)
                # print("\033c", end="")

                print("=" * 60)
                print(f"Job: {status['job_id']}")
                print(f"Status: {status['status']}")
                print(f"Keyword: {status['keyword']} | State: {status['state']}")
                print("=" * 60)

                totals = status['totals']
                print(f"Queries: {totals['successes']}/{totals['queries']} complete")
                print(f"Places: {totals['places']}")
                print(f"Credits: {totals['credits']}")
                print(f"Failures: {totals['failures']}")

                if status['status'] == 'done':
                    print("\n✓ Job completed!")
                    return 0

                print(f"\nNext update in {args.interval}s...")
                time.sleep(args.interval)

            except ValueError as e:
                print(f"\nError: {e}", file=sys.stderr)
                return 1

    except KeyboardInterrupt:
        print("\n\nStopped monitoring.")
        return 0

    except Exception as e:
        print(f"\nError monitoring job: {e}", file=sys.stderr)
        return 1


def health_check_cli():
    """CLI entry point for system health check.

    Usage:
        serper-health-check
    """
    parser = argparse.ArgumentParser(
        description="Check system health and configuration",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    parser.add_argument(
        "--json",
        action="store_true",
        help="Output health status as JSON"
    )

    args = parser.parse_args()

    try:
        from datetime import datetime, timezone
        health = get_system_health()
        health["timestamp"] = datetime.now(timezone.utc).isoformat()

        if args.json:
            import json
            print(json.dumps(health, indent=2))
        else:
            # Human-readable output
            print("\n" + "=" * 60)
            print("SYSTEM HEALTH CHECK")
            print("=" * 60)

            status_emoji = "✅" if health["status"] == "healthy" else "❌"
            print(f"\nOverall Status: {status_emoji} {health['status'].upper()}")
            print(f"Timestamp: {health['timestamp']}")

            print("\n" + "-" * 60)
            print("COMPONENT HEALTH")
            print("-" * 60)

            for component_name, component in health["components"].items():
                status_emoji = "✅" if component["status"] == "healthy" else "❌"
                print(f"\n{component_name.upper()}: {status_emoji} {component['status']}")
                print(f"  Message: {component['message']}")

                if "issues" in component:
                    print("  Issues:")
                    for issue in component["issues"]:
                        print(f"    - {issue}")

                if "error" in component:
                    print(f"  Error: {component['error']}")

                # Show additional details
                for key, value in component.items():
                    if key not in ["status", "message", "issues", "error"]:
                        print(f"  {key}: {value}")

            print()

        # Exit code: 0 if healthy, 1 if unhealthy
        return 0 if health["status"] == "healthy" else 1

    except Exception as e:
        print(f"\nError running health check: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    # For testing CLI directly
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == "create":
        sys.exit(create_job_cli())
    elif len(sys.argv) > 1 and sys.argv[1] == "process":
        sys.exit(process_batches_cli())
    elif len(sys.argv) > 1 and sys.argv[1] == "monitor":
        sys.argv.pop(1)  # Remove 'monitor' argument
        sys.exit(monitor_job_cli())
    elif len(sys.argv) > 1 and sys.argv[1] == "health":
        sys.exit(health_check_cli())
    else:
        print("Usage: python -m src.cli {create|process|monitor|health}")
        sys.exit(1)
