"""Cost tracking and budget management utilities.

Provides functions to track API credit usage, calculate costs, and enforce budget limits.
"""

from datetime import datetime, timedelta
from typing import Any

from google.cloud import bigquery

from src.utils.bigquery_client import execute_query
from src.utils.config import settings


def get_daily_credit_usage(date: datetime | None = None) -> dict[str, Any]:
    """Get credit usage for a specific date.

    Args:
        date: Date to check (defaults to today UTC)

    Returns:
        Dict with total_credits, total_cost_usd, job_count
    """
    if date is None:
        date = datetime.utcnow()

    # Set to start of day UTC
    start_of_day = date.replace(hour=0, minute=0, second=0, microsecond=0)
    end_of_day = start_of_day + timedelta(days=1)

    query = f"""
    SELECT
        COALESCE(SUM(totals.credits), 0) as total_credits,
        COUNT(*) as job_count
    FROM `{settings.bigquery_project_id}.{settings.bigquery_dataset}.serper_jobs`
    WHERE created_at >= @start_date
      AND created_at < @end_date
    """

    params = [
        bigquery.ScalarQueryParameter("start_date", "TIMESTAMP", start_of_day),
        bigquery.ScalarQueryParameter("end_date", "TIMESTAMP", end_of_day)
    ]

    results = execute_query(query, params)
    row = next(iter(results), None)

    if row:
        total_credits = row.total_credits
        job_count = row.job_count
    else:
        total_credits = 0
        job_count = 0

    total_cost_usd = total_credits * settings.cost_per_credit

    return {
        "date": start_of_day.strftime("%Y-%m-%d"),
        "total_credits": int(total_credits),
        "total_cost_usd": round(total_cost_usd, 2),
        "job_count": job_count,
        "daily_budget_usd": settings.daily_budget_usd
    }


def check_budget_status() -> dict[str, Any]:
    """Check current budget status for today.

    Returns:
        Dict with budget status, usage, and whether new jobs should be blocked
    """
    usage = get_daily_credit_usage()

    # Calculate percentage of budget used
    budget_used_pct = 0
    if settings.daily_budget_usd > 0:
        budget_used_pct = (usage["total_cost_usd"] / settings.daily_budget_usd) * 100

    # Determine budget status
    if budget_used_pct >= settings.budget_hard_threshold_pct:
        status = "exceeded"
        block_new_jobs = True
        message = f"Daily budget exceeded ({budget_used_pct:.1f}% of ${settings.daily_budget_usd})"
    elif budget_used_pct >= settings.budget_soft_threshold_pct:
        status = "warning"
        block_new_jobs = False
        message = f"Approaching daily budget ({budget_used_pct:.1f}% of ${settings.daily_budget_usd})"
    else:
        status = "ok"
        block_new_jobs = False
        message = f"Within daily budget ({budget_used_pct:.1f}% of ${settings.daily_budget_usd})"

    return {
        "status": status,
        "message": message,
        "block_new_jobs": block_new_jobs,
        "budget_used_pct": round(budget_used_pct, 1),
        "remaining_budget_usd": round(settings.daily_budget_usd - usage["total_cost_usd"], 2),
        **usage
    }


def estimate_job_cost(num_queries: int) -> dict[str, Any]:
    """Estimate the cost of a job based on number of queries.

    Args:
        num_queries: Number of queries the job will execute

    Returns:
        Dict with estimated credits and cost
    """
    # Each query consumes 1 credit
    estimated_credits = num_queries
    estimated_cost_usd = estimated_credits * settings.cost_per_credit

    return {
        "num_queries": num_queries,
        "estimated_credits": estimated_credits,
        "estimated_cost_usd": round(estimated_cost_usd, 2),
        "cost_per_credit": settings.cost_per_credit
    }


def validate_budget_for_job(num_queries: int) -> dict[str, Any]:
    """Validate if a new job would exceed budget limits.

    Args:
        num_queries: Number of queries the job will execute

    Returns:
        Dict with validation result and details
    """
    budget_status = check_budget_status()
    job_estimate = estimate_job_cost(num_queries)

    # Check if budget is already exceeded
    if budget_status["block_new_jobs"]:
        return {
            "allowed": False,
            "reason": "daily_budget_exceeded",
            "message": f"Daily budget already exceeded. Current usage: ${budget_status['total_cost_usd']} / ${settings.daily_budget_usd}",
            "budget_status": budget_status,
            "job_estimate": job_estimate
        }

    # Check if this job would exceed budget
    projected_cost = budget_status["total_cost_usd"] + job_estimate["estimated_cost_usd"]
    if projected_cost > settings.daily_budget_usd:
        return {
            "allowed": False,
            "reason": "would_exceed_budget",
            "message": (
                f"Job would exceed daily budget. "
                f"Current: ${budget_status['total_cost_usd']}, "
                f"Job: ${job_estimate['estimated_cost_usd']}, "
                f"Total: ${projected_cost:.2f}, "
                f"Budget: ${settings.daily_budget_usd}"
            ),
            "budget_status": budget_status,
            "job_estimate": job_estimate
        }

    # Budget OK
    return {
        "allowed": True,
        "reason": "within_budget",
        "message": f"Job within budget ({projected_cost:.2f} / ${settings.daily_budget_usd})",
        "budget_status": budget_status,
        "job_estimate": job_estimate
    }
