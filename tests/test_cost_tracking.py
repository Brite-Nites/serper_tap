"""Tests for cost tracking and budget management."""

from datetime import datetime

from src.utils.cost_tracking import (
    check_budget_status,
    estimate_job_cost,
    get_daily_credit_usage,
    validate_budget_for_job,
)


class TestGetDailyCreditUsage:
    """Test daily credit usage calculation."""

    def test_with_usage(self, mock_execute_query, sample_bigquery_row, mock_datetime_utcnow):
        """Test credit usage when there are jobs."""
        # Mock BigQuery response
        mock_row = sample_bigquery_row(total_credits=500, job_count=5)
        mock_execute_query.return_value = [mock_row]

        result = get_daily_credit_usage()

        assert result["total_credits"] == 500
        assert result["total_cost_usd"] == 5.0  # 500 * 0.01
        assert result["job_count"] == 5
        assert result["date"] == "2025-01-01"
        assert result["daily_budget_usd"] == 100.0

    def test_with_no_usage(self, mock_execute_query, sample_bigquery_row):
        """Test credit usage when there are no jobs."""
        mock_row = sample_bigquery_row(total_credits=0, job_count=0)
        mock_execute_query.return_value = [mock_row]

        result = get_daily_credit_usage()

        assert result["total_credits"] == 0
        assert result["total_cost_usd"] == 0.0
        assert result["job_count"] == 0

    def test_with_custom_date(self, mock_execute_query, sample_bigquery_row):
        """Test credit usage for a specific date."""
        mock_row = sample_bigquery_row(total_credits=100, job_count=1)
        mock_execute_query.return_value = [mock_row]

        custom_date = datetime(2025, 6, 15, 14, 30, 0)
        result = get_daily_credit_usage(date=custom_date)

        assert result["date"] == "2025-06-15"
        assert result["total_credits"] == 100


class TestEstimateJobCost:
    """Test job cost estimation."""

    def test_small_job(self):
        """Test cost estimation for a small job."""
        result = estimate_job_cost(100)

        assert result["num_queries"] == 100
        assert result["estimated_credits"] == 100
        assert result["estimated_cost_usd"] == 1.0
        assert result["cost_per_credit"] == 0.01

    def test_large_job(self):
        """Test cost estimation for a large job (California-sized)."""
        result = estimate_job_cost(5301)  # CA: 1767 zips × 3 pages

        assert result["num_queries"] == 5301
        assert result["estimated_credits"] == 5301
        assert result["estimated_cost_usd"] == 53.01

    def test_zero_queries(self):
        """Test cost estimation for zero queries."""
        result = estimate_job_cost(0)

        assert result["num_queries"] == 0
        assert result["estimated_credits"] == 0
        assert result["estimated_cost_usd"] == 0.0


class TestCheckBudgetStatus:
    """Test budget status checking."""

    def test_ok_status(self, mock_execute_query, sample_bigquery_row):
        """Test budget status when well below limits."""
        # Usage: $20 / $100 budget = 20%
        mock_row = sample_bigquery_row(total_credits=2000, job_count=10)
        mock_execute_query.return_value = [mock_row]

        result = check_budget_status()

        assert result["status"] == "ok"
        assert result["block_new_jobs"] is False
        assert result["budget_used_pct"] == 20.0
        assert result["remaining_budget_usd"] == 80.0
        assert result["total_cost_usd"] == 20.0

    def test_warning_status(self, mock_execute_query, sample_bigquery_row):
        """Test budget status when approaching limits."""
        # Usage: $85 / $100 budget = 85% (above 80% soft threshold)
        mock_row = sample_bigquery_row(total_credits=8500, job_count=50)
        mock_execute_query.return_value = [mock_row]

        result = check_budget_status()

        assert result["status"] == "warning"
        assert result["block_new_jobs"] is False
        assert result["budget_used_pct"] == 85.0
        assert result["remaining_budget_usd"] == 15.0

    def test_exceeded_status(self, mock_execute_query, sample_bigquery_row):
        """Test budget status when budget is exceeded."""
        # Usage: $120 / $100 budget = 120% (above 100% hard threshold)
        mock_row = sample_bigquery_row(total_credits=12000, job_count=100)
        mock_execute_query.return_value = [mock_row]

        result = check_budget_status()

        assert result["status"] == "exceeded"
        assert result["block_new_jobs"] is True
        assert result["budget_used_pct"] == 120.0
        assert result["remaining_budget_usd"] == -20.0


class TestValidateBudgetForJob:
    """Test budget validation for new jobs."""

    def test_job_allowed_within_budget(self, mock_execute_query, sample_bigquery_row):
        """Test that job is allowed when within budget."""
        # Current usage: $20
        mock_row = sample_bigquery_row(total_credits=2000, job_count=10)
        mock_execute_query.return_value = [mock_row]

        # New job: 1000 queries = $10
        # Total would be $30 / $100 = 30% (OK)
        result = validate_budget_for_job(1000)

        assert result["allowed"] is True
        assert result["reason"] == "within_budget"
        assert "within budget" in result["message"].lower()
        assert result["job_estimate"]["estimated_cost_usd"] == 10.0

    def test_job_would_exceed_budget(self, mock_execute_query, sample_bigquery_row):
        """Test that job is blocked when it would exceed budget."""
        # Current usage: $80
        mock_row = sample_bigquery_row(total_credits=8000, job_count=50)
        mock_execute_query.return_value = [mock_row]

        # New job: 3000 queries = $30
        # Total would be $110 / $100 = 110% (BLOCKED)
        result = validate_budget_for_job(3000)

        assert result["allowed"] is False
        assert result["reason"] == "would_exceed_budget"
        assert "would exceed" in result["message"].lower()
        assert result["job_estimate"]["estimated_cost_usd"] == 30.0

    def test_job_blocked_budget_already_exceeded(
        self, mock_execute_query, sample_bigquery_row
    ):
        """Test that job is blocked when budget already exceeded."""
        # Current usage: $120 (already exceeded)
        mock_row = sample_bigquery_row(total_credits=12000, job_count=100)
        mock_execute_query.return_value = [mock_row]

        # Any new job should be blocked
        result = validate_budget_for_job(100)

        assert result["allowed"] is False
        assert result["reason"] == "daily_budget_exceeded"
        assert "already exceeded" in result["message"].lower()

    def test_large_job_validation(self, mock_execute_query, sample_bigquery_row):
        """Test budget validation for a large state like California."""
        # Current usage: $0
        mock_row = sample_bigquery_row(total_credits=0, job_count=0)
        mock_execute_query.return_value = [mock_row]

        # California: 1767 zips × 3 pages = 5301 queries = $53.01
        # This would exceed $50 budget
        result = validate_budget_for_job(5301)

        # With default $100 budget, should be allowed
        assert result["allowed"] is True

    def test_zero_query_job(self, mock_execute_query, sample_bigquery_row):
        """Test validation for a job with zero queries."""
        mock_row = sample_bigquery_row(total_credits=0, job_count=0)
        mock_execute_query.return_value = [mock_row]

        result = validate_budget_for_job(0)

        assert result["allowed"] is True
        assert result["job_estimate"]["estimated_cost_usd"] == 0.0
