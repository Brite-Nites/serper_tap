"""Tests for configuration management."""

import pytest
from pydantic import ValidationError

from src.models.schemas import JobParams
from src.utils.config import settings


class TestSettings:
    """Test configuration settings."""

    def test_settings_loaded(self):
        """Test that settings are loaded with default values."""
        assert settings.bigquery_project_id == "test-project"
        assert settings.bigquery_dataset == "test_dataset"
        assert settings.use_mock_api is True

    def test_default_values(self):
        """Test that default values are set correctly."""
        assert settings.default_batch_size == 100
        assert settings.default_concurrency == 20
        assert settings.default_pages == 3
        assert settings.processor_max_workers == 100
        assert settings.early_exit_threshold == 10

    def test_cost_settings(self):
        """Test cost management settings."""
        assert settings.daily_budget_usd == 100.0
        assert settings.cost_per_credit == 0.01
        assert settings.budget_soft_threshold_pct == 80
        assert settings.budget_hard_threshold_pct == 100

    def test_api_settings(self):
        """Test API configuration."""
        assert settings.serper_timeout_seconds == 30.0
        assert settings.serper_retries == 3
        assert settings.serper_retry_delay_seconds == 5


class TestJobParams:
    """Test JobParams validation."""

    def test_valid_params(self, sample_job_params):
        """Test that valid params are accepted."""
        params = JobParams(**sample_job_params)
        assert params.keyword == "bars"
        assert params.state == "AZ"
        assert params.pages == 3
        assert params.batch_size == 100
        assert params.concurrency == 20
        assert params.dry_run is False

    def test_state_normalization(self):
        """Test that state is normalized to uppercase."""
        params = JobParams(
            keyword="bars",
            state="az",  # lowercase
            pages=3,
            batch_size=100,
            concurrency=20
        )
        assert params.state == "AZ"  # should be uppercase

    def test_invalid_state_length(self):
        """Test that invalid state length is rejected."""
        with pytest.raises(ValidationError) as exc_info:
            JobParams(
                keyword="bars",
                state="AZZ",  # too long
                pages=3,
                batch_size=100,
                concurrency=20
            )
        assert "state" in str(exc_info.value).lower()

    def test_invalid_pages(self):
        """Test that invalid page numbers are rejected."""
        with pytest.raises(ValidationError):
            JobParams(
                keyword="bars",
                state="AZ",
                pages=0,  # too small
                batch_size=100,
                concurrency=20
            )

        with pytest.raises(ValidationError):
            JobParams(
                keyword="bars",
                state="AZ",
                pages=4,  # too large
                batch_size=100,
                concurrency=20
            )

    def test_invalid_batch_size(self):
        """Test that invalid batch sizes are rejected."""
        with pytest.raises(ValidationError):
            JobParams(
                keyword="bars",
                state="AZ",
                pages=3,
                batch_size=0,  # too small
                concurrency=20
            )

    def test_invalid_concurrency(self):
        """Test that invalid concurrency values are rejected."""
        with pytest.raises(ValidationError):
            JobParams(
                keyword="bars",
                state="AZ",
                pages=3,
                batch_size=100,
                concurrency=0  # too small
            )

    def test_empty_keyword(self):
        """Test that empty keyword is rejected."""
        with pytest.raises(ValidationError):
            JobParams(
                keyword="",  # empty
                state="AZ",
                pages=3,
                batch_size=100,
                concurrency=20
            )

    def test_defaults_applied(self):
        """Test that defaults are applied from settings."""
        params = JobParams(
            keyword="bars",
            state="AZ"
        )
        assert params.pages == settings.default_pages
        assert params.batch_size == settings.default_batch_size
        assert params.concurrency == settings.default_concurrency
        assert params.dry_run is False
