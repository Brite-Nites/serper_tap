"""Pytest configuration and shared fixtures.

This file is automatically loaded by pytest and provides fixtures
that can be used across all test files.
"""

from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest


@pytest.fixture(autouse=True)
def mock_env_vars(monkeypatch):
    """Set required environment variables for all tests.

    This fixture runs automatically for all tests (autouse=True)
    to ensure a consistent test environment.
    """
    # Set minimal required env vars
    monkeypatch.setenv("GOOGLE_APPLICATION_CREDENTIALS", "/fake/path/to/credentials.json")
    monkeypatch.setenv("BIGQUERY_PROJECT_ID", "test-project")
    monkeypatch.setenv("BIGQUERY_DATASET", "test_dataset")
    monkeypatch.setenv("USE_MOCK_API", "true")
    monkeypatch.setenv("SERPER_API_KEY", "")  # Not needed for mock


@pytest.fixture
def mock_bigquery_client():
    """Mock BigQuery client for testing database operations."""
    with patch("src.utils.bigquery_client.get_bigquery_client") as mock:
        client = MagicMock()
        mock.return_value = client
        yield client


@pytest.fixture
def mock_execute_query():
    """Mock execute_query function."""
    with patch("src.utils.bigquery_client.execute_query") as mock:
        yield mock


@pytest.fixture
def mock_execute_dml():
    """Mock execute_dml function."""
    with patch("src.utils.bigquery_client.execute_dml") as mock:
        yield mock


@pytest.fixture
def sample_job_params():
    """Sample job parameters for testing."""
    return {
        "keyword": "bars",
        "state": "AZ",
        "pages": 3,
        "batch_size": 100,
        "concurrency": 20,
        "dry_run": False
    }


@pytest.fixture
def sample_queries():
    """Sample queries for testing."""
    return [
        {"zip": "85001", "page": 1, "q": "85001 bars"},
        {"zip": "85001", "page": 2, "q": "85001 bars"},
        {"zip": "85002", "page": 1, "q": "85002 bars"},
    ]


@pytest.fixture
def sample_places():
    """Sample place data for testing."""
    return [
        {
            "keyword": "bars",
            "state": "AZ",
            "zip": "85001",
            "page": 1,
            "place_uid": "ChIJ123",
            "payload": {
                "title": "Test Bar 1",
                "address": "123 Main St",
                "placeId": "ChIJ123"
            },
            "api_status": 200,
            "results_count": 10,
            "credits": 1
        },
        {
            "keyword": "bars",
            "state": "AZ",
            "zip": "85001",
            "page": 1,
            "place_uid": "ChIJ456",
            "payload": {
                "title": "Test Bar 2",
                "address": "456 Main St",
                "placeId": "ChIJ456"
            },
            "api_status": 200,
            "results_count": 10,
            "credits": 1
        }
    ]


@pytest.fixture
def sample_bigquery_row():
    """Create a mock BigQuery row object."""
    class MockRow:
        def __init__(self, **kwargs):
            for key, value in kwargs.items():
                setattr(self, key, value)

    return MockRow


@pytest.fixture
def mock_datetime_utcnow():
    """Mock datetime.utcnow for consistent timestamps in tests."""
    fixed_time = datetime(2025, 1, 1, 12, 0, 0)
    with patch("src.utils.cost_tracking.datetime") as mock_dt:
        mock_dt.utcnow.return_value = fixed_time
        mock_dt.side_effect = lambda *args, **kw: datetime(*args, **kw)
        yield fixed_time
