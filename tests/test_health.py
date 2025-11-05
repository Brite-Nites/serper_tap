"""Tests for health check utilities."""

from unittest.mock import MagicMock

from src.utils.health import (
    check_bigquery_connection,
    check_configuration,
    get_system_health,
)


class TestCheckConfiguration:
    """Test configuration health check."""

    def test_healthy_configuration(self):
        """Test that all required configuration is present."""
        result = check_configuration()

        assert result["status"] == "healthy"
        assert "required configuration present" in result["message"].lower()
        assert result["use_mock_api"] is True
        assert result["early_exit_threshold"] == 10

    def test_missing_credentials(self, monkeypatch):
        """Test configuration check when credentials are missing."""
        monkeypatch.setenv("GOOGLE_APPLICATION_CREDENTIALS", "")

        result = check_configuration()

        assert result["status"] == "unhealthy"
        assert "issues" in result
        assert any("CREDENTIALS" in issue for issue in result["issues"])

    def test_missing_project_id(self, monkeypatch):
        """Test configuration check when project ID is missing."""
        monkeypatch.setenv("BIGQUERY_PROJECT_ID", "")

        result = check_configuration()

        assert result["status"] == "unhealthy"
        assert any("PROJECT_ID" in issue for issue in result["issues"])

    def test_missing_api_key_with_real_api(self, monkeypatch):
        """Test that API key is required when use_mock_api=False."""
        monkeypatch.setenv("USE_MOCK_API", "false")
        monkeypatch.setenv("SERPER_API_KEY", "")

        result = check_configuration()

        assert result["status"] == "unhealthy"
        assert any("API_KEY" in issue for issue in result["issues"])


class TestCheckBigQueryConnection:
    """Test BigQuery connection health check."""

    def test_healthy_connection(self, mock_bigquery_client):
        """Test successful BigQuery connection."""
        # Mock successful query
        mock_result = MagicMock()
        mock_result.health_check = 1
        mock_bigquery_client.query.return_value.result.return_value = [mock_result]

        result = check_bigquery_connection()

        assert result["status"] == "healthy"
        assert "successful" in result["message"].lower()
        assert result["project_id"] == "test-project"
        assert result["dataset"] == "test_dataset"

    def test_connection_failure(self, mock_bigquery_client):
        """Test BigQuery connection failure."""
        # Mock connection error
        mock_bigquery_client.query.side_effect = Exception("Connection refused")

        result = check_bigquery_connection()

        assert result["status"] == "unhealthy"
        assert "failed" in result["message"].lower()
        assert "error" in result
        assert "Connection refused" in result["error"]

    def test_unexpected_query_result(self, mock_bigquery_client):
        """Test when query returns unexpected result."""
        # Mock unexpected result
        mock_result = MagicMock()
        mock_result.health_check = 0  # Wrong value
        mock_bigquery_client.query.return_value.result.return_value = [mock_result]

        result = check_bigquery_connection()

        assert result["status"] == "unhealthy"
        assert "unexpected result" in result["message"].lower()


class TestGetSystemHealth:
    """Test overall system health check."""

    def test_all_healthy(self, mock_bigquery_client):
        """Test when all components are healthy."""
        # Mock successful BigQuery connection
        mock_result = MagicMock()
        mock_result.health_check = 1
        mock_bigquery_client.query.return_value.result.return_value = [mock_result]

        result = get_system_health()

        assert result["status"] == "healthy"
        assert "components" in result
        assert result["components"]["configuration"]["status"] == "healthy"
        assert result["components"]["bigquery"]["status"] == "healthy"

    def test_bigquery_unhealthy(self, mock_bigquery_client):
        """Test when BigQuery is unhealthy."""
        # Mock BigQuery connection error
        mock_bigquery_client.query.side_effect = Exception("Connection error")

        result = get_system_health()

        assert result["status"] == "unhealthy"
        assert result["components"]["configuration"]["status"] == "healthy"
        assert result["components"]["bigquery"]["status"] == "unhealthy"

    def test_configuration_unhealthy(self, monkeypatch, mock_bigquery_client):
        """Test when configuration is unhealthy."""
        # Remove required config
        monkeypatch.setenv("GOOGLE_APPLICATION_CREDENTIALS", "")

        # Mock successful BigQuery (won't be reached due to config failure)
        mock_result = MagicMock()
        mock_result.health_check = 1
        mock_bigquery_client.query.return_value.result.return_value = [mock_result]

        result = get_system_health()

        assert result["status"] == "unhealthy"
        assert result["components"]["configuration"]["status"] == "unhealthy"

    def test_all_unhealthy(self, monkeypatch, mock_bigquery_client):
        """Test when all components are unhealthy."""
        # Remove required config
        monkeypatch.setenv("BIGQUERY_PROJECT_ID", "")

        # Mock BigQuery connection error
        mock_bigquery_client.query.side_effect = Exception("Connection error")

        result = get_system_health()

        assert result["status"] == "unhealthy"
        assert result["components"]["configuration"]["status"] == "unhealthy"
        assert result["components"]["bigquery"]["status"] == "unhealthy"
