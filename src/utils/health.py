"""Health check utilities for system monitoring.

Provides functions to check the health of various system components.
"""

from typing import Any

from src.utils.bigquery_client import get_bigquery_client
from src.utils.config import settings


def check_bigquery_connection() -> dict[str, Any]:
    """Check if BigQuery connection is working.

    Returns:
        Dict with status, message, and optional error details
    """
    try:
        client = get_bigquery_client()

        # Simple query to test connection
        query = f"""
        SELECT 1 as health_check
        """

        query_job = client.query(query)
        result = list(query_job.result())

        if result and result[0].health_check == 1:
            return {
                "status": "healthy",
                "message": "BigQuery connection successful",
                "project_id": settings.bigquery_project_id,
                "dataset": settings.bigquery_dataset
            }
        else:
            return {
                "status": "unhealthy",
                "message": "BigQuery query returned unexpected result"
            }

    except Exception as e:
        return {
            "status": "unhealthy",
            "message": f"BigQuery connection failed: {type(e).__name__}",
            "error": str(e)
        }


def check_configuration() -> dict[str, Any]:
    """Check if required configuration is present.

    Returns:
        Dict with status and configuration details
    """
    issues = []

    # Check required settings
    if not settings.google_application_credentials:
        issues.append("GOOGLE_APPLICATION_CREDENTIALS not set")

    if not settings.bigquery_project_id:
        issues.append("BIGQUERY_PROJECT_ID not set")

    if not settings.use_mock_api and not settings.serper_api_key:
        issues.append("SERPER_API_KEY not set (required when use_mock_api=False)")

    if issues:
        return {
            "status": "unhealthy",
            "message": "Configuration issues detected",
            "issues": issues
        }
    else:
        return {
            "status": "healthy",
            "message": "All required configuration present",
            "use_mock_api": settings.use_mock_api,
            "early_exit_threshold": settings.early_exit_threshold
        }


def get_system_health() -> dict[str, Any]:
    """Get overall system health status.

    Returns:
        Dict with overall status and component health checks
    """
    config_health = check_configuration()
    bigquery_health = check_bigquery_connection()

    # System is healthy only if all components are healthy
    overall_healthy = (
        config_health["status"] == "healthy" and
        bigquery_health["status"] == "healthy"
    )

    return {
        "status": "healthy" if overall_healthy else "unhealthy",
        "timestamp": None,  # Will be set by caller if needed
        "components": {
            "configuration": config_health,
            "bigquery": bigquery_health
        }
    }
