"""BigQuery client connection management.

Provides a factory function for creating BigQuery client instances
with proper authentication and configuration.
"""

import os
from functools import lru_cache

from google.cloud import bigquery
from google.oauth2 import service_account

from src.utils.config import settings


@lru_cache(maxsize=1)
def get_bigquery_client() -> bigquery.Client:
    """Get a configured BigQuery client instance.

    This function is cached to ensure we reuse the same client instance
    across multiple calls, which is more efficient for connection pooling.

    Returns:
        bigquery.Client: Configured BigQuery client

    Raises:
        FileNotFoundError: If credentials file doesn't exist
        ValueError: If credentials are invalid
    """
    credentials_path = settings.google_application_credentials

    # Validate credentials file exists
    if not os.path.exists(credentials_path):
        raise FileNotFoundError(
            f"Google Cloud credentials file not found: {credentials_path}\n"
            f"Please ensure GOOGLE_APPLICATION_CREDENTIALS points to a valid service account key file."
        )

    # Load credentials from service account file
    credentials = service_account.Credentials.from_service_account_file(
        credentials_path,
        scopes=["https://www.googleapis.com/auth/bigquery"]
    )

    # Create and return BigQuery client
    client = bigquery.Client(
        credentials=credentials,
        project=settings.bigquery_project_id
    )

    return client


def execute_query(
    query: str,
    parameters: list[bigquery.ScalarQueryParameter] | None = None
) -> bigquery.table.RowIterator:
    """Execute a parameterized BigQuery query.

    Args:
        query: SQL query string
        parameters: Optional list of query parameters for parameterized queries

    Returns:
        RowIterator: Query results iterator

    Raises:
        google.cloud.exceptions.GoogleCloudError: If query execution fails
    """
    client = get_bigquery_client()

    job_config = bigquery.QueryJobConfig()
    if parameters:
        job_config.query_parameters = parameters

    query_job = client.query(query, job_config=job_config)
    return query_job.result()


def execute_dml(
    query: str,
    parameters: list[bigquery.ScalarQueryParameter] | None = None
) -> int:
    """Execute a DML statement (INSERT, UPDATE, DELETE, MERGE) and return rows affected.

    Args:
        query: DML statement string
        parameters: Optional list of query parameters

    Returns:
        int: Number of rows affected by the DML statement

    Raises:
        google.cloud.exceptions.GoogleCloudError: If execution fails
    """
    client = get_bigquery_client()

    job_config = bigquery.QueryJobConfig()
    if parameters:
        job_config.query_parameters = parameters

    query_job = client.query(query, job_config=job_config)
    result = query_job.result()

    # For DML statements, num_dml_affected_rows contains the count
    return query_job.num_dml_affected_rows or 0
