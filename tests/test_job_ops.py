"""Unit tests for src/operations/job_ops.py

Tests job lifecycle operations including job creation, status retrieval,
and statistics management. These are the entry points for the pipeline.
"""

from unittest.mock import MagicMock, patch

import pytest
from google.cloud import bigquery

from src.models.schemas import JobParams
from src.operations.job_ops import create_job


class TestCreateJob:
    """Test job creation operation.

    The create_job function is the pipeline entry point - it initializes
    a new scraping job in BigQuery. This must correctly insert job metadata
    and return the right structure for downstream processing.
    """

    def test_create_job_happy_path_standard(self, mock_execute_dml):
        """Test successful creation of a standard (non-dry-run) job.

        This is the most common case: create a job with standard parameters
        and verify it's inserted into BigQuery with status='running'.
        """
        # Arrange: Create standard job parameters
        params = JobParams(
            keyword="bars",
            state="AZ",
            pages=3,
            batch_size=100,
            concurrency=20,
            dry_run=False
        )

        # Mock successful insert (1 row affected)
        mock_execute_dml.return_value = 1

        # Act: Create job
        result = create_job(job_id="test-job-123", params=params)

        # Assert: Verify execute_dml was called exactly once
        assert mock_execute_dml.call_count == 1

        # Assert: Verify SQL query structure
        call_args = mock_execute_dml.call_args
        query = call_args[0][0]  # First positional arg is the query

        # Verify INSERT INTO statement
        assert "INSERT INTO" in query
        assert "serper_jobs" in query

        # Verify column list
        assert "job_id" in query
        assert "keyword" in query
        assert "state" in query
        assert "pages" in query
        assert "dry_run" in query
        assert "batch_size" in query
        assert "concurrency" in query
        assert "status" in query
        assert "created_at" in query
        assert "started_at" in query
        assert "totals" in query

        # Verify status is hardcoded to 'running' (not parameterized)
        assert "'running'" in query or "\"running\"" in query

        # Verify CURRENT_TIMESTAMP() is used for created_at and started_at
        assert "CURRENT_TIMESTAMP()" in query

        # Verify totals STRUCT is initialized to zeros
        assert "STRUCT(" in query
        assert "0 AS zips" in query
        assert "0 AS queries" in query
        assert "0 AS successes" in query
        assert "0 AS failures" in query
        assert "0 AS places" in query
        assert "0 AS credits" in query

        # Assert: Verify return value structure
        assert isinstance(result, dict)
        assert result["job_id"] == "test-job-123"
        assert result["status"] == "running"
        assert "created_at" in result
        # created_at should be an ISO 8601 timestamp string
        assert "T" in result["created_at"]  # ISO format includes 'T'
        assert isinstance(result["created_at"], str)

    def test_create_job_parameter_validation(self, mock_execute_dml):
        """Test that all JobParams are correctly passed as ScalarQueryParameters.

        This verifies SQL injection protection and correct type handling.
        Each parameter must be a bigquery.ScalarQueryParameter with the
        correct name, type, and value.
        """
        # Arrange: Create job with specific parameter values
        params = JobParams(
            keyword="test-keyword",
            state="CA",
            pages=5,
            batch_size=50,
            concurrency=10,
            dry_run=True
        )

        # Mock successful insert
        mock_execute_dml.return_value = 1

        # Act: Create job
        create_job(job_id="param-test-job", params=params)

        # Assert: Extract parameters from call
        call_args = mock_execute_dml.call_args
        parameters = call_args[0][1]  # Second positional arg is parameters list

        # Verify all parameters are ScalarQueryParameter instances
        assert all(isinstance(p, bigquery.ScalarQueryParameter) for p in parameters)

        # Verify correct number of parameters (7 total)
        assert len(parameters) == 7

        # Build parameter dict for easier assertions
        param_dict = {p.name: (p.type_, p.value) for p in parameters}

        # Assert: job_id parameter
        assert "job_id" in param_dict
        assert param_dict["job_id"][0] == "STRING"
        assert param_dict["job_id"][1] == "param-test-job"

        # Assert: keyword parameter
        assert "keyword" in param_dict
        assert param_dict["keyword"][0] == "STRING"
        assert param_dict["keyword"][1] == "test-keyword"

        # Assert: state parameter
        assert "state" in param_dict
        assert param_dict["state"][0] == "STRING"
        assert param_dict["state"][1] == "CA"

        # Assert: pages parameter
        assert "pages" in param_dict
        assert param_dict["pages"][0] == "INT64"
        assert param_dict["pages"][1] == 5

        # Assert: dry_run parameter
        assert "dry_run" in param_dict
        assert param_dict["dry_run"][0] == "BOOL"
        assert param_dict["dry_run"][1] is True

        # Assert: batch_size parameter
        assert "batch_size" in param_dict
        assert param_dict["batch_size"][0] == "INT64"
        assert param_dict["batch_size"][1] == 50

        # Assert: concurrency parameter
        assert "concurrency" in param_dict
        assert param_dict["concurrency"][0] == "INT64"
        assert param_dict["concurrency"][1] == 10

    def test_create_job_dry_run_false(self, mock_execute_dml):
        """Test job creation with dry_run=False (production mode).

        Verify that the dry_run parameter is correctly set to False
        when creating a production job.
        """
        # Arrange: Create production job (dry_run=False)
        params = JobParams(
            keyword="restaurants",
            state="NY",
            pages=3,
            batch_size=150,
            concurrency=30,
            dry_run=False  # Production mode
        )

        # Mock successful insert
        mock_execute_dml.return_value = 1

        # Act: Create job
        create_job(job_id="prod-job-456", params=params)

        # Assert: Verify dry_run parameter is False
        parameters = mock_execute_dml.call_args[0][1]
        param_dict = {p.name: p.value for p in parameters}

        assert param_dict["dry_run"] is False

    def test_create_job_dry_run_true(self, mock_execute_dml):
        """Test job creation with dry_run=True (test mode).

        Verify that the dry_run parameter is correctly set to True
        when creating a test/simulation job.
        """
        # Arrange: Create dry-run job
        params = JobParams(
            keyword="cafes",
            state="TX",
            pages=2,
            batch_size=50,
            concurrency=10,
            dry_run=True  # Test mode
        )

        # Mock successful insert
        mock_execute_dml.return_value = 1

        # Act: Create job
        create_job(job_id="dryrun-job-789", params=params)

        # Assert: Verify dry_run parameter is True
        parameters = mock_execute_dml.call_args[0][1]
        param_dict = {p.name: p.value for p in parameters}

        assert param_dict["dry_run"] is True

    def test_create_job_state_uppercase_conversion(self, mock_execute_dml):
        """Test that state codes are converted to uppercase.

        The JobParams validator should uppercase state codes, but verify
        this happens correctly in the create_job flow.
        """
        # Arrange: Create job with lowercase state (will be uppercased by validator)
        params = JobParams(
            keyword="bars",
            state="az",  # Lowercase - should be converted to "AZ"
            pages=3,
            batch_size=100,
            concurrency=20,
            dry_run=False
        )

        # Mock successful insert
        mock_execute_dml.return_value = 1

        # Act: Create job
        create_job(job_id="state-test-job", params=params)

        # Assert: Verify state was uppercased
        parameters = mock_execute_dml.call_args[0][1]
        param_dict = {p.name: p.value for p in parameters}

        assert param_dict["state"] == "AZ"

    def test_create_job_default_parameters(self, mock_execute_dml):
        """Test job creation with default parameter values.

        JobParams has defaults for pages, batch_size, concurrency, and dry_run.
        Verify these defaults are correctly applied when not specified.
        """
        # Arrange: Create job with only required params (keyword and state)
        # All other params should use defaults from JobParams
        params = JobParams(
            keyword="hotels",
            state="FL"
            # pages=3 (default)
            # batch_size=100 (default)
            # concurrency=20 (default)
            # dry_run=False (default)
        )

        # Mock successful insert
        mock_execute_dml.return_value = 1

        # Act: Create job
        create_job(job_id="defaults-job-999", params=params)

        # Assert: Verify default values were used
        parameters = mock_execute_dml.call_args[0][1]
        param_dict = {p.name: p.value for p in parameters}

        assert param_dict["pages"] == 3  # Default from JobParams
        assert param_dict["batch_size"] == 100  # Default from JobParams
        assert param_dict["concurrency"] == 20  # Default from JobParams
        assert param_dict["dry_run"] is False  # Default from JobParams

    def test_create_job_sql_injection_protection(self, mock_execute_dml):
        """Test that parameterized queries prevent SQL injection.

        Verify that even if malicious strings are provided, they are safely
        parameterized and cannot inject SQL commands.
        """
        # Arrange: Create job with SQL injection attempt in keyword
        params = JobParams(
            keyword="bars'; DROP TABLE serper_jobs; --",
            state="CA",
            pages=3,
            batch_size=100,
            concurrency=20,
            dry_run=False
        )

        # Mock successful insert
        mock_execute_dml.return_value = 1

        # Act: Create job with malicious job_id
        create_job(
            job_id="test'; DELETE FROM serper_jobs WHERE '1'='1",
            params=params
        )

        # Assert: Verify parameters are safely parameterized
        parameters = mock_execute_dml.call_args[0][1]
        param_dict = {p.name: p.value for p in parameters}

        # The malicious strings should be safely stored as parameter values
        assert param_dict["job_id"] == "test'; DELETE FROM serper_jobs WHERE '1'='1"
        assert param_dict["keyword"] == "bars'; DROP TABLE serper_jobs; --"

        # Verify parameters are ScalarQueryParameter objects (safe)
        assert all(isinstance(p, bigquery.ScalarQueryParameter) for p in parameters)

    def test_create_job_return_value_structure(self, mock_execute_dml):
        """Test that the return value has the correct structure.

        The return dict must have exactly: job_id, status, and created_at.
        This contract is relied upon by the CLI and other callers.
        """
        # Arrange: Create standard job
        params = JobParams(
            keyword="gyms",
            state="WA",
            pages=3,
            batch_size=100,
            concurrency=20,
            dry_run=False
        )

        # Mock successful insert
        mock_execute_dml.return_value = 1

        # Act: Create job
        result = create_job(job_id="return-test-job", params=params)

        # Assert: Verify return value structure
        assert isinstance(result, dict)
        assert len(result) == 3  # Exactly 3 keys

        # Assert: Verify required keys
        assert "job_id" in result
        assert "status" in result
        assert "created_at" in result

        # Assert: Verify types
        assert isinstance(result["job_id"], str)
        assert isinstance(result["status"], str)
        assert isinstance(result["created_at"], str)

        # Assert: Verify values
        assert result["job_id"] == "return-test-job"
        assert result["status"] == "running"  # Always 'running' for new jobs

        # Assert: Verify timestamp format (ISO 8601)
        assert "T" in result["created_at"]
        assert "+" in result["created_at"] or "Z" in result["created_at"]

    def test_create_job_status_always_running(self, mock_execute_dml):
        """Test that new jobs always have status='running'.

        The status is hardcoded to 'running' in the SQL, not parameterized.
        This is by design - all new jobs start as 'running'.
        """
        # Arrange: Create job
        params = JobParams(
            keyword="parks",
            state="OR",
            pages=3,
            batch_size=100,
            concurrency=20,
            dry_run=False
        )

        # Mock successful insert
        mock_execute_dml.return_value = 1

        # Act: Create job
        result = create_job(job_id="status-test-job", params=params)

        # Assert: Verify status is 'running'
        assert result["status"] == "running"

        # Assert: Verify status is NOT parameterized (hardcoded in SQL)
        query = mock_execute_dml.call_args[0][0]
        parameters = mock_execute_dml.call_args[0][1]

        # Status should appear as string literal in SQL, not as parameter
        assert "'running'" in query or "\"running\"" in query

        # Status should NOT be in parameters list
        param_names = [p.name for p in parameters]
        assert "status" not in param_names
