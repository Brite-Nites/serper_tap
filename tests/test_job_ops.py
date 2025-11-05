"""Unit tests for src/operations/job_ops.py

Tests job lifecycle operations including job creation, status retrieval,
and statistics management. These are the entry points for the pipeline.
"""

from unittest.mock import MagicMock, patch

import pytest
from google.cloud import bigquery

from src.models.schemas import JobParams
from src.operations.job_ops import (
    create_job,
    get_job_stats,
    get_job_status,
    get_running_jobs,
    get_zips_for_state,
    mark_job_done,
    update_job_stats,
)


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


class TestGetZipsForState:
    """Test retrieving zip codes for a state from reference table.

    The get_zips_for_state function queries the geo_zip_all reference table
    to get all zip codes for a given state. This is used during job setup
    to build the full list of queries needed.
    """

    def test_get_zips_happy_path(self, mock_execute_query, sample_bigquery_row):
        """Test successful retrieval of zip codes for a state.

        This is the typical case: query the reference table and get back
        a list of zip codes for the specified state.
        """
        # Arrange: Mock 5 zip codes for Arizona
        mock_rows = [
            sample_bigquery_row(zip="85001"),
            sample_bigquery_row(zip="85002"),
            sample_bigquery_row(zip="85003"),
            sample_bigquery_row(zip="85004"),
            sample_bigquery_row(zip="85005"),
        ]
        mock_execute_query.return_value = mock_rows

        # Act: Get zips for state
        result = get_zips_for_state(state="AZ")

        # Assert: execute_query called exactly once
        assert mock_execute_query.call_count == 1

        # Assert: Extract query and parameters
        call_args = mock_execute_query.call_args
        query = call_args[0][0]
        parameters = call_args[0][1]

        # Assert: Query structure
        assert "SELECT DISTINCT zip" in query
        assert "reference.geo_zip_all" in query
        assert "WHERE state = @state" in query
        assert "ORDER BY zip" in query

        # Assert: State parameter
        assert len(parameters) == 1
        param = parameters[0]
        assert isinstance(param, bigquery.ScalarQueryParameter)
        assert param.name == "state"
        assert param.type_ == "STRING"
        assert param.value == "AZ"  # Uppercased

        # Assert: Return value is list of zip strings
        assert result == ["85001", "85002", "85003", "85004", "85005"]

    def test_get_zips_state_uppercase_conversion(self, mock_execute_query):
        """Test that state codes are converted to uppercase.

        The function should uppercase the state parameter before querying
        to ensure consistent lookups.
        """
        # Arrange: Mock empty result (focus is on parameter)
        mock_execute_query.return_value = []

        # Act: Call with lowercase state
        get_zips_for_state(state="ca")

        # Assert: State parameter was uppercased
        parameters = mock_execute_query.call_args[0][1]
        param_dict = {p.name: p.value for p in parameters}
        assert param_dict["state"] == "CA"

    def test_get_zips_empty_results(self, mock_execute_query):
        """Test behavior when state has no zip codes.

        If the state doesn't exist or has no zips in reference table,
        return empty list (not an error).
        """
        # Arrange: Mock empty results
        mock_execute_query.return_value = []

        # Act: Get zips for non-existent state
        result = get_zips_for_state(state="XX")

        # Assert: Returns empty list
        assert result == []


class TestUpdateJobStats:
    """Test recalculating and updating job statistics.

    The update_job_stats function uses subqueries to recalculate the totals
    STRUCT from actual query/place data. This ensures statistics stay in sync
    with the actual data even after retries or failures.
    """

    def test_update_job_stats_happy_path(
        self, mock_execute_dml, mock_execute_query, sample_bigquery_row
    ):
        """Test successful stats update with comprehensive subqueries.

        Verify that the function:
        1. Executes UPDATE with STRUCT subqueries
        2. Calls get_job_stats to return updated values
        3. Returns dict with all stat fields
        """
        # Arrange: Mock successful UPDATE (1 row affected)
        mock_execute_dml.return_value = 1

        # Mock get_job_stats return value (called after UPDATE)
        mock_stats_row = sample_bigquery_row(
            zips=100,
            queries=300,
            successes=280,
            failures=20,
            places=1234,
            credits=300
        )
        mock_execute_query.return_value = [mock_stats_row]

        # Act: Update job stats
        result = update_job_stats(job_id="stats-test-job")

        # Assert: execute_dml called exactly once (UPDATE)
        assert mock_execute_dml.call_count == 1

        # Assert: Extract UPDATE query
        update_query = mock_execute_dml.call_args[0][0]
        update_params = mock_execute_dml.call_args[0][1]

        # Assert: UPDATE statement structure
        assert "UPDATE" in update_query
        assert "serper_jobs" in update_query
        assert "SET totals = STRUCT(" in update_query

        # Assert: Subqueries for each stat field
        assert "AS zips" in update_query
        assert "AS queries" in update_query
        assert "AS successes" in update_query
        assert "AS failures" in update_query
        assert "AS places" in update_query
        assert "AS credits" in update_query

        # Assert: WHERE clause targets specific job
        assert "WHERE job_id = @job_id" in update_query

        # Assert: Parameter validation
        assert len(update_params) == 7  # 1 job_id + 6 for nested subqueries
        param_dict = {p.name: p.value for p in update_params}
        assert param_dict["job_id"] == "stats-test-job"

        # Assert: execute_query called once (get_job_stats)
        assert mock_execute_query.call_count == 1

        # Assert: Return value matches updated stats
        assert result == {
            "zips": 100,
            "queries": 300,
            "successes": 280,
            "failures": 20,
            "places": 1234,
            "credits": 300
        }


class TestGetJobStats:
    """Test retrieving current statistics for a job.

    The get_job_stats function reads the totals STRUCT from the serper_jobs
    table. This provides a quick snapshot of job progress without expensive
    aggregations.
    """

    def test_get_job_stats_happy_path(self, mock_execute_query, sample_bigquery_row):
        """Test successful retrieval of job statistics.

        Verify that the function queries the totals STRUCT and returns
        a dict with all stat fields.
        """
        # Arrange: Mock job stats row
        mock_row = sample_bigquery_row(
            zips=50,
            queries=150,
            successes=145,
            failures=5,
            places=678,
            credits=150
        )
        mock_execute_query.return_value = [mock_row]

        # Act: Get job stats
        result = get_job_stats(job_id="stats-get-job")

        # Assert: execute_query called exactly once
        assert mock_execute_query.call_count == 1

        # Assert: Extract query and parameters
        query = mock_execute_query.call_args[0][0]
        parameters = mock_execute_query.call_args[0][1]

        # Assert: Query structure
        assert "SELECT" in query
        assert "totals.zips AS zips" in query
        assert "totals.queries AS queries" in query
        assert "totals.successes AS successes" in query
        assert "totals.failures AS failures" in query
        assert "totals.places AS places" in query
        assert "totals.credits AS credits" in query
        assert "FROM" in query
        assert "serper_jobs" in query
        assert "WHERE job_id = @job_id" in query

        # Assert: Parameter validation
        assert len(parameters) == 1
        param_dict = {p.name: p.value for p in parameters}
        assert param_dict["job_id"] == "stats-get-job"

        # Assert: Return value structure
        assert result == {
            "zips": 50,
            "queries": 150,
            "successes": 145,
            "failures": 5,
            "places": 678,
            "credits": 150
        }

    def test_get_job_stats_job_not_found(self, mock_execute_query):
        """Test error handling when job doesn't exist.

        If the job_id doesn't exist, the function should raise ValueError.
        """
        # Arrange: Mock empty results (job not found)
        mock_execute_query.return_value = []

        # Act & Assert: Raises ValueError
        with pytest.raises(ValueError, match="Job stats-missing-job not found"):
            get_job_stats(job_id="stats-missing-job")


class TestGetJobStatus:
    """Test retrieving complete job status information.

    The get_job_status function returns full job metadata including the
    nested totals STRUCT. This is used for monitoring and debugging.
    """

    def test_get_job_status_happy_path(self, mock_execute_query, sample_bigquery_row):
        """Test successful retrieval of complete job status.

        Verify that the function returns all job fields including metadata
        and the nested totals STRUCT.
        """
        # Arrange: Mock complete job row
        mock_totals = sample_bigquery_row(
            zips=25,
            queries=75,
            successes=70,
            failures=5,
            places=321,
            credits=75
        )
        mock_row = sample_bigquery_row(
            job_id="status-test-job",
            keyword="restaurants",
            state="NY",
            pages=3,
            dry_run=False,
            batch_size=100,
            concurrency=20,
            status="running",
            created_at="2024-01-15T10:00:00+00:00",
            started_at="2024-01-15T10:00:00+00:00",
            finished_at=None,
            totals=mock_totals
        )
        mock_execute_query.return_value = [mock_row]

        # Act: Get job status
        result = get_job_status(job_id="status-test-job")

        # Assert: execute_query called exactly once
        assert mock_execute_query.call_count == 1

        # Assert: Extract query and parameters
        query = mock_execute_query.call_args[0][0]
        parameters = mock_execute_query.call_args[0][1]

        # Assert: Query structure (SELECT *)
        assert "SELECT *" in query or "SELECT" in query
        assert "FROM" in query
        assert "serper_jobs" in query
        assert "WHERE job_id = @job_id" in query

        # Assert: Parameter validation
        assert len(parameters) == 1
        param_dict = {p.name: p.value for p in parameters}
        assert param_dict["job_id"] == "status-test-job"

        # Assert: Return value contains all job fields
        assert result["job_id"] == "status-test-job"
        assert result["keyword"] == "restaurants"
        assert result["state"] == "NY"
        assert result["pages"] == 3
        assert result["dry_run"] is False
        assert result["batch_size"] == 100
        assert result["concurrency"] == 20
        assert result["status"] == "running"
        assert result["created_at"] == "2024-01-15T10:00:00+00:00"
        assert result["started_at"] == "2024-01-15T10:00:00+00:00"
        assert result["finished_at"] is None

        # Assert: Nested totals STRUCT
        assert "totals" in result
        assert result["totals"]["zips"] == 25
        assert result["totals"]["queries"] == 75
        assert result["totals"]["successes"] == 70
        assert result["totals"]["failures"] == 5
        assert result["totals"]["places"] == 321
        assert result["totals"]["credits"] == 75

    def test_get_job_status_job_not_found(self, mock_execute_query):
        """Test error handling when job doesn't exist.

        If the job_id doesn't exist, the function should raise ValueError.
        """
        # Arrange: Mock empty results (job not found)
        mock_execute_query.return_value = []

        # Act & Assert: Raises ValueError
        with pytest.raises(ValueError, match="Job status-missing-job not found"):
            get_job_status(job_id="status-missing-job")


class TestGetRunningJobs:
    """Test retrieving all jobs with status='running'.

    The get_running_jobs function queries for jobs that need processing.
    This is used by the batch processor to find work.
    """

    def test_get_running_jobs_happy_path(self, mock_execute_query, sample_bigquery_row):
        """Test successful retrieval of running jobs.

        Verify that the function queries for status='running' and returns
        job metadata needed for batch processing.
        """
        # Arrange: Mock 3 running jobs
        mock_rows = [
            sample_bigquery_row(
                job_id="job-1",
                keyword="bars",
                state="AZ",
                pages=3,
                batch_size=100,
                concurrency=20
            ),
            sample_bigquery_row(
                job_id="job-2",
                keyword="restaurants",
                state="CA",
                pages=3,
                batch_size=150,
                concurrency=30
            ),
            sample_bigquery_row(
                job_id="job-3",
                keyword="cafes",
                state="NY",
                pages=2,
                batch_size=50,
                concurrency=10
            ),
        ]
        mock_execute_query.return_value = mock_rows

        # Act: Get running jobs
        result = get_running_jobs()

        # Assert: execute_query called exactly once
        assert mock_execute_query.call_count == 1

        # Assert: Extract query and parameters
        query = mock_execute_query.call_args[0][0]
        parameters = mock_execute_query.call_args[0][1]

        # Assert: Query structure
        assert "SELECT" in query
        assert "FROM" in query
        assert "serper_jobs" in query
        assert "WHERE status = 'running'" in query or "WHERE status = \"running\"" in query
        assert "ORDER BY created_at" in query

        # Assert: No parameters (status is hardcoded)
        assert len(parameters) == 0

        # Assert: Return value is list of job dicts
        assert len(result) == 3
        assert result[0]["job_id"] == "job-1"
        assert result[0]["keyword"] == "bars"
        assert result[1]["job_id"] == "job-2"
        assert result[1]["state"] == "CA"
        assert result[2]["job_id"] == "job-3"
        assert result[2]["batch_size"] == 50

    def test_get_running_jobs_no_jobs(self, mock_execute_query):
        """Test behavior when no jobs are running.

        If no jobs have status='running', return empty list (not an error).
        """
        # Arrange: Mock empty results
        mock_execute_query.return_value = []

        # Act: Get running jobs
        result = get_running_jobs()

        # Assert: Returns empty list
        assert result == []


class TestMarkJobDone:
    """Test marking a job as completed.

    The mark_job_done function updates job status to 'done' and sets the
    finished_at timestamp. This signals that all processing is complete.
    """

    def test_mark_job_done_happy_path(self, mock_execute_dml):
        """Test successful job completion marking.

        Verify that the function updates status to 'done' and sets
        finished_at to current timestamp.
        """
        # Arrange: Mock successful UPDATE (1 row affected)
        mock_execute_dml.return_value = 1

        # Act: Mark job as done
        mark_job_done(job_id="done-test-job")

        # Assert: execute_dml called exactly once
        assert mock_execute_dml.call_count == 1

        # Assert: Extract query and parameters
        query = mock_execute_dml.call_args[0][0]
        parameters = mock_execute_dml.call_args[0][1]

        # Assert: UPDATE statement structure
        assert "UPDATE" in query
        assert "serper_jobs" in query
        assert "SET status = 'done'" in query or "SET status = \"done\"" in query
        assert "finished_at = CURRENT_TIMESTAMP()" in query
        assert "WHERE job_id = @job_id" in query

        # Assert: Parameter validation
        assert len(parameters) == 1
        param_dict = {p.name: p.value for p in parameters}
        assert param_dict["job_id"] == "done-test-job"

        # Assert: status is hardcoded (not parameterized)
        param_names = [p.name for p in parameters]
        assert "status" not in param_names

    def test_mark_job_done_job_not_found(self, mock_execute_dml):
        """Test behavior when job doesn't exist.

        If the job_id doesn't exist, execute_dml returns 0 rows affected.
        The function should complete without error (idempotent).
        """
        # Arrange: Mock no rows affected (job not found)
        mock_execute_dml.return_value = 0

        # Act: Mark non-existent job as done (should not raise)
        mark_job_done(job_id="missing-job")

        # Assert: execute_dml was called (operation attempted)
        assert mock_execute_dml.call_count == 1
