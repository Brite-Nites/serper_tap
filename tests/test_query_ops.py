"""Unit tests for src/operations/query_ops.py

Tests the atomic batch dequeue operations and query status management.
These are critical functions for pipeline resilience and concurrency safety.
"""

from unittest.mock import MagicMock, call, patch

import pytest
from google.cloud import bigquery

from src.operations.query_ops import (
    MERGE_CHUNK_SIZE,
    batch_skip_remaining_pages,
    batch_update_query_statuses,
    dequeue_batch,
    enqueue_queries,
    reset_batch_to_queued,
    skip_remaining_pages,
    update_query_status,
)


class TestDequeueBatch:
    """Test atomic batch dequeue operation.

    The dequeue_batch function is the most critical function in the pipeline.
    It implements an atomic claim pattern to ensure concurrent workers don't
    process duplicate queries. Any bugs here cause duplicate processing or
    lost queries in production.
    """

    def test_dequeue_happy_path(
        self, mock_execute_dml, mock_execute_query, sample_bigquery_row
    ):
        """Test successful claim of a full batch of queries.

        This is the most common case: worker requests N queries, N are available,
        all N are claimed atomically and returned.
        """
        # Arrange: Mock DML returns 10 rows claimed
        mock_execute_dml.return_value = 10

        # Mock query returns 10 BigQuery row objects
        mock_rows = [
            sample_bigquery_row(
                zip=f"8500{i}",
                page=1,
                q=f"8500{i} bars",
                claim_id="claim-test-123"
            )
            for i in range(10)
        ]
        mock_execute_query.return_value = mock_rows

        # Act: Dequeue batch of 10 queries
        result = dequeue_batch(job_id="test-job-123", batch_size=10)

        # Assert: Verify return value structure
        assert len(result) == 10
        assert result[0]["zip"] == "85000"
        assert result[0]["page"] == 1
        assert result[0]["q"] == "85000 bars"
        assert result[0]["claim_id"] == "claim-test-123"

        # Assert: Verify execute_dml was called exactly once
        assert mock_execute_dml.call_count == 1

        # Assert: Verify execute_query was called exactly once
        assert mock_execute_query.call_count == 1

        # Assert: Verify SQL parameters passed to execute_dml
        dml_call_args = mock_execute_dml.call_args
        dml_query = dml_call_args[0][0]  # First positional arg is the query
        dml_params = dml_call_args[0][1]  # Second positional arg is parameters

        # Verify UPDATE query contains critical clauses
        assert "UPDATE" in dml_query
        assert "SET status = 'processing'" in dml_query
        assert "WHERE job_id = @job_id" in dml_query
        assert "AND status = 'queued'" in dml_query

        # Verify parameters are correct ScalarQueryParameter objects
        assert len(dml_params) == 3

        # Extract parameter values by name
        param_dict = {p.name: p.value for p in dml_params}
        assert param_dict["job_id"] == "test-job-123"
        assert param_dict["batch_size"] == 10
        assert "claim_id" in param_dict
        assert param_dict["claim_id"].startswith("claim-")

        # Assert: Verify SQL parameters passed to execute_query
        select_call_args = mock_execute_query.call_args
        select_query = select_call_args[0][0]
        select_params = select_call_args[0][1]

        # Verify SELECT query structure
        assert "SELECT zip, page, q, claim_id" in select_query
        assert "WHERE job_id = @job_id AND claim_id = @claim_id" in select_query
        assert "ORDER BY zip, page" in select_query

        # Verify same job_id and claim_id used in both queries
        select_param_dict = {p.name: p.value for p in select_params}
        assert select_param_dict["job_id"] == "test-job-123"
        assert select_param_dict["claim_id"] == param_dict["claim_id"]

    def test_dequeue_empty_queue(self, mock_execute_dml, mock_execute_query):
        """Test dequeue when no queries are available.

        This happens when:
        - All queries already processed (job complete)
        - Another worker claimed all remaining queries
        - Job was created but no queries enqueued yet

        Expected behavior: Return empty list, skip SELECT query.
        """
        # Arrange: Mock DML returns 0 (no rows updated)
        mock_execute_dml.return_value = 0

        # Act: Attempt to dequeue batch
        result = dequeue_batch(job_id="test-job-456", batch_size=10)

        # Assert: Returns empty list
        assert result == []

        # Assert: execute_dml was called (to attempt claim)
        assert mock_execute_dml.call_count == 1

        # Assert: execute_query was NOT called (early return optimization)
        assert mock_execute_query.call_count == 0

    def test_dequeue_partial_batch(
        self, mock_execute_dml, mock_execute_query, sample_bigquery_row
    ):
        """Test dequeue when fewer queries available than requested.

        This is common near the end of a job when only a few queries remain.
        For example: Request 50, but only 20 are queued.

        Expected behavior: Claim all available (20), return list of 20.
        """
        # Arrange: Request 50, but only 20 available
        mock_execute_dml.return_value = 20

        # Mock query returns exactly 20 rows
        mock_rows = [
            sample_bigquery_row(
                zip=f"8500{i:02d}",
                page=1,
                q=f"8500{i:02d} bars",
                claim_id="claim-partial-789"
            )
            for i in range(20)
        ]
        mock_execute_query.return_value = mock_rows

        # Act: Request 50 queries
        result = dequeue_batch(job_id="test-job-789", batch_size=50)

        # Assert: Returns exactly 20 (all available)
        assert len(result) == 20

        # Assert: All returned queries have correct structure
        for i, query in enumerate(result):
            assert query["zip"] == f"8500{i:02d}"
            assert query["page"] == 1
            assert query["claim_id"] == "claim-partial-789"

        # Assert: Verify batch_size=50 was passed to UPDATE
        dml_params = mock_execute_dml.call_args[0][1]
        param_dict = {p.name: p.value for p in dml_params}
        assert param_dict["batch_size"] == 50

    def test_dequeue_claim_uniqueness(
        self, mock_execute_dml, mock_execute_query, sample_bigquery_row
    ):
        """Test that multiple dequeue calls generate unique claim_ids.

        This is critical for concurrent workers - each must claim a different batch.
        The claim_id includes timestamp and random UUID to ensure uniqueness.
        """
        # Arrange: Mock successful claims
        mock_execute_dml.return_value = 5
        mock_execute_query.return_value = [
            sample_bigquery_row(zip="85001", page=1, q="85001 bars", claim_id="claim-1")
            for _ in range(5)
        ]

        # Act: Call dequeue_batch twice
        dequeue_batch(job_id="test-job", batch_size=5)
        dequeue_batch(job_id="test-job", batch_size=5)

        # Assert: Two separate UPDATE calls were made
        assert mock_execute_dml.call_count == 2

        # Extract claim_ids from both calls
        call_1_params = mock_execute_dml.call_args_list[0][0][1]
        call_2_params = mock_execute_dml.call_args_list[1][0][1]

        claim_id_1 = next(p.value for p in call_1_params if p.name == "claim_id")
        claim_id_2 = next(p.value for p in call_2_params if p.name == "claim_id")

        # Assert: claim_ids are different
        assert claim_id_1 != claim_id_2

        # Assert: Both follow expected format
        assert claim_id_1.startswith("claim-")
        assert claim_id_2.startswith("claim-")

    def test_dequeue_ordering(
        self, mock_execute_dml, mock_execute_query, sample_bigquery_row
    ):
        """Test that queries are ordered by zip, then page.

        The SELECT query should include ORDER BY zip, page to ensure
        predictable processing order (all pages of zip 85001, then 85002, etc.).
        """
        # Arrange: Mock 10 queries with mixed ordering
        mock_execute_dml.return_value = 10
        mock_rows = [
            sample_bigquery_row(zip="85001", page=1, q="85001 bars", claim_id="c1"),
            sample_bigquery_row(zip="85001", page=2, q="85001 bars", claim_id="c1"),
            sample_bigquery_row(zip="85001", page=3, q="85001 bars", claim_id="c1"),
            sample_bigquery_row(zip="85002", page=1, q="85002 bars", claim_id="c1"),
            sample_bigquery_row(zip="85002", page=2, q="85002 bars", claim_id="c1"),
        ]
        mock_execute_query.return_value = mock_rows

        # Act: Dequeue batch
        result = dequeue_batch(job_id="test-job", batch_size=10)

        # Assert: Results maintain order from query
        assert result[0]["zip"] == "85001" and result[0]["page"] == 1
        assert result[1]["zip"] == "85001" and result[1]["page"] == 2
        assert result[2]["zip"] == "85001" and result[2]["page"] == 3
        assert result[3]["zip"] == "85002" and result[3]["page"] == 1
        assert result[4]["zip"] == "85002" and result[4]["page"] == 2

        # Assert: SELECT query includes ORDER BY clause
        select_query = mock_execute_query.call_args[0][0]
        assert "ORDER BY zip, page" in select_query

    def test_dequeue_filters_by_status_queued(self, mock_execute_dml, mock_execute_query):
        """Test that UPDATE only claims queries with status='queued'.

        This prevents claiming queries that are:
        - Already processing (status='processing')
        - Already completed (status='success', 'failed', 'skipped')
        """
        # Arrange: Mock successful claim
        mock_execute_dml.return_value = 5
        mock_execute_query.return_value = []

        # Act: Dequeue batch
        dequeue_batch(job_id="test-job", batch_size=10)

        # Assert: UPDATE query filters by status='queued'
        update_query = mock_execute_dml.call_args[0][0]
        assert "WHERE job_id = @job_id" in update_query
        assert "AND status = 'queued'" in update_query

    def test_dequeue_return_structure(
        self, mock_execute_dml, mock_execute_query, sample_bigquery_row
    ):
        """Test that returned dictionaries have the correct structure.

        Each returned query must have exactly 4 keys: zip, page, q, claim_id.
        This contract is relied upon by the batch processing flow.
        """
        # Arrange: Mock single query result
        mock_execute_dml.return_value = 1
        mock_execute_query.return_value = [
            sample_bigquery_row(
                zip="85001",
                page=2,
                q="85001 bars",
                claim_id="claim-test-999"
            )
        ]

        # Act: Dequeue
        result = dequeue_batch(job_id="test-job", batch_size=1)

        # Assert: Single query returned
        assert len(result) == 1
        query = result[0]

        # Assert: Correct keys present
        assert set(query.keys()) == {"zip", "page", "q", "claim_id"}

        # Assert: Correct types
        assert isinstance(query["zip"], str)
        assert isinstance(query["page"], int)
        assert isinstance(query["q"], str)
        assert isinstance(query["claim_id"], str)

        # Assert: Correct values
        assert query["zip"] == "85001"
        assert query["page"] == 2
        assert query["q"] == "85001 bars"
        assert query["claim_id"] == "claim-test-999"

    def test_dequeue_uses_scalar_query_parameters(
        self, mock_execute_dml, mock_execute_query
    ):
        """Test that SQL parameters use BigQuery ScalarQueryParameter objects.

        This is the correct and safe way to pass parameters to BigQuery.
        It prevents SQL injection and ensures correct type handling.
        """
        # Arrange: Mock successful claim
        mock_execute_dml.return_value = 0  # Early return, we just need to check params

        # Act: Dequeue
        dequeue_batch(job_id="sql-injection-test'; DROP TABLE serper_queries;--", batch_size=100)

        # Assert: execute_dml was called with ScalarQueryParameter objects
        params = mock_execute_dml.call_args[0][1]

        # All parameters should be ScalarQueryParameter instances
        assert all(isinstance(p, bigquery.ScalarQueryParameter) for p in params)

        # Verify parameter types
        param_dict = {p.name: (p.type_, p.value) for p in params}
        assert param_dict["job_id"][0] == "STRING"
        assert param_dict["batch_size"][0] == "INT64"
        assert param_dict["claim_id"][0] == "STRING"

        # Verify dangerous string is safely parameterized
        assert param_dict["job_id"][1] == "sql-injection-test'; DROP TABLE serper_queries;--"


class TestBatchUpdateQueryStatuses:
    """Test batched query status update operation.

    The batch_update_query_statuses function is a high-performance batched version
    of update_query_status. It uses MERGE + UNNEST to update multiple queries in
    a single database call, reducing 20+ sequential 2.5s calls to a single 2.5s call.

    This is critical for production performance - batching provides 10x+ speedup.
    """

    def test_batch_update_happy_path(self, mock_execute_dml):
        """Test successful batch update of multiple query statuses.

        This is the most common case: update 3-4 queries after parallel API calls.
        Verify MERGE syntax, UNNEST pattern, and parameter handling.
        """
        # Arrange: Create 4 status updates from successful API calls
        updates = [
            {
                "zip": "85001",
                "page": 1,
                "status": "success",
                "api_status": 200,
                "results_count": 10,
                "credits": 1,
                "error": None
            },
            {
                "zip": "85002",
                "page": 1,
                "status": "success",
                "api_status": 200,
                "results_count": 8,
                "credits": 1,
                "error": None
            },
            {
                "zip": "85003",
                "page": 1,
                "status": "success",
                "api_status": 200,
                "results_count": 5,
                "credits": 1,
                "error": None
            },
            {
                "zip": "85004",
                "page": 2,
                "status": "failed",
                "api_status": 500,
                "results_count": 0,
                "credits": 0,
                "error": "API timeout"
            }
        ]

        # Mock: 4 rows updated
        mock_execute_dml.return_value = 4

        # Act: Batch update all queries
        result = batch_update_query_statuses(job_id="test-job-123", updates=updates)

        # Assert: execute_dml called exactly once (batched operation)
        assert mock_execute_dml.call_count == 1

        # Assert: Extract query and parameters
        call_args = mock_execute_dml.call_args
        query = call_args[0][0]
        parameters = call_args[0][1]

        # Assert: MERGE statement structure
        assert "MERGE" in query
        assert "serper_queries" in query
        assert "AS target" in query
        assert "AS source" in query

        # Assert: UNNEST pattern for batching
        assert "UNNEST" in query
        assert "STRUCT<" in query

        # Assert: MERGE ON clause (matches on job_id, zip, page)
        assert "ON target.job_id = source.job_id" in query
        assert "AND target.zip = source.zip" in query
        assert "AND target.page = source.page" in query

        # Assert: WHEN MATCHED THEN UPDATE clause
        assert "WHEN MATCHED THEN" in query
        assert "UPDATE SET" in query

        # Assert: UPDATE sets all status fields
        assert "status = source.status" in query
        assert "api_status = source.api_status" in query
        assert "results_count = source.results_count" in query
        assert "credits = source.credits" in query
        assert "error = source.error" in query
        assert "ran_at = CURRENT_TIMESTAMP()" in query

        # Assert: Function returns rows affected
        assert result == 4

    def test_batch_update_parameter_validation(self, mock_execute_dml):
        """Test that all parameters use correct BigQuery ScalarQueryParameter types.

        This verifies SQL injection protection and correct type handling for
        the batched operation. Each update generates 7 parameters.
        """
        # Arrange: Create 2 updates with different field combinations
        updates = [
            {
                "zip": "85001",
                "page": 1,
                "status": "success",
                "api_status": 200,
                "results_count": 10,
                "credits": 1,
                "error": None
            },
            {
                "zip": "85002",
                "page": 2,
                "status": "failed",
                "api_status": 500,
                "results_count": 0,
                "credits": 0,
                "error": "Timeout"
            }
        ]

        # Mock: 2 rows updated
        mock_execute_dml.return_value = 2

        # Act: Batch update
        batch_update_query_statuses(job_id="param-test-job", updates=updates)

        # Assert: Extract parameters
        parameters = mock_execute_dml.call_args[0][1]

        # Assert: All parameters are ScalarQueryParameter instances
        assert all(isinstance(p, bigquery.ScalarQueryParameter) for p in parameters)

        # Assert: Verify parameter count
        # 1 shared (job_id) + 7 per update × 2 updates = 15 parameters
        assert len(parameters) == 1 + (7 * 2)

        # Build parameter dict for type checking
        param_dict = {p.name: (p.type_, p.value) for p in parameters}

        # Assert: Shared parameter type
        assert param_dict["job_id"][0] == "STRING"
        assert param_dict["job_id"][1] == "param-test-job"

        # Assert: First update parameter types (index 0)
        assert param_dict["zip_0"][0] == "STRING"
        assert param_dict["zip_0"][1] == "85001"
        assert param_dict["page_0"][0] == "INT64"
        assert param_dict["page_0"][1] == 1
        assert param_dict["status_0"][0] == "STRING"
        assert param_dict["status_0"][1] == "success"
        assert param_dict["api_status_0"][0] == "INT64"
        assert param_dict["api_status_0"][1] == 200
        assert param_dict["results_count_0"][0] == "INT64"
        assert param_dict["results_count_0"][1] == 10
        assert param_dict["credits_0"][0] == "INT64"
        assert param_dict["credits_0"][1] == 1
        assert param_dict["error_0"][0] == "STRING"
        assert param_dict["error_0"][1] is None

        # Assert: Second update parameter types (index 1)
        assert param_dict["zip_1"][0] == "STRING"
        assert param_dict["zip_1"][1] == "85002"
        assert param_dict["page_1"][0] == "INT64"
        assert param_dict["page_1"][1] == 2
        assert param_dict["status_1"][0] == "STRING"
        assert param_dict["status_1"][1] == "failed"
        assert param_dict["api_status_1"][0] == "INT64"
        assert param_dict["api_status_1"][1] == 500
        assert param_dict["results_count_1"][0] == "INT64"
        assert param_dict["results_count_1"][1] == 0
        assert param_dict["credits_1"][0] == "INT64"
        assert param_dict["credits_1"][1] == 0
        assert param_dict["error_1"][0] == "STRING"
        assert param_dict["error_1"][1] == "Timeout"

    def test_batch_update_empty_list_raises_error(self, mock_execute_dml):
        """Test that empty updates list raises ValueError.

        The function should raise ValueError immediately if called with an empty
        list, as this indicates a programming error (batch should not be called
        with no data).
        """
        # Arrange: Empty updates list
        updates = []

        # Act & Assert: Calling with empty list raises ValueError
        with pytest.raises(ValueError, match="updates list cannot be empty"):
            batch_update_query_statuses(job_id="empty-test-job", updates=updates)

        # Assert: execute_dml was NOT called (early validation)
        assert mock_execute_dml.call_count == 0

    def test_batch_update_optional_fields_none(self, mock_execute_dml):
        """Test handling of optional fields when they are None.

        Fields like api_status, results_count, credits, error are optional
        in the update dict. Verify they're handled correctly when None.
        """
        # Arrange: Updates with minimal fields (optional fields as None)
        updates = [
            {
                "zip": "85001",
                "page": 1,
                "status": "success",
                # All optional fields missing (will be None via .get())
            },
            {
                "zip": "85002",
                "page": 1,
                "status": "failed",
                "error": "Some error",
                # Other optional fields missing
            }
        ]

        # Mock: 2 rows updated
        mock_execute_dml.return_value = 2

        # Act: Batch update with minimal fields
        batch_update_query_statuses(job_id="minimal-job", updates=updates)

        # Assert: Parameters include None values for optional fields
        parameters = mock_execute_dml.call_args[0][1]
        param_dict = {p.name: p.value for p in parameters}

        # First update: all optional fields should be None
        assert param_dict.get("api_status_0") is None
        assert param_dict.get("results_count_0") is None
        assert param_dict.get("credits_0") is None
        assert param_dict.get("error_0") is None

        # Second update: only error is populated
        assert param_dict.get("api_status_1") is None
        assert param_dict.get("results_count_1") is None
        assert param_dict.get("credits_1") is None
        assert param_dict.get("error_1") == "Some error"

    def test_batch_update_struct_declaration(self, mock_execute_dml):
        """Test that STRUCT declaration matches UPDATE columns.

        The STRUCT in UNNEST must declare all columns with correct types.
        """
        # Arrange: Create single update
        updates = [
            {
                "zip": "85001",
                "page": 1,
                "status": "success",
                "api_status": 200,
                "results_count": 10,
                "credits": 1,
                "error": None
            }
        ]

        # Mock: 1 row updated
        mock_execute_dml.return_value = 1

        # Act: Batch update
        batch_update_query_statuses(job_id="struct-job", updates=updates)

        # Assert: Extract query
        query = mock_execute_dml.call_args[0][0]

        # Assert: STRUCT declaration includes all column types
        assert "STRUCT<" in query
        assert "job_id STRING" in query
        assert "zip STRING" in query
        assert "page INT64" in query
        assert "status STRING" in query
        assert "api_status INT64" in query
        assert "results_count INT64" in query
        assert "credits INT64" in query
        assert "error STRING" in query

    def test_batch_update_values_clause_count(self, mock_execute_dml):
        """Test that VALUES clause contains correct number of entries.

        For N updates, the UNNEST should contain exactly N STRUCT entries.
        """
        # Arrange: Create 3 updates
        updates = [
            {"zip": "85001", "page": 1, "status": "success", "api_status": 200,
             "results_count": 10, "credits": 1, "error": None},
            {"zip": "85002", "page": 1, "status": "success", "api_status": 200,
             "results_count": 8, "credits": 1, "error": None},
            {"zip": "85003", "page": 1, "status": "success", "api_status": 200,
             "results_count": 5, "credits": 1, "error": None},
        ]

        # Mock: 3 rows updated
        mock_execute_dml.return_value = 3

        # Act: Batch update
        batch_update_query_statuses(job_id="values-job", updates=updates)

        # Assert: Extract query
        query = mock_execute_dml.call_args[0][0]

        # Assert: Query contains parameter references for all 3 updates
        # Each update has 7 parameters: zip, page, status, api_status, results_count, credits, error
        for i in range(3):
            assert f"@zip_{i}" in query
            assert f"@page_{i}" in query
            assert f"@status_{i}" in query
            assert f"@api_status_{i}" in query
            assert f"@results_count_{i}" in query
            assert f"@credits_{i}" in query
            assert f"@error_{i}" in query

    def test_batch_update_sql_injection_protection(self, mock_execute_dml):
        """Test that parameterized queries prevent SQL injection.

        Verify that even if malicious strings are provided, they are safely
        parameterized and cannot inject SQL commands.
        """
        # Arrange: Create update with SQL injection attempts
        updates = [
            {
                "zip": "85001'; DROP TABLE serper_queries; --",
                "page": 1,
                "status": "success'; DELETE FROM serper_queries WHERE '1'='1",
                "api_status": 200,
                "results_count": 10,
                "credits": 1,
                "error": "'; TRUNCATE TABLE serper_jobs; --"
            }
        ]

        # Mock: 1 row updated
        mock_execute_dml.return_value = 1

        # Act: Batch update with malicious strings
        batch_update_query_statuses(
            job_id="test'; DELETE FROM serper_queries WHERE '1'='1",
            updates=updates
        )

        # Assert: Verify parameters are safely parameterized
        parameters = mock_execute_dml.call_args[0][1]
        param_dict = {p.name: p.value for p in parameters}

        # The malicious strings should be safely stored as parameter values
        assert param_dict["job_id"] == "test'; DELETE FROM serper_queries WHERE '1'='1"
        assert param_dict["zip_0"] == "85001'; DROP TABLE serper_queries; --"
        assert param_dict["status_0"] == "success'; DELETE FROM serper_queries WHERE '1'='1"
        assert param_dict["error_0"] == "'; TRUNCATE TABLE serper_jobs; --"

        # Verify all parameters are ScalarQueryParameter objects (safe)
        assert all(isinstance(p, bigquery.ScalarQueryParameter) for p in parameters)


class TestBatchSkipRemainingPages:
    """Test batched early exit optimization operation.

    The batch_skip_remaining_pages function is a high-performance batched version
    of skip_remaining_pages. It uses MERGE + UNNEST to skip pages 2-3 for multiple
    zip codes in a single database call.

    This is a critical cost optimization: when page 1 returns <10 results, we skip
    pages 2-3 to avoid wasting API credits on empty pages. Batching provides major
    performance improvement over sequential skip operations.
    """

    def test_batch_skip_happy_path(self, mock_execute_dml):
        """Test successful batch skip of pages 2-3 for multiple zip codes.

        This is the most common case: skip remaining pages for 3-4 zip codes
        that returned sparse results on page 1. Verify MERGE syntax, UNNEST
        pattern with 2 entries per zip (page 2 and page 3).
        """
        # Arrange: Create list of 4 zip codes to skip
        zips_to_skip = ["85001", "85002", "85003", "85004"]

        # Mock: 8 rows updated (2 pages × 4 zips)
        mock_execute_dml.return_value = 8

        # Act: Batch skip remaining pages
        result = batch_skip_remaining_pages(job_id="test-job-123", zips_to_skip=zips_to_skip)

        # Assert: execute_dml called exactly once (batched operation)
        assert mock_execute_dml.call_count == 1

        # Assert: Extract query and parameters
        call_args = mock_execute_dml.call_args
        query = call_args[0][0]
        parameters = call_args[0][1]

        # Assert: MERGE statement structure
        assert "MERGE" in query
        assert "serper_queries" in query
        assert "AS target" in query
        assert "AS source" in query

        # Assert: UNNEST pattern for batching
        assert "UNNEST" in query
        assert "STRUCT<" in query

        # Assert: STRUCT declaration includes all column types
        assert "job_id STRING" in query
        assert "zip STRING" in query
        assert "page INT64" in query

        # Assert: MERGE ON clause (matches on job_id, zip, page, and status='queued')
        assert "ON target.job_id = source.job_id" in query
        assert "AND target.zip = source.zip" in query
        assert "AND target.page = source.page" in query
        assert "AND target.status = 'queued'" in query

        # Assert: WHEN MATCHED THEN UPDATE clause
        assert "WHEN MATCHED THEN" in query
        assert "UPDATE SET" in query

        # Assert: UPDATE sets status to 'skipped' and error to 'early_exit_page1_lt10'
        assert "status = 'skipped'" in query
        assert "error = 'early_exit_page1_lt10'" in query
        assert "ran_at = CURRENT_TIMESTAMP()" in query

        # Assert: Query contains parameter references for all 4 zips, both pages
        # Each zip generates 2 UNNEST entries: (@job_id, @zip_i, 2), (@job_id, @zip_i, 3)
        for i in range(4):
            assert f"@zip_{i}" in query
            # Should appear twice per zip (once for page 2, once for page 3)
            assert query.count(f"@zip_{i}") == 2

        # Assert: Query contains literal page numbers (2 and 3)
        # Each zip should have entries for both page 2 and page 3
        assert ", 2)" in query  # Page 2 entries
        assert ", 3)" in query  # Page 3 entries

        # Assert: Function returns rows affected
        assert result == 8

    def test_batch_skip_parameter_validation(self, mock_execute_dml):
        """Test that all parameters use correct BigQuery ScalarQueryParameter types.

        This verifies SQL injection protection and correct type handling for
        the batched skip operation.
        """
        # Arrange: Create 2 zips to skip
        zips_to_skip = ["85001", "85002"]

        # Mock: 4 rows updated (2 pages × 2 zips)
        mock_execute_dml.return_value = 4

        # Act: Batch skip
        batch_skip_remaining_pages(job_id="param-test-job", zips_to_skip=zips_to_skip)

        # Assert: Extract parameters
        parameters = mock_execute_dml.call_args[0][1]

        # Assert: All parameters are ScalarQueryParameter instances
        assert all(isinstance(p, bigquery.ScalarQueryParameter) for p in parameters)

        # Assert: Verify parameter count
        # 1 shared (job_id) + 1 per zip × 2 zips = 3 parameters
        # NOTE: Each zip only needs ONE parameter (@zip_i), used twice in VALUES
        assert len(parameters) == 1 + 2

        # Build parameter dict for type checking
        param_dict = {p.name: (p.type_, p.value) for p in parameters}

        # Assert: Shared parameter type
        assert param_dict["job_id"][0] == "STRING"
        assert param_dict["job_id"][1] == "param-test-job"

        # Assert: Zip parameter types
        assert param_dict["zip_0"][0] == "STRING"
        assert param_dict["zip_0"][1] == "85001"
        assert param_dict["zip_1"][0] == "STRING"
        assert param_dict["zip_1"][1] == "85002"

    def test_batch_skip_empty_list_raises_error(self, mock_execute_dml):
        """Test that empty zips_to_skip list raises ValueError.

        The function should raise ValueError immediately if called with an empty
        list, as this indicates a programming error (batch should not be called
        with no data).
        """
        # Arrange: Empty zips list
        zips_to_skip = []

        # Act & Assert: Calling with empty list raises ValueError
        with pytest.raises(ValueError, match="zips_to_skip list cannot be empty"):
            batch_skip_remaining_pages(job_id="empty-test-job", zips_to_skip=zips_to_skip)

        # Assert: execute_dml was NOT called (early validation)
        assert mock_execute_dml.call_count == 0

    def test_batch_skip_single_zip(self, mock_execute_dml):
        """Test batch skip with a single zip code.

        Even with one zip, the function should use the batched MERGE approach
        for consistency. Should generate 2 UNNEST entries (page 2 and page 3).
        """
        # Arrange: Single zip to skip
        zips_to_skip = ["85001"]

        # Mock: 2 rows updated (page 2 and page 3)
        mock_execute_dml.return_value = 2

        # Act: Batch skip with single zip
        result = batch_skip_remaining_pages(job_id="single-zip-job", zips_to_skip=zips_to_skip)

        # Assert: execute_dml called once
        assert mock_execute_dml.call_count == 1

        # Assert: Extract query
        query = mock_execute_dml.call_args[0][0]

        # Assert: Query contains @zip_0 parameter (used twice, for page 2 and 3)
        assert f"@zip_0" in query
        assert query.count("@zip_0") == 2

        # Assert: Query contains both page 2 and page 3
        assert ", 2)" in query
        assert ", 3)" in query

        # Assert: Returns 2 rows updated
        assert result == 2

    def test_batch_skip_values_clause_count(self, mock_execute_dml):
        """Test that VALUES clause contains correct number of entries.

        For N zips, the UNNEST should contain exactly 2N STRUCT entries
        (2 pages per zip: page 2 and page 3).
        """
        # Arrange: Create 3 zips to skip
        zips_to_skip = ["85001", "85002", "85003"]

        # Mock: 6 rows updated (2 pages × 3 zips)
        mock_execute_dml.return_value = 6

        # Act: Batch skip
        batch_skip_remaining_pages(job_id="values-job", zips_to_skip=zips_to_skip)

        # Assert: Extract query
        query = mock_execute_dml.call_args[0][0]

        # Assert: Query contains parameter references for all 3 zips
        for i in range(3):
            assert f"@zip_{i}" in query
            # Each zip should appear exactly twice (page 2 and page 3)
            assert query.count(f"@zip_{i}") == 2

        # Assert: Query contains 6 total page entries (2 per zip × 3 zips)
        # Count occurrences of ", 2)" and ", 3)" patterns
        page_2_count = query.count(", 2)")
        page_3_count = query.count(", 3)")
        assert page_2_count == 3  # One page 2 entry per zip
        assert page_3_count == 3  # One page 3 entry per zip

    def test_batch_skip_sql_injection_protection(self, mock_execute_dml):
        """Test that parameterized queries prevent SQL injection.

        Verify that even if malicious strings are provided in zip codes,
        they are safely parameterized and cannot inject SQL commands.
        """
        # Arrange: Create zips with SQL injection attempts
        zips_to_skip = [
            "85001'; DROP TABLE serper_queries; --",
            "85002'; DELETE FROM serper_jobs WHERE '1'='1"
        ]

        # Mock: 4 rows updated
        mock_execute_dml.return_value = 4

        # Act: Batch skip with malicious zip codes
        batch_skip_remaining_pages(
            job_id="test'; TRUNCATE TABLE serper_queries;--",
            zips_to_skip=zips_to_skip
        )

        # Assert: Verify parameters are safely parameterized
        parameters = mock_execute_dml.call_args[0][1]
        param_dict = {p.name: p.value for p in parameters}

        # The malicious strings should be safely stored as parameter values
        assert param_dict["job_id"] == "test'; TRUNCATE TABLE serper_queries;--"
        assert param_dict["zip_0"] == "85001'; DROP TABLE serper_queries; --"
        assert param_dict["zip_1"] == "85002'; DELETE FROM serper_jobs WHERE '1'='1"

        # Verify all parameters are ScalarQueryParameter objects (safe)
        assert all(isinstance(p, bigquery.ScalarQueryParameter) for p in parameters)

    def test_batch_skip_only_targets_queued_status(self, mock_execute_dml):
        """Test that MERGE only updates queries with status='queued'.

        This prevents accidentally skipping queries that are already processing,
        succeeded, or failed. The ON clause must include status='queued'.
        """
        # Arrange: Create zips to skip
        zips_to_skip = ["85001", "85002"]

        # Mock: 4 rows updated
        mock_execute_dml.return_value = 4

        # Act: Batch skip
        batch_skip_remaining_pages(job_id="status-job", zips_to_skip=zips_to_skip)

        # Assert: Query includes status='queued' in ON clause
        query = mock_execute_dml.call_args[0][0]
        assert "AND target.status = 'queued'" in query

        # Assert: This ensures only pending queries are skipped, not ones already processed


class TestEnqueueQueries:
    """Test idempotent query enqueue operation.

    The enqueue_queries function is the scalability workhorse - it performs the
    initial batch insert of all queries for a new job. It uses MERGE to ensure
    idempotency and automatically chunks large query sets (e.g., California's
    5,301 queries) into batches of 500 to avoid BigQuery parameter limits.

    This is the foundation for the entire pipeline - if queries aren't enqueued
    correctly, nothing else works.
    """

    def test_enqueue_happy_path_small_batch(self, mock_execute_dml):
        """Test successful enqueue of a small batch of queries.

        This is the common case for small states: enqueue 2-3 queries and verify
        they're inserted with status='queued' and correct MERGE idempotency logic.
        """
        # Arrange: Create 3 sample queries
        queries = [
            {"zip": "85001", "page": 1, "q": "85001 bars"},
            {"zip": "85001", "page": 2, "q": "85001 bars"},
            {"zip": "85002", "page": 1, "q": "85002 bars"},
        ]

        # Mock: 3 new queries inserted
        mock_execute_dml.return_value = 3

        # Act: Enqueue queries
        result = enqueue_queries(job_id="test-job-123", queries=queries)

        # Assert: execute_dml called exactly once (small batch, no chunking)
        assert mock_execute_dml.call_count == 1

        # Assert: Extract query and parameters
        call_args = mock_execute_dml.call_args
        query = call_args[0][0]
        parameters = call_args[0][1]

        # Assert: MERGE statement structure
        assert "MERGE" in query
        assert "serper_queries" in query
        assert "AS target" in query
        assert "AS source" in query

        # Assert: UNNEST pattern for batching
        assert "UNNEST" in query
        assert "STRUCT<" in query

        # Assert: STRUCT declaration includes all column types
        assert "job_id STRING" in query
        assert "zip STRING" in query
        assert "page INT64" in query
        assert "q STRING" in query
        assert "status STRING" in query
        assert "claim_id STRING" in query
        assert "claimed_at TIMESTAMP" in query
        assert "api_status INT64" in query
        assert "results_count INT64" in query
        assert "credits INT64" in query
        assert "error STRING" in query
        assert "ran_at TIMESTAMP" in query

        # Assert: MERGE ON clause (idempotency key: job_id, zip, page)
        assert "ON target.job_id = source.job_id" in query
        assert "AND target.zip = source.zip" in query
        assert "AND target.page = source.page" in query

        # Assert: WHEN NOT MATCHED THEN INSERT clause
        assert "WHEN NOT MATCHED THEN" in query
        assert "INSERT" in query

        # Assert: INSERT includes all required columns
        assert "INSERT (job_id, zip, page, q, status" in query
        assert "VALUES (source.job_id, source.zip, source.page, source.q, source.status" in query

        # Assert: Status is hardcoded to 'queued' in VALUES clause
        assert "'queued'" in query

        # Assert: Verify parameters
        param_dict = {p.name: p.value for p in parameters}

        # Shared parameter
        assert param_dict["job_id"] == "test-job-123"

        # Query-specific parameters (3 params per query: zip, page, q)
        assert param_dict["zip_0"] == "85001"
        assert param_dict["page_0"] == 1
        assert param_dict["q_0"] == "85001 bars"

        assert param_dict["zip_1"] == "85001"
        assert param_dict["page_1"] == 2
        assert param_dict["q_1"] == "85001 bars"

        assert param_dict["zip_2"] == "85002"
        assert param_dict["page_2"] == 1
        assert param_dict["q_2"] == "85002 bars"

        # Assert: Function returns rows affected
        assert result == 3

    def test_enqueue_chunking_large_batch(self, mock_execute_dml):
        """Test automatic chunking for batches > MERGE_CHUNK_SIZE (500).

        When enqueueing >500 queries (e.g., California: 5,301), the function
        should automatically chunk into batches of 500 to avoid BigQuery
        parameter limits (10,000 params max).
        """
        # Arrange: Create 505 queries (requires 2 chunks: 500 + 5)
        queries = []
        for i in range(505):
            queries.append({
                "zip": f"7500{i % 100:02d}",
                "page": (i % 3) + 1,
                "q": f"7500{i % 100:02d} bars"
            })

        # Mock: First chunk inserts 500, second chunk inserts 5
        mock_execute_dml.side_effect = [500, 5]

        # Act: Enqueue large batch
        result = enqueue_queries(job_id="chunk-test-job", queries=queries)

        # Assert: execute_dml called exactly twice
        assert mock_execute_dml.call_count == 2

        # Assert: First call - 500 queries
        first_call_params = mock_execute_dml.call_args_list[0][0][1]
        # Count parameters: 1 shared (job_id) + 3 per query × 500 queries
        # = 1 + (500 * 3) = 1501 parameters
        first_call_param_count = len(first_call_params)
        assert first_call_param_count == 1 + (500 * 3)

        # Assert: Second call - 5 queries
        second_call_params = mock_execute_dml.call_args_list[1][0][1]
        # = 1 + (5 * 3) = 16 parameters
        second_call_param_count = len(second_call_params)
        assert second_call_param_count == 1 + (5 * 3)

        # Assert: Total return value is sum of chunks
        assert result == 505  # 500 + 5

    def test_enqueue_empty_list(self, mock_execute_dml):
        """Test enqueueing an empty list of queries.

        When no queries are provided, the function should:
        1. Return 0 immediately (early return optimization)
        2. NOT call execute_dml (no database call needed)
        """
        # Arrange: Empty queries list
        queries = []

        # Act: Enqueue empty list
        result = enqueue_queries(job_id="empty-test-job", queries=queries)

        # Assert: execute_dml was NOT called
        assert mock_execute_dml.call_count == 0

        # Assert: Returns 0
        assert result == 0

    def test_enqueue_idempotency_merge_logic(self, mock_execute_dml):
        """Test that MERGE provides idempotency guarantees.

        The ON clause (job_id, zip, page) ensures that re-enqueueing the same
        queries does NOT create duplicates. This is critical for retry safety
        and job restart scenarios.
        """
        # Arrange: Create queries
        queries = [
            {"zip": "85001", "page": 1, "q": "85001 bars"},
            {"zip": "85002", "page": 1, "q": "85002 bars"},
            {"zip": "85003", "page": 1, "q": "85003 bars"},
        ]

        # Mock: 0 rows inserted (all queries already exist - duplicates)
        mock_execute_dml.return_value = 0

        # Act: Enqueue queries (simulating a retry)
        result = enqueue_queries(job_id="idempotency-job", queries=queries)

        # Assert: Query uses correct ON clause for deduplication
        query = mock_execute_dml.call_args[0][0]
        assert "ON target.job_id = source.job_id" in query
        assert "AND target.zip = source.zip" in query
        assert "AND target.page = source.page" in query

        # Assert: WHEN NOT MATCHED prevents duplicates
        assert "WHEN NOT MATCHED THEN" in query

        # Assert: Returns 0 (no new queries inserted, duplicates were skipped)
        assert result == 0

    def test_enqueue_parameter_types(self, mock_execute_dml):
        """Test that all parameters have correct BigQuery types.

        Verify ScalarQueryParameter objects are used with correct types.
        """
        # Arrange: Create queries with all fields populated
        queries = [
            {"zip": "85001", "page": 1, "q": "85001 bars"},
            {"zip": "85002", "page": 2, "q": "85002 bars"},
        ]

        # Mock: 2 queries inserted
        mock_execute_dml.return_value = 2

        # Act: Enqueue queries
        enqueue_queries(job_id="types-test-job", queries=queries)

        # Assert: Extract parameters
        parameters = mock_execute_dml.call_args[0][1]

        # Assert: All parameters are ScalarQueryParameter instances
        assert all(isinstance(p, bigquery.ScalarQueryParameter) for p in parameters)

        # Build parameter dict for type checking
        param_dict = {p.name: (p.type_, p.value) for p in parameters}

        # Assert: Shared parameter type
        assert param_dict["job_id"][0] == "STRING"
        assert param_dict["job_id"][1] == "types-test-job"

        # Assert: First query parameter types (index 0)
        assert param_dict["zip_0"][0] == "STRING"
        assert param_dict["zip_0"][1] == "85001"
        assert param_dict["page_0"][0] == "INT64"
        assert param_dict["page_0"][1] == 1
        assert param_dict["q_0"][0] == "STRING"
        assert param_dict["q_0"][1] == "85001 bars"

        # Assert: Second query parameter types (index 1)
        assert param_dict["zip_1"][0] == "STRING"
        assert param_dict["zip_1"][1] == "85002"
        assert param_dict["page_1"][0] == "INT64"
        assert param_dict["page_1"][1] == 2
        assert param_dict["q_1"][0] == "STRING"
        assert param_dict["q_1"][1] == "85002 bars"

    def test_enqueue_exact_chunk_boundary(self, mock_execute_dml):
        """Test behavior at exact chunk boundary (500 queries).

        When exactly 500 queries are provided, it should use single chunk
        (no chunking needed, stays within limit).
        """
        # Arrange: Create exactly 500 queries (at boundary)
        queries = []
        for i in range(500):
            queries.append({
                "zip": f"7500{i % 100:02d}",
                "page": (i % 3) + 1,
                "q": f"7500{i % 100:02d} bars"
            })

        # Mock: All 500 inserted in single operation
        mock_execute_dml.return_value = 500

        # Act: Enqueue 500 queries
        result = enqueue_queries(job_id="boundary-job", queries=queries)

        # Assert: execute_dml called exactly once (no chunking at boundary)
        assert mock_execute_dml.call_count == 1

        # Assert: Returns 500
        assert result == 500

    def test_enqueue_over_boundary_by_one(self, mock_execute_dml):
        """Test behavior just over chunk boundary (501 queries).

        When 501 queries are provided, it should chunk into 500 + 1.
        """
        # Arrange: Create 501 queries (1 over boundary)
        queries = []
        for i in range(501):
            queries.append({
                "zip": f"7500{i % 100:02d}",
                "page": (i % 3) + 1,
                "q": f"7500{i % 100:02d} bars"
            })

        # Mock: First chunk 500, second chunk 1
        mock_execute_dml.side_effect = [500, 1]

        # Act: Enqueue 501 queries
        result = enqueue_queries(job_id="501-job", queries=queries)

        # Assert: execute_dml called exactly twice
        assert mock_execute_dml.call_count == 2

        # Assert: Returns total 501
        assert result == 501

    def test_enqueue_values_clause_structure(self, mock_execute_dml):
        """Test that VALUES clause contains correct structure.

        Each query should generate a VALUES tuple with all 12 columns,
        with status hardcoded to 'queued' and nullable fields as NULL.
        """
        # Arrange: Create single query
        queries = [
            {"zip": "85001", "page": 1, "q": "85001 bars"}
        ]

        # Mock: 1 query inserted
        mock_execute_dml.return_value = 1

        # Act: Enqueue query
        enqueue_queries(job_id="values-job", queries=queries)

        # Assert: Extract query
        query = mock_execute_dml.call_args[0][0]

        # Assert: VALUES clause structure includes all 12 fields
        # (@job_id, @zip_0, @page_0, @q_0, 'queued', NULL, NULL, NULL, NULL, NULL, NULL, NULL)
        assert "@job_id" in query
        assert "@zip_0" in query
        assert "@page_0" in query
        assert "@q_0" in query
        assert "'queued'" in query  # Status hardcoded
        assert "NULL" in query  # Nullable fields (claim_id, claimed_at, etc.)

    def test_enqueue_sql_injection_protection(self, mock_execute_dml):
        """Test that parameterized queries prevent SQL injection.

        Verify that even if malicious strings are provided, they are safely
        parameterized and cannot inject SQL commands.
        """
        # Arrange: Create queries with SQL injection attempts
        queries = [
            {
                "zip": "85001'; DROP TABLE serper_queries; --",
                "page": 1,
                "q": "bars'; DELETE FROM serper_jobs WHERE '1'='1"
            }
        ]

        # Mock: 1 query inserted
        mock_execute_dml.return_value = 1

        # Act: Enqueue queries with malicious strings
        enqueue_queries(
            job_id="test'; TRUNCATE TABLE serper_queries;--",
            queries=queries
        )

        # Assert: Verify parameters are safely parameterized
        parameters = mock_execute_dml.call_args[0][1]
        param_dict = {p.name: p.value for p in parameters}

        # The malicious strings should be safely stored as parameter values
        assert param_dict["job_id"] == "test'; TRUNCATE TABLE serper_queries;--"
        assert param_dict["zip_0"] == "85001'; DROP TABLE serper_queries; --"
        assert param_dict["q_0"] == "bars'; DELETE FROM serper_jobs WHERE '1'='1"

        # Verify all parameters are ScalarQueryParameter objects (safe)
        assert all(isinstance(p, bigquery.ScalarQueryParameter) for p in parameters)

    def test_enqueue_uses_merge_chunk_size_constant(self, mock_execute_dml):
        """Test that chunking logic uses MERGE_CHUNK_SIZE constant.

        This verifies that the constant is correctly imported and used
        for determining when to chunk queries.
        """
        # Arrange: Create exactly MERGE_CHUNK_SIZE + 1 queries
        # This should trigger chunking: MERGE_CHUNK_SIZE queries + 1 query
        queries = []
        for i in range(MERGE_CHUNK_SIZE + 1):
            queries.append({
                "zip": f"7500{i % 100:02d}",
                "page": (i % 3) + 1,
                "q": f"7500{i % 100:02d} bars"
            })

        # Mock: First chunk inserts MERGE_CHUNK_SIZE, second chunk inserts 1
        mock_execute_dml.side_effect = [MERGE_CHUNK_SIZE, 1]

        # Act: Enqueue MERGE_CHUNK_SIZE + 1 queries
        result = enqueue_queries(job_id="constant-test-job", queries=queries)

        # Assert: execute_dml called exactly twice (chunking triggered)
        assert mock_execute_dml.call_count == 2

        # Assert: Returns total MERGE_CHUNK_SIZE + 1
        assert result == MERGE_CHUNK_SIZE + 1

        # Assert: Verify first chunk had exactly MERGE_CHUNK_SIZE queries
        first_call_params = mock_execute_dml.call_args_list[0][0][1]
        # 1 shared (job_id) + 3 per query × MERGE_CHUNK_SIZE
        expected_first_chunk_params = 1 + (MERGE_CHUNK_SIZE * 3)
        assert len(first_call_params) == expected_first_chunk_params


class TestUpdateQueryStatus:
    """Test single query status update operation.

    The update_query_status function is the simple, single-query version of
    batch_update_query_statuses. It updates status and metadata for one specific
    query after processing.

    This is used for individual error handling and backward compatibility.
    For batch processing, use batch_update_query_statuses instead.
    """

    def test_update_query_status_happy_path(self, mock_execute_dml):
        """Test successful status update with all fields populated.

        This is the common case: update a query after successful API call
        with complete metadata (status, api_status, results_count, credits).
        """
        # Arrange: Prepare successful query update
        job_id = "test-job-123"
        zip_code = "85001"
        page = 1
        status = "success"
        api_status = 200
        results_count = 10
        credits = 1
        error = None

        # Mock: 1 row updated
        mock_execute_dml.return_value = 1

        # Act: Update query status
        update_query_status(
            job_id=job_id,
            zip_code=zip_code,
            page=page,
            status=status,
            api_status=api_status,
            results_count=results_count,
            credits=credits,
            error=error
        )

        # Assert: execute_dml called exactly once
        assert mock_execute_dml.call_count == 1

        # Assert: Extract query and parameters
        call_args = mock_execute_dml.call_args
        query = call_args[0][0]
        parameters = call_args[0][1]

        # Assert: UPDATE statement structure
        assert "UPDATE" in query
        assert "serper_queries" in query

        # Assert: SET clause updates all fields
        assert "status = @status" in query
        assert "api_status = @api_status" in query
        assert "results_count = @results_count" in query
        assert "credits = @credits" in query
        assert "error = @error" in query
        assert "ran_at = CURRENT_TIMESTAMP()" in query

        # Assert: WHERE clause targets specific query
        assert "WHERE job_id = @job_id" in query
        assert "AND zip = @zip" in query
        assert "AND page = @page" in query

        # Assert: Verify parameters
        param_dict = {p.name: (p.type_, p.value) for p in parameters}

        # Required parameters
        assert param_dict["job_id"] == ("STRING", job_id)
        assert param_dict["zip"] == ("STRING", zip_code)
        assert param_dict["page"] == ("INT64", page)
        assert param_dict["status"] == ("STRING", status)

        # Optional parameters (all populated in this case)
        assert param_dict["api_status"] == ("INT64", api_status)
        assert param_dict["results_count"] == ("INT64", results_count)
        assert param_dict["credits"] == ("INT64", credits)
        assert param_dict["error"] == ("STRING", error)

    def test_update_query_status_error_path(self, mock_execute_dml):
        """Test status update for a failed query with error message.

        This is the error handling case: update a query after API failure
        with error message and status='failed'.
        """
        # Arrange: Prepare failed query update
        job_id = "error-job-456"
        zip_code = "85002"
        page = 2
        status = "failed"
        api_status = 500
        results_count = 0
        credits = 0
        error = "API timeout after 30 seconds"

        # Mock: 1 row updated
        mock_execute_dml.return_value = 1

        # Act: Update query status with error
        update_query_status(
            job_id=job_id,
            zip_code=zip_code,
            page=page,
            status=status,
            api_status=api_status,
            results_count=results_count,
            credits=credits,
            error=error
        )

        # Assert: execute_dml called once
        assert mock_execute_dml.call_count == 1

        # Assert: Verify error parameter is correctly passed
        parameters = mock_execute_dml.call_args[0][1]
        param_dict = {p.name: p.value for p in parameters}

        assert param_dict["status"] == "failed"
        assert param_dict["error"] == "API timeout after 30 seconds"
        assert param_dict["api_status"] == 500
        assert param_dict["results_count"] == 0
        assert param_dict["credits"] == 0

    def test_update_query_status_optional_fields_none(self, mock_execute_dml):
        """Test status update with optional fields as None.

        This tests the case where only status is updated and all optional
        metadata fields (api_status, results_count, credits, error) are None.
        """
        # Arrange: Prepare minimal update (only status, no metadata)
        job_id = "minimal-job-789"
        zip_code = "85003"
        page = 1
        status = "processing"

        # Mock: 1 row updated
        mock_execute_dml.return_value = 1

        # Act: Update with all optional fields as None
        update_query_status(
            job_id=job_id,
            zip_code=zip_code,
            page=page,
            status=status
            # api_status, results_count, credits, error all default to None
        )

        # Assert: execute_dml called once
        assert mock_execute_dml.call_count == 1

        # Assert: Verify all optional fields are None
        parameters = mock_execute_dml.call_args[0][1]
        param_dict = {p.name: p.value for p in parameters}

        assert param_dict["status"] == "processing"
        assert param_dict["api_status"] is None
        assert param_dict["results_count"] is None
        assert param_dict["credits"] is None
        assert param_dict["error"] is None

    def test_update_query_status_parameter_types(self, mock_execute_dml):
        """Test that all parameters use correct BigQuery types.

        Verify ScalarQueryParameter objects with correct type_ values.
        """
        # Arrange: Prepare update
        update_query_status(
            job_id="types-job",
            zip_code="85001",
            page=1,
            status="success",
            api_status=200,
            results_count=10,
            credits=1,
            error=None
        )

        # Assert: All parameters are ScalarQueryParameter instances
        parameters = mock_execute_dml.call_args[0][1]
        assert all(isinstance(p, bigquery.ScalarQueryParameter) for p in parameters)

        # Assert: Verify parameter types
        param_dict = {p.name: p.type_ for p in parameters}

        assert param_dict["job_id"] == "STRING"
        assert param_dict["zip"] == "STRING"
        assert param_dict["page"] == "INT64"
        assert param_dict["status"] == "STRING"
        assert param_dict["api_status"] == "INT64"
        assert param_dict["results_count"] == "INT64"
        assert param_dict["credits"] == "INT64"
        assert param_dict["error"] == "STRING"


class TestSkipRemainingPages:
    """Test single-zip early exit optimization operation.

    The skip_remaining_pages function implements the cost-saving early exit
    optimization for a single zip code. When page 1 returns <10 results, it
    skips pages 2-3 to avoid wasting API credits on empty pages.

    This is the simple, single-zip version. For batch processing, use
    batch_skip_remaining_pages instead.
    """

    def test_skip_remaining_pages_triggers_skip(self, mock_execute_dml):
        """Test that skip is triggered when page=1 and results_count<10.

        This is the exact condition for the early exit optimization:
        page 1 returned sparse results (< 10), so skip pages 2 and 3.
        """
        # Arrange: Conditions that trigger skip (page=1, results=9)
        job_id = "skip-job-123"
        zip_code = "85001"
        page = 1
        results_count = 9  # Less than 10, triggers skip

        # Mock: 2 rows updated (pages 2 and 3 marked as skipped)
        mock_execute_dml.return_value = 2

        # Act: Call skip function
        result = skip_remaining_pages(
            job_id=job_id,
            zip_code=zip_code,
            page=page,
            results_count=results_count
        )

        # Assert: execute_dml called exactly once
        assert mock_execute_dml.call_count == 1

        # Assert: Extract query and parameters
        call_args = mock_execute_dml.call_args
        query = call_args[0][0]
        parameters = call_args[0][1]

        # Assert: UPDATE statement structure
        assert "UPDATE" in query
        assert "serper_queries" in query

        # Assert: SET clause marks as skipped with error code
        assert "status = 'skipped'" in query
        assert "error = 'early_exit_page1_lt10'" in query
        assert "ran_at = CURRENT_TIMESTAMP()" in query

        # Assert: WHERE clause targets pages 2 and 3 only
        assert "WHERE job_id = @job_id" in query
        assert "AND zip = @zip" in query
        assert "AND page IN (2, 3)" in query
        assert "AND status = 'queued'" in query

        # Assert: Verify parameters
        param_dict = {p.name: (p.type_, p.value) for p in parameters}
        assert param_dict["job_id"] == ("STRING", job_id)
        assert param_dict["zip"] == ("STRING", zip_code)

        # Assert: Returns number of rows updated
        assert result == 2

    def test_skip_remaining_pages_no_op_page_not_1(self, mock_execute_dml):
        """Test that skip is NOT triggered when page != 1.

        The function should be a no-op (return 0, no DB call) when called
        for page 2 or page 3, even if results_count < 10.
        """
        # Arrange: page=2 (not page 1), results_count=5
        job_id = "noop-job-456"
        zip_code = "85002"
        page = 2  # Not page 1, should not trigger skip
        results_count = 5  # Less than 10, but page != 1

        # Act: Call skip function
        result = skip_remaining_pages(
            job_id=job_id,
            zip_code=zip_code,
            page=page,
            results_count=results_count
        )

        # Assert: execute_dml was NOT called (early return)
        assert mock_execute_dml.call_count == 0

        # Assert: Returns 0 (no-op)
        assert result == 0

    def test_skip_remaining_pages_no_op_sufficient_results(self, mock_execute_dml):
        """Test that skip is NOT triggered when results_count >= 10.

        The function should be a no-op when page 1 has sufficient results
        (10 or more), indicating more pages may be available.
        """
        # Arrange: page=1, but results_count=10 (sufficient results)
        job_id = "sufficient-job-789"
        zip_code = "85003"
        page = 1
        results_count = 10  # Exactly 10, should not skip

        # Act: Call skip function
        result = skip_remaining_pages(
            job_id=job_id,
            zip_code=zip_code,
            page=page,
            results_count=results_count
        )

        # Assert: execute_dml was NOT called (early return)
        assert mock_execute_dml.call_count == 0

        # Assert: Returns 0 (no-op)
        assert result == 0

    def test_skip_remaining_pages_no_op_more_than_10_results(self, mock_execute_dml):
        """Test that skip is NOT triggered when results_count > 10.

        When page 1 returns more than 10 results, pages 2-3 should be
        processed normally (not skipped).
        """
        # Arrange: page=1, results_count=15 (more than 10)
        job_id = "full-job-999"
        zip_code = "85004"
        page = 1
        results_count = 15  # More than 10, should not skip

        # Act: Call skip function
        result = skip_remaining_pages(
            job_id=job_id,
            zip_code=zip_code,
            page=page,
            results_count=results_count
        )

        # Assert: execute_dml was NOT called
        assert mock_execute_dml.call_count == 0

        # Assert: Returns 0
        assert result == 0

    def test_skip_remaining_pages_boundary_case_9_results(self, mock_execute_dml):
        """Test boundary case: exactly 9 results triggers skip.

        Verify that results_count=9 (the boundary just below 10) correctly
        triggers the skip optimization.
        """
        # Arrange: Boundary case (page=1, exactly 9 results)
        job_id = "boundary-job"
        zip_code = "85005"
        page = 1
        results_count = 9  # Exactly 9, should trigger skip

        # Mock: 2 rows updated
        mock_execute_dml.return_value = 2

        # Act: Call skip function
        result = skip_remaining_pages(
            job_id=job_id,
            zip_code=zip_code,
            page=page,
            results_count=results_count
        )

        # Assert: execute_dml was called (skip triggered)
        assert mock_execute_dml.call_count == 1

        # Assert: Returns 2
        assert result == 2

    def test_skip_remaining_pages_targets_queued_only(self, mock_execute_dml):
        """Test that skip only affects queries with status='queued'.

        The WHERE clause must include status='queued' to prevent accidentally
        skipping queries that are already processing or completed.
        """
        # Arrange: Trigger skip condition
        skip_remaining_pages(
            job_id="queued-check-job",
            zip_code="85006",
            page=1,
            results_count=5
        )

        # Assert: Query includes status='queued' filter
        query = mock_execute_dml.call_args[0][0]
        assert "AND status = 'queued'" in query


class TestResetBatchToQueued:
    """Test batch failure recovery operation.

    The reset_batch_to_queued function implements error recovery for failed
    batches. When a batch fails (network error, API error, etc.), this function
    resets all queries in that batch from 'processing' back to 'queued' so they
    can be retried by another worker.

    This is critical for pipeline resilience and ensuring no queries are lost.
    """

    def test_reset_batch_happy_path(self, mock_execute_dml):
        """Test successful reset of a failed batch.

        This is the error recovery case: a batch with claim_id failed,
        so reset all its queries from 'processing' to 'queued' for retry.
        """
        # Arrange: Failed batch with claim_id
        claim_id = "claim-1234567890-abc123def"

        # Mock: 10 rows reset (batch had 10 queries)
        mock_execute_dml.return_value = 10

        # Act: Reset batch to queued
        result = reset_batch_to_queued(claim_id=claim_id)

        # Assert: execute_dml called exactly once
        assert mock_execute_dml.call_count == 1

        # Assert: Extract query and parameters
        call_args = mock_execute_dml.call_args
        query = call_args[0][0]
        parameters = call_args[0][1]

        # Assert: UPDATE statement structure
        assert "UPDATE" in query
        assert "serper_queries" in query

        # Assert: SET clause resets to queued and clears claim metadata
        assert "status = 'queued'" in query
        assert "claim_id = NULL" in query
        assert "claimed_at = NULL" in query

        # Assert: WHERE clause targets specific claim_id and processing status
        assert "WHERE claim_id = @claim_id" in query
        assert "AND status = 'processing'" in query

        # Assert: Verify parameter
        assert len(parameters) == 1
        param = parameters[0]
        assert isinstance(param, bigquery.ScalarQueryParameter)
        assert param.name == "claim_id"
        assert param.type_ == "STRING"
        assert param.value == claim_id

        # Assert: Returns number of rows reset
        assert result == 10

    def test_reset_batch_no_matching_queries(self, mock_execute_dml):
        """Test reset when no queries match the claim_id.

        This can happen if:
        - Batch already completed successfully
        - Batch was already reset
        - claim_id doesn't exist
        """
        # Arrange: claim_id with no matching queries
        claim_id = "claim-nonexistent-999"

        # Mock: 0 rows reset (no matching queries)
        mock_execute_dml.return_value = 0

        # Act: Reset batch
        result = reset_batch_to_queued(claim_id=claim_id)

        # Assert: execute_dml called once
        assert mock_execute_dml.call_count == 1

        # Assert: Returns 0 (no queries reset)
        assert result == 0

    def test_reset_batch_parameter_type(self, mock_execute_dml):
        """Test that claim_id parameter uses correct BigQuery type.

        Verify ScalarQueryParameter with type STRING.
        """
        # Arrange: claim_id
        claim_id = "claim-types-test-xyz"

        # Mock: 5 rows reset
        mock_execute_dml.return_value = 5

        # Act: Reset batch
        reset_batch_to_queued(claim_id=claim_id)

        # Assert: Parameter has correct type
        parameters = mock_execute_dml.call_args[0][1]
        assert len(parameters) == 1

        param = parameters[0]
        assert param.name == "claim_id"
        assert param.type_ == "STRING"
        assert param.value == claim_id

    def test_reset_batch_only_targets_processing_status(self, mock_execute_dml):
        """Test that reset only affects queries with status='processing'.

        The WHERE clause must include status='processing' to prevent
        accidentally resetting queries that are:
        - Already queued
        - Successfully completed
        - Failed for other reasons
        """
        # Arrange: Reset batch
        reset_batch_to_queued(claim_id="status-check-claim")

        # Assert: Query includes status='processing' filter
        query = mock_execute_dml.call_args[0][0]
        assert "AND status = 'processing'" in query

    def test_reset_batch_sql_injection_protection(self, mock_execute_dml):
        """Test that malicious claim_id strings are safely parameterized.

        Verify that SQL injection attempts in claim_id are prevented
        through parameterization.
        """
        # Arrange: Malicious claim_id with SQL injection attempt
        claim_id = "claim-123'; DROP TABLE serper_queries; --"

        # Mock: 0 rows reset (malicious claim_id doesn't exist)
        mock_execute_dml.return_value = 0

        # Act: Reset batch with malicious claim_id
        reset_batch_to_queued(claim_id=claim_id)

        # Assert: claim_id is safely parameterized
        parameters = mock_execute_dml.call_args[0][1]
        param = parameters[0]

        assert isinstance(param, bigquery.ScalarQueryParameter)
        assert param.value == "claim-123'; DROP TABLE serper_queries; --"

        # Assert: Malicious string is NOT in the query itself (it's parameterized)
        query = mock_execute_dml.call_args[0][0]
        assert "DROP TABLE" not in query
