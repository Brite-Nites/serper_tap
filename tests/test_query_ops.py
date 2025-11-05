"""Unit tests for src/operations/query_ops.py

Tests the atomic batch dequeue operations and query status management.
These are critical functions for pipeline resilience and concurrency safety.
"""

from unittest.mock import MagicMock, call, patch

import pytest
from google.cloud import bigquery

from src.operations.query_ops import dequeue_batch


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
