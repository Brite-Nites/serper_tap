"""Unit tests for src/operations/place_ops.py

Tests place storage operations including idempotent MERGE logic and chunking.
These operations are critical for preventing duplicate places in the database.
"""

import json
from unittest.mock import MagicMock, patch

import pytest
from google.cloud import bigquery

from src.operations.place_ops import MERGE_CHUNK_SIZE, store_places


class TestStorePlaces:
    """Test idempotent place storage with MERGE operation.

    The store_places function uses BigQuery MERGE to ensure idempotency.
    Even if the same places are stored multiple times (e.g., due to retries),
    only the first instance is kept. This is critical for data integrity.

    The function also implements chunking to avoid BigQuery parameter limits.
    """

    def test_store_places_happy_path_small_batch(self, mock_execute_dml):
        """Test successful storage of a small batch of places.

        This is the most common case: store a few places from a single
        API response. Verify MERGE syntax and idempotency guarantees.
        """
        # Arrange: Create 3 sample places
        places = [
            {
                "keyword": "bars",
                "state": "AZ",
                "zip": "85001",
                "page": 1,
                "place_uid": "ChIJ123",
                "payload": {"title": "Bar 1", "address": "123 Main St"},
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
                "payload": {"title": "Bar 2", "address": "456 Oak Ave"},
                "api_status": 200,
                "results_count": 10,
                "credits": 1
            },
            {
                "keyword": "bars",
                "state": "AZ",
                "zip": "85002",
                "page": 1,
                "place_uid": "ChIJ789",
                "payload": {"title": "Bar 3", "address": "789 Pine Rd"},
                "api_status": 200,
                "results_count": 8,
                "credits": 1
            }
        ]

        # Mock: 3 new places inserted (none were duplicates)
        mock_execute_dml.return_value = 3

        # Act: Store places
        result = store_places(job_id="test-job-123", places=places)

        # Assert: execute_dml called exactly once (small batch)
        assert mock_execute_dml.call_count == 1

        # Assert: Extract query and parameters
        call_args = mock_execute_dml.call_args
        query = call_args[0][0]
        parameters = call_args[0][1]

        # Assert: MERGE statement structure
        assert "MERGE" in query
        assert "serper_places" in query
        assert "AS target" in query
        assert "AS source" in query

        # Assert: MERGE ON clause (idempotency key)
        assert "ON target.job_id = source.job_id" in query
        assert "AND target.place_uid = source.place_uid" in query

        # Assert: WHEN NOT MATCHED THEN INSERT clause
        assert "WHEN NOT MATCHED THEN" in query
        assert "INSERT" in query

        # Assert: INSERT includes all required columns
        assert "ingest_id" in query
        assert "job_id" in query
        assert "source" in query
        assert "keyword" in query
        assert "state" in query
        assert "zip" in query
        assert "page" in query
        assert "place_uid" in query
        assert "payload" in query
        assert "payload_raw" in query
        assert "api_status" in query
        assert "results_count" in query
        assert "credits" in query

        # Assert: Function returns rows affected
        assert result == 3

    def test_store_places_json_hardening_adr_0001(self, mock_execute_dml):
        """Test JSON hardening per ADR-0001.

        Verifies defense-in-depth strategy:
        1. payload: Uses SAFE.PARSE_JSON() to handle malformed JSON gracefully
        2. payload_raw: Stores raw JSON string for fallback/debugging
        3. Both use the same JSON string to ensure consistency
        """
        # Arrange: Create place with complex JSON payload
        places = [
            {
                "keyword": "restaurants",
                "state": "CA",
                "zip": "90210",
                "page": 1,
                "place_uid": "ChIJ_test",
                "payload": {
                    "title": "Test Restaurant",
                    "address": "123 Main St",
                    "rating": 4.5,
                    "reviews": 100,
                    "hours": {"monday": "9-5", "tuesday": "9-5"}
                },
                "api_status": 200,
                "results_count": 15,
                "credits": 1
            }
        ]

        # Mock: 1 place inserted
        mock_execute_dml.return_value = 1

        # Act: Store places
        store_places(job_id="json-test-job", places=places)

        # Assert: Extract query and parameters
        query = mock_execute_dml.call_args[0][0]
        parameters = mock_execute_dml.call_args[0][1]

        # Assert: payload uses SAFE.PARSE_JSON() for resilience
        assert "SAFE.PARSE_JSON(@payload_0)" in query

        # Assert: payload_raw uses direct parameter (raw string)
        assert "@payload_raw_0" in query

        # Assert: Find both parameters in parameters list
        param_dict = {p.name: p.value for p in parameters}
        assert "payload_0" in param_dict
        assert "payload_raw_0" in param_dict

        # Assert: Both parameters contain the same JSON string
        payload_param = param_dict["payload_0"]
        payload_raw_param = param_dict["payload_raw_0"]
        assert payload_param == payload_raw_param

        # Assert: Parameter value is valid JSON string
        assert isinstance(payload_param, str)
        parsed = json.loads(payload_param)
        assert parsed["title"] == "Test Restaurant"
        assert parsed["rating"] == 4.5

    def test_store_places_chunking_large_batch(self, mock_execute_dml):
        """Test automatic chunking for batches > MERGE_CHUNK_SIZE (500).

        When storing >500 places, the function should automatically split
        into multiple MERGE operations to avoid BigQuery parameter limits.
        """
        # Arrange: Create 505 places (requires 2 chunks: 500 + 5)
        places = []
        for i in range(505):
            places.append({
                "keyword": "stores",
                "state": "TX",
                "zip": f"7500{i % 100:02d}",
                "page": 1,
                "place_uid": f"ChIJ{i:05d}",
                "payload": {"title": f"Store {i}", "address": f"{i} Test St"},
                "api_status": 200,
                "results_count": 10,
                "credits": 1
            })

        # Mock: First chunk inserts 500, second chunk inserts 5
        mock_execute_dml.side_effect = [500, 5]

        # Act: Store large batch
        result = store_places(job_id="chunk-test-job", places=places)

        # Assert: execute_dml called exactly twice
        assert mock_execute_dml.call_count == 2

        # Assert: First call - 500 places
        first_call_params = mock_execute_dml.call_args_list[0][0][1]
        # Count parameters: 3 shared (job_id, source, source_version) + 14 per place
        # For 500 places: 3 + (500 * 14) = 7003 parameters
        first_call_param_count = len(first_call_params)
        assert first_call_param_count == 3 + (500 * 14)

        # Assert: Second call - 5 places
        second_call_params = mock_execute_dml.call_args_list[1][0][1]
        # For 5 places: 3 + (5 * 14) = 73 parameters
        second_call_param_count = len(second_call_params)
        assert second_call_param_count == 3 + (5 * 14)

        # Assert: Total return value is sum of chunks
        assert result == 505  # 500 + 5

    def test_store_places_empty_list(self, mock_execute_dml):
        """Test storing an empty list of places.

        When no places are provided, the function should:
        1. Return 0 immediately (early return optimization)
        2. NOT call execute_dml (no database call needed)
        """
        # Arrange: Empty places list
        places = []

        # Act: Store empty list
        result = store_places(job_id="empty-test-job", places=places)

        # Assert: execute_dml was NOT called
        assert mock_execute_dml.call_count == 0

        # Assert: Returns 0
        assert result == 0

    def test_store_places_idempotency_merge_logic(self, mock_execute_dml):
        """Test that MERGE provides idempotency guarantees.

        The ON clause (job_id, place_uid) ensures that re-storing the same
        place does NOT create duplicates. This is critical for retry safety.
        """
        # Arrange: Create place
        places = [
            {
                "keyword": "cafes",
                "state": "WA",
                "zip": "98101",
                "page": 1,
                "place_uid": "ChIJ_duplicate_test",
                "payload": {"title": "Test Cafe"},
                "api_status": 200,
                "results_count": 5,
                "credits": 1
            }
        ]

        # Mock: 0 rows inserted (place already exists - duplicate)
        mock_execute_dml.return_value = 0

        # Act: Store place (simulating a retry)
        result = store_places(job_id="idempotency-job", places=places)

        # Assert: Query uses correct ON clause for deduplication
        query = mock_execute_dml.call_args[0][0]
        assert "ON target.job_id = source.job_id" in query
        assert "AND target.place_uid = source.place_uid" in query

        # Assert: Returns 0 (no new places inserted, duplicate was skipped)
        assert result == 0

    def test_store_places_parameter_types(self, mock_execute_dml):
        """Test that all parameters have correct BigQuery types.

        Verify ScalarQueryParameter objects are used with correct types.
        """
        # Arrange: Create place with all fields populated
        places = [
            {
                "keyword": "hotels",
                "state": "NV",
                "zip": "89101",
                "page": 2,
                "place_uid": "ChIJ_types_test",
                "payload": {"title": "Test Hotel"},
                "api_status": 200,
                "api_ms": 150,
                "results_count": 20,
                "credits": 2,
                "error": None
            }
        ]

        # Mock: 1 place inserted
        mock_execute_dml.return_value = 1

        # Act: Store places
        store_places(job_id="types-test-job", places=places)

        # Assert: Extract parameters
        parameters = mock_execute_dml.call_args[0][1]

        # Assert: All parameters are ScalarQueryParameter instances
        assert all(isinstance(p, bigquery.ScalarQueryParameter) for p in parameters)

        # Build parameter dict for type checking
        param_dict = {p.name: (p.type_, p.value) for p in parameters}

        # Assert: Shared parameter types
        assert param_dict["job_id"][0] == "STRING"
        assert param_dict["source"][0] == "STRING"
        assert param_dict["source_version"][0] == "STRING"

        # Assert: Place-specific parameter types (index 0)
        assert param_dict["ingest_id_0"][0] == "STRING"
        assert param_dict["ingest_ts_0"][0] == "TIMESTAMP"
        assert param_dict["keyword_0"][0] == "STRING"
        assert param_dict["state_0"][0] == "STRING"
        assert param_dict["zip_0"][0] == "STRING"
        assert param_dict["page_0"][0] == "INT64"
        assert param_dict["place_uid_0"][0] == "STRING"
        assert param_dict["payload_0"][0] == "STRING"  # JSON as string
        assert param_dict["payload_raw_0"][0] == "STRING"  # Raw JSON string
        assert param_dict["api_status_0"][0] == "INT64"
        assert param_dict["api_ms_0"][0] == "INT64"
        assert param_dict["results_count_0"][0] == "INT64"
        assert param_dict["credits_0"][0] == "INT64"
        assert param_dict["error_0"][0] == "STRING"

    def test_store_places_exact_chunk_boundary(self, mock_execute_dml):
        """Test behavior at exact chunk boundary (500 places).

        When exactly 500 places are provided, it should use single chunk
        (no chunking needed, stays within limit).
        """
        # Arrange: Create exactly 500 places (at boundary)
        places = []
        for i in range(500):
            places.append({
                "keyword": "gyms",
                "state": "FL",
                "zip": "33101",
                "page": 1,
                "place_uid": f"ChIJ_boundary_{i}",
                "payload": {"title": f"Gym {i}"},
                "api_status": 200,
                "results_count": 10,
                "credits": 1
            })

        # Mock: All 500 inserted in single operation
        mock_execute_dml.return_value = 500

        # Act: Store 500 places
        result = store_places(job_id="boundary-job", places=places)

        # Assert: execute_dml called exactly once (no chunking at boundary)
        assert mock_execute_dml.call_count == 1

        # Assert: Returns 500
        assert result == 500

    def test_store_places_over_boundary_by_one(self, mock_execute_dml):
        """Test behavior just over chunk boundary (501 places).

        When 501 places are provided, it should chunk into 500 + 1.
        """
        # Arrange: Create 501 places (1 over boundary)
        places = []
        for i in range(501):
            places.append({
                "keyword": "pharmacies",
                "state": "IL",
                "zip": "60601",
                "page": 1,
                "place_uid": f"ChIJ_501_{i}",
                "payload": {"title": f"Pharmacy {i}"},
                "api_status": 200,
                "results_count": 10,
                "credits": 1
            })

        # Mock: First chunk 500, second chunk 1
        mock_execute_dml.side_effect = [500, 1]

        # Act: Store 501 places
        result = store_places(job_id="501-job", places=places)

        # Assert: execute_dml called exactly twice
        assert mock_execute_dml.call_count == 2

        # Assert: Returns total 501
        assert result == 501

    def test_store_places_optional_fields_none(self, mock_execute_dml):
        """Test handling of optional fields when they are None.

        Fields like api_ms, error are optional. Verify they're handled correctly.
        """
        # Arrange: Place with only required fields (optional fields missing)
        places = [
            {
                "keyword": "banks",
                "state": "NY",
                "zip": "10001",
                "page": 1,
                "place_uid": "ChIJ_minimal",
                "payload": {"title": "Test Bank"},
                # api_status, api_ms, results_count, credits, error all missing
            }
        ]

        # Mock: 1 place inserted
        mock_execute_dml.return_value = 1

        # Act: Store place with minimal fields
        store_places(job_id="minimal-job", places=places)

        # Assert: Parameters include None values for optional fields
        parameters = mock_execute_dml.call_args[0][1]
        param_dict = {p.name: p.value for p in parameters}

        # Optional fields should be present but with None values
        assert param_dict.get("api_status_0") is None
        assert param_dict.get("api_ms_0") is None
        assert param_dict.get("results_count_0") is None
        assert param_dict.get("credits_0") is None
        assert param_dict.get("error_0") is None

    def test_store_places_struct_declaration(self, mock_execute_dml):
        """Test that STRUCT declaration matches INSERT columns.

        The STRUCT in UNNEST must declare all columns with correct types.
        """
        # Arrange: Create place
        places = [
            {
                "keyword": "libraries",
                "state": "MA",
                "zip": "02101",
                "page": 1,
                "place_uid": "ChIJ_struct_test",
                "payload": {"title": "Test Library"},
                "api_status": 200,
                "results_count": 5,
                "credits": 1
            }
        ]

        # Mock: 1 place inserted
        mock_execute_dml.return_value = 1

        # Act: Store place
        store_places(job_id="struct-job", places=places)

        # Assert: Extract query
        query = mock_execute_dml.call_args[0][0]

        # Assert: STRUCT declaration includes all column types
        assert "STRUCT<" in query
        assert "ingest_id STRING" in query
        assert "job_id STRING" in query
        assert "source STRING" in query
        assert "source_version STRING" in query
        assert "ingest_ts TIMESTAMP" in query
        assert "keyword STRING" in query
        assert "state STRING" in query
        assert "zip STRING" in query
        assert "page INT64" in query
        assert "place_uid STRING" in query
        assert "payload JSON" in query  # Note: JSON type, not STRING
        assert "payload_raw STRING" in query
        assert "api_status INT64" in query
        assert "api_ms INT64" in query
        assert "results_count INT64" in query
        assert "credits INT64" in query
        assert "error STRING" in query
