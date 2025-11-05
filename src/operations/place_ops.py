"""Place storage operations for BigQuery.

This module handles place persistence:
- store_places: Store scraped places using idempotent MERGE
- _store_places_chunk: Internal helper for chunked operations
"""

import json
import time
from datetime import datetime, timezone
from typing import Any

from google.cloud import bigquery

from src.utils.bigquery_client import execute_dml
from src.utils.config import settings
from src.utils.timing import timing

# BigQuery MERGE operation limits
# Safe chunk size to avoid hitting parameter limits (10000 params max)
# With ~10 params per row, 500 rows = ~5000 params (50% safety margin)
MERGE_CHUNK_SIZE = 500


def _store_places_chunk(job_id: str, places: list[dict[str, Any]]) -> int:
    """Internal helper: store a single chunk of places (<=500 rows).

    Args:
        job_id: Job identifier
        places: List of place dicts (max 500 rows)

    Returns:
        Number of new places inserted

    Raises:
        google.cloud.exceptions.GoogleCloudError: If MERGE fails
    """
    if not places:
        return 0

    # Build rows for MERGE operation
    rows_to_insert = []
    for place in places:
        row = {
            "ingest_id": place.get("ingest_id", f"{job_id}-{place['place_uid']}-{int(time.time())}"),
            "job_id": job_id,
            "source": "serper_places",
            "source_version": "v1",
            "ingest_ts": place.get("ingest_ts", datetime.now(timezone.utc)),
            "keyword": place["keyword"],
            "state": place["state"],
            "zip": place["zip"],
            "page": place["page"],
            "place_uid": place["place_uid"],
            "payload": place["payload"],
            "payload_raw": json.dumps(place["payload"], ensure_ascii=False),
            "api_status": place.get("api_status"),
            "api_ms": place.get("api_ms"),
            "results_count": place.get("results_count"),
            "credits": place.get("credits"),
            "error": place.get("error"),
        }
        rows_to_insert.append(row)

    # Always use MERGE for guaranteed idempotency (works for all batch sizes)
    values_clauses = []
    for i, _ in enumerate(places):
        values_clauses.append(
            f"(@ingest_id_{i}, @job_id, @source, @source_version, @ingest_ts_{i}, "
            f"@keyword_{i}, @state_{i}, @zip_{i}, @page_{i}, @place_uid_{i}, "
            f"SAFE.PARSE_JSON(@payload_{i}), @payload_raw_{i}, @api_status_{i}, @api_ms_{i}, "
            f"@results_count_{i}, @credits_{i}, @error_{i})"
        )

    values_sql = ",\n        ".join(values_clauses)

    merge_query = f"""
    MERGE `{settings.bigquery_project_id}.{settings.bigquery_dataset}.serper_places` AS target
    USING (
        SELECT * FROM UNNEST([
            STRUCT<ingest_id STRING, job_id STRING, source STRING, source_version STRING,
                   ingest_ts TIMESTAMP, keyword STRING, state STRING, zip STRING, page INT64,
                   place_uid STRING, payload JSON, payload_raw STRING, api_status INT64, api_ms INT64,
                   results_count INT64, credits INT64, error STRING>
            {values_sql}
        ])
    ) AS source
    ON target.job_id = source.job_id AND target.place_uid = source.place_uid
    WHEN NOT MATCHED THEN
        INSERT (ingest_id, job_id, source, source_version, ingest_ts, keyword, state,
                zip, page, place_uid, payload, payload_raw, api_status, api_ms, results_count, credits, error)
        VALUES (source.ingest_id, source.job_id, source.source, source.source_version,
                source.ingest_ts, source.keyword, source.state, source.zip, source.page,
                source.place_uid, source.payload, source.payload_raw, source.api_status, source.api_ms,
                source.results_count, source.credits, source.error)
    """

    # Build parameters
    parameters = [
        bigquery.ScalarQueryParameter("job_id", "STRING", job_id),
        bigquery.ScalarQueryParameter("source", "STRING", "serper_places"),
        bigquery.ScalarQueryParameter("source_version", "STRING", "v1"),
    ]

    for i, place in enumerate(places):
        row = rows_to_insert[i]
        parameters.extend([
            bigquery.ScalarQueryParameter(f"ingest_id_{i}", "STRING", row["ingest_id"]),
            bigquery.ScalarQueryParameter(f"ingest_ts_{i}", "TIMESTAMP", row["ingest_ts"]),
            bigquery.ScalarQueryParameter(f"keyword_{i}", "STRING", row["keyword"]),
            bigquery.ScalarQueryParameter(f"state_{i}", "STRING", row["state"]),
            bigquery.ScalarQueryParameter(f"zip_{i}", "STRING", row["zip"]),
            bigquery.ScalarQueryParameter(f"page_{i}", "INT64", row["page"]),
            bigquery.ScalarQueryParameter(f"place_uid_{i}", "STRING", row["place_uid"]),
            bigquery.ScalarQueryParameter(f"payload_{i}", "STRING", row["payload_raw"]),
            bigquery.ScalarQueryParameter(f"payload_raw_{i}", "STRING", row["payload_raw"]),
            bigquery.ScalarQueryParameter(f"api_status_{i}", "INT64", row.get("api_status")),
            bigquery.ScalarQueryParameter(f"api_ms_{i}", "INT64", row.get("api_ms")),
            bigquery.ScalarQueryParameter(f"results_count_{i}", "INT64", row.get("results_count")),
            bigquery.ScalarQueryParameter(f"credits_{i}", "INT64", row.get("credits")),
            bigquery.ScalarQueryParameter(f"error_{i}", "STRING", row.get("error")),
        ])

    with timing(f"MERGE store {len(places)} places"):
        rows_affected = execute_dml(merge_query, parameters)
    return rows_affected


def store_places(job_id: str, places: list[dict[str, Any]]) -> int:
    """Store scraped places using idempotent MERGE operation.

    MERGE prevents duplicate places based on (job_id, place_uid). If the same
    place is scraped multiple times (e.g., due to retries), only the first
    instance is stored.

    For large place sets, this automatically chunks the operation into batches
    of 500 rows to avoid BigQuery parameter limits (10,000 params max).

    Args:
        job_id: Job identifier
        places: List of place dicts with keys matching PlaceRecord schema

    Returns:
        Number of new places inserted

    Raises:
        google.cloud.exceptions.GoogleCloudError: If MERGE fails
    """
    if not places:
        return 0

    # Chunk large place sets to avoid BigQuery parameter limits
    if len(places) > MERGE_CHUNK_SIZE:
        total_inserted = 0
        for i in range(0, len(places), MERGE_CHUNK_SIZE):
            chunk = places[i:i + MERGE_CHUNK_SIZE]
            inserted = _store_places_chunk(job_id, chunk)
            total_inserted += inserted
        return total_inserted
    else:
        return _store_places_chunk(job_id, places)
