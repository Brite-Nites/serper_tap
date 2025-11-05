# ADR-0001: Dual JSON Columns with SAFE.PARSE_JSON

**Status:** Accepted
**Date:** October 2024
**Deciders:** Brite Nites Data Platform Team
**Context:** Migration 002 (add `payload_raw` column)

## Context and Problem Statement

When ingesting JSON payloads from the Serper.dev API into BigQuery, we face a critical reliability question: **What happens if the API returns malformed JSON or BigQuery's JSON parser fails?**

Traditional approaches:
1. **Parse-only**: Store only parsed JSON → ingestion fails on parse errors, data loss
2. **String-only**: Store raw JSON string → requires re-parsing on every query, poor performance

We need a solution that:
- **Never loses data**, even if JSON parsing fails
- Enables **forensic analysis** of parse failures
- Maintains **query performance** for the 99%+ case where JSON parses correctly
- Minimizes operational complexity

## Decision

We will use **dual JSON columns** with `SAFE.PARSE_JSON`:

```sql
CREATE TABLE serper_places (
  ...,
  payload JSON,          -- Parsed JSON (may be NULL if parse fails)
  payload_raw STRING,    -- Raw JSON string (always populated)
  ...
);
```

**Insertion pattern**:
```sql
MERGE serper_places AS target
USING (...) AS source
WHEN NOT MATCHED THEN
  INSERT (..., payload, payload_raw, ...)
  VALUES (..., SAFE.PARSE_JSON(@payload_raw), @payload_raw, ...);
```

**Query pattern**:
```sql
-- Normal case: Use parsed JSON for fast queries
SELECT payload.name, payload.rating
FROM serper_places
WHERE payload.rating > 4.0;

-- Forensics: Analyze parse failures
SELECT place_uid, payload_raw
FROM serper_places
WHERE payload IS NULL AND payload_raw IS NOT NULL;
```

## Consequences

### Positive

✅ **Zero data loss**: `payload_raw` always succeeds, even if JSON is malformed
✅ **Performance**: Parsed JSON (`payload`) enables fast queries without re-parsing
✅ **Observability**: `WHERE payload IS NULL AND payload_raw IS NOT NULL` identifies parse failures
✅ **Recovery**: Can re-parse `payload_raw` if BigQuery's JSON parser improves
✅ **Forensics**: Can debug API changes by inspecting raw payloads

### Negative

❌ **Storage cost**: 2x storage for JSON data (~50-100KB per place)
❌ **Maintenance**: Must keep both columns in sync during INSERTs/MERGEs

### Neutral

⚖️ **Complexity**: Modest increase (one extra column, one `SAFE.PARSE_JSON` call)

## Health Monitoring

**KPI**: JSON parse success rate ≥ **99.5%** (24-hour window)

**Query**:
```sql
SELECT
  COUNTIF(payload IS NULL AND payload_raw IS NOT NULL) AS unparsable,
  COUNTIF(payload IS NOT NULL) AS parsed,
  ROUND(100.0 * COUNTIF(payload IS NOT NULL) / COUNT(*), 2) AS success_pct
FROM `{project}.{dataset}.serper_places`
WHERE ingest_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR);
```

**Alert thresholds**:
- **Warning**: <99.5% for 1 hour → Investigate Serper API changes
- **Critical**: <99.0% for 4 hours → Potential API schema change, escalate

## Alternative Approaches Considered

### Alternative 1: Parse-only (rejected)

**Approach**: Store only `payload JSON`

**Pros**:
- Simpler schema
- Lower storage cost

**Cons**:
- ❌ **Data loss**: If JSON parsing fails, entire place is lost
- ❌ **No forensics**: Can't diagnose why parsing failed
- ❌ **High stakes**: Single parse error during ingestion is unrecoverable

**Why rejected**: Unacceptable data loss risk for production system.

### Alternative 2: String-only (rejected)

**Approach**: Store only `payload_raw STRING`

**Pros**:
- Simple schema
- No parse failures during ingestion

**Cons**:
- ❌ **Performance**: Must `PARSE_JSON(payload_raw)` on every query
- ❌ **Query complexity**: All queries must handle parse failures
- ❌ **Cost**: Re-parsing on every query is expensive

**Why rejected**: Poor query performance, shifts complexity to analysts.

### Alternative 3: External validation layer (rejected)

**Approach**: Validate JSON before insertion, reject bad payloads

**Pros**:
- Ensures only valid JSON enters BigQuery

**Cons**:
- ❌ **Data loss**: Rejected payloads are lost
- ❌ **Complexity**: Requires separate validation service
- ❌ **Latency**: Extra round-trip before ingestion

**Why rejected**: Adds complexity, still results in data loss.

## Implementation Notes

**Migration**: Added `payload_raw` column in Migration 002 (2024-10)
**Backward compatibility**: Existing queries on `payload` column continue to work
**Storage impact**: ~50-100KB per place × 2 = ~100-200KB per place
**Cost**: Negligible compared to Serper API costs ($0.01/1000 queries)

## Related

- [ARCHITECTURE.md - BigQuery Schema](../../ARCHITECTURE.md#bigquery-schema)
- [Migration 002: Add payload_raw column](../../sql/migrations/002_add_payload_raw.sql)
- Production health dashboard: See `serper-health-check` CLI command

## Revision History

- **2024-10**: Initial decision, implemented in Migration 002
- **2024-11**: Added health monitoring query and thresholds
