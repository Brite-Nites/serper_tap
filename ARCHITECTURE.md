# Serper Tap Architecture

> **Last Updated:** November 2025
> **Status:** Production
> **Maintainer:** Brite Nites Data Platform Team

## Table of Contents

- [System Overview](#system-overview)
- [Data Flow](#data-flow)
- [Component Architecture](#component-architecture)
- [BigQuery Schema](#bigquery-schema)
- [Idempotency & Atomicity](#idempotency--atomicity)
- [Orchestration & Deployment](#orchestration--deployment)
- [Performance & Scalability](#performance--scalability)
- [Key Architectural Decisions](#key-architectural-decisions)
- [Operational KPIs](#operational-kpis)

---

## System Overview

**Serper Tap** is a production-grade web scraping pipeline that fetches business listings from Serper.dev using a BigQuery-backed job queue architecture. The system is designed for:

- **Reliability**: All operations are idempotent with atomic dequeue guarantees
- **Scale**: Processes 300+ queries/minute with configurable concurrency
- **Cost Efficiency**: Early exit optimization saves ~66% of API calls for sparse areas
- **Observability**: Comprehensive stats tracking and health monitoring

### High-Level Architecture

```
┌──────────────┐
│              │
│   CLI / API  │  Create scraping jobs
│              │
└──────┬───────┘
       │
       ▼
┌─────────────────────────────────────────────────────────────┐
│                                                               │
│                    BIGQUERY (Queue + Storage)                │
│                                                               │
│  ┌──────────────┐  ┌──────────────┐  ┌────────────────┐    │
│  │ serper_jobs  │  │serper_queries│  │ serper_places  │    │
│  │              │  │              │  │                │    │
│  │ job metadata │  │ query queue  │  │ place results  │    │
│  └──────────────┘  └──────────────┘  └────────────────┘    │
│                                                               │
└───────────────────────────────┬───────────────────────────────┘
                                │
                    ┌───────────┴───────────┐
                    │                       │
                    ▼                       ▼
        ┌──────────────────┐    ┌──────────────────┐
        │                  │    │                  │
        │  Prefect Worker  │    │  Prefect Worker  │
        │  (GCE VM)        │    │  (GCE VM)        │
        │                  │    │                  │
        │  Process Batches │    │  Process Batches │
        │                  │    │                  │
        └─────────┬────────┘    └─────────┬────────┘
                  │                       │
                  └───────────┬───────────┘
                              │
                              ▼
                    ┌──────────────────┐
                    │                  │
                    │   Serper.dev API │
                    │                  │
                    └──────────────────┘
```

### Key Characteristics

- **Queue-Based**: Jobs are decomposed into queries and processed asynchronously
- **Stateless Workers**: Multiple workers can process the same job concurrently
- **Cloud-Native**: Leverages BigQuery for both queue and persistence
- **Orchestrated**: Prefect 2.x handles flow execution and monitoring

---

## Data Flow

### 1. Job Creation Flow

```
┌─────────┐
│  User   │
│ (CLI)   │
└────┬────┘
     │
     │ serper-create-job --keyword "bars" --state "AZ"
     │
     ▼
┌──────────────────────────────────────────────────────────────┐
│  create_scraping_job flow                                     │
│                                                               │
│  1. Create job record (status='running')                     │
│  2. Get zip codes for state (from reference.geo_zip_all)    │
│  3. Generate queries: zip × page combinations                │
│  4. Enqueue queries (MERGE into serper_queries)             │
│  5. Return job_id                                            │
└──────────────────────────────────────────────────────────────┘
     │
     │ job_id: "29ff7292-f104-49b9-a0fb-eb2c044b29de"
     │
     ▼
┌──────────────────────────────────────────────────────────────┐
│  BigQuery: serper_jobs                                        │
│  ┌────────────────────────────────────────────────────────┐ │
│  │ job_id  │ keyword │ state │ status    │ totals        │ │
│  ├─────────┼─────────┼───────┼───────────┼───────────────┤ │
│  │ 29ff... │ bars    │ AZ    │ running   │ {queries:0}   │ │
│  └────────────────────────────────────────────────────────┘ │
│                                                               │
│  BigQuery: serper_queries (queue)                            │
│  ┌────────────────────────────────────────────────────────┐ │
│  │ job_id  │ zip   │ page │ q         │ status  │ ...    │ │
│  ├─────────┼───────┼──────┼───────────┼─────────┼────────┤ │
│  │ 29ff... │ 85001 │ 1    │ 85001 bars│ queued  │ NULL   │ │
│  │ 29ff... │ 85001 │ 2    │ 85001 bars│ queued  │ NULL   │ │
│  │ 29ff... │ 85002 │ 1    │ 85002 bars│ queued  │ NULL   │ │
│  │ ...                                                      │ │
│  └────────────────────────────────────────────────────────┘ │
└──────────────────────────────────────────────────────────────┘
```

### 2. Batch Processing Flow

```
┌──────────────────────────────────────────────────────────────┐
│  process_job_batches flow (runs on Prefect worker)           │
│                                                               │
│  ┌─────────────────────────────────────────────────────┐    │
│  │ LOOP: while queued queries exist                    │    │
│  │                                                      │    │
│  │  1. Dequeue batch (atomic claim with claim_id)     │    │
│  │  2. Process queries in parallel (map)              │    │
│  │     ├─ Call Serper API                             │    │
│  │     ├─ Extract places                              │    │
│  │     └─ Check for early exit (page 1, <10 results) │    │
│  │  3. Batch update query statuses (MERGE)            │    │
│  │  4. Batch store places (MERGE)                     │    │
│  │  5. Batch skip remaining pages (MERGE)             │    │
│  │  6. Update job stats                               │    │
│  │  7. Check if job complete → mark done              │    │
│  │                                                      │    │
│  └─────────────────────────────────────────────────────┘    │
└──────────────────────────────────────────────────────────────┘
                             │
                             │ Updates
                             ▼
┌──────────────────────────────────────────────────────────────┐
│  BigQuery: serper_queries                                     │
│  ┌────────────────────────────────────────────────────────┐ │
│  │ job_id  │ zip   │ page │ status    │ claim_id │ ...   │ │
│  ├─────────┼───────┼──────┼───────────┼──────────┼───────┤ │
│  │ 29ff... │ 85001 │ 1    │ success   │ claim-..│ 200   │ │
│  │ 29ff... │ 85001 │ 2    │ skipped   │ NULL    │ NULL  │ │
│  │ 29ff... │ 85002 │ 1    │ processing│ claim-..│ NULL  │ │
│  └────────────────────────────────────────────────────────┘ │
│                                                               │
│  BigQuery: serper_places                                      │
│  ┌────────────────────────────────────────────────────────┐ │
│  │ job_id  │ place_uid │ payload         │ payload_raw   │ │
│  ├─────────┼───────────┼─────────────────┼───────────────┤ │
│  │ 29ff... │ ChIJN...  │ {"name": "..."}│ {"name": ...} │ │
│  └────────────────────────────────────────────────────────┘ │
└──────────────────────────────────────────────────────────────┘
```

### 3. Query States

```
┌─────────┐
│ queued  │  Initial state when query is enqueued
└────┬────┘
     │
     │ dequeue_batch() → atomic UPDATE with claim_id
     │
     ▼
┌────────────┐
│processing  │  Claimed by a worker, API call in progress
└────┬───┬───┘
     │   │
     │   │ API error, no retry exhausted
     │   └──────────────────┐
     │                      │
     │ API success          │
     ▼                      ▼
┌─────────┐           ┌─────────┐
│ success │           │ failed  │
└─────────┘           └─────────┘
     │
     │ page 1, results < 10
     ├────────────────────────┐
     │                        │
     │ pages 2-3 not          │
     │ yet processed          │
     │                        ▼
     │                  ┌──────────┐
     │                  │ skipped  │  Early exit optimization
     │                  └──────────┘
     ▼
(job continues)
```

---

## Component Architecture

### Module Hierarchy

```
src/
├── cli.py                    # Command-line interface (4 commands)
├── models/
│   └── schemas.py            # Pydantic models for type safety
├── utils/
│   ├── config.py             # Environment variable management
│   ├── bigquery_client.py    # BigQuery connection factory (ADC-aware)
│   └── timing.py             # Performance measurement utilities
├── operations/               # Plain Python functions (no Prefect)
│   ├── __init__.py          # Public API re-exports
│   ├── job_ops.py           # Job lifecycle operations
│   ├── query_ops.py         # Query queue management
│   ├── place_ops.py         # Place storage operations
│   └── bigquery_ops.py      # DEPRECATED: Compatibility shim
├── tasks/                   # Prefect @task decorated functions
│   ├── bigquery_tasks.py    # Task wrappers for operations
│   └── serper_tasks.py      # Serper API client tasks
└── flows/                   # Prefect @flow orchestration
    ├── create_job.py        # Flow 1: Job initialization
    └── process_batches.py   # Flow 2: Self-looping batch processor
```

### Layer Separation

| Layer | Responsibility | Example |
|-------|---------------|---------|
| **CLI** | User interface, argument parsing | `serper-create-job` |
| **Flows** | Orchestration, control flow, error handling | `create_scraping_job()` |
| **Tasks** | Prefect-aware wrappers, retries, logging | `@task(retries=3) dequeue_batch_task()` |
| **Operations** | Plain Python, SQL generation, BigQuery calls | `dequeue_batch(job_id, size)` |
| **Utils** | Shared infrastructure (config, clients) | `get_bigquery_client()` |
| **Models** | Type definitions, validation | `JobParams`, `PlaceRecord` |

### Operations Module Split (Post-Refactoring)

As of Phase 3A, the monolithic `bigquery_ops.py` (932 lines) was split into focused modules:

- **job_ops.py** (320 lines): `create_job`, `get_job_status`, `update_job_stats`, `mark_job_done`
- **query_ops.py** (472 lines): `enqueue_queries`, `dequeue_batch`, `update_query_status`, batched operations
- **place_ops.py** (173 lines): `store_places` with chunked MERGE logic
- **bigquery_ops.py** (62 lines): Deprecated compatibility shim with `DeprecationWarning`

All functions are re-exported from `operations/__init__.py` to maintain backward compatibility.

---

## BigQuery Schema

### Table: `serper_jobs`

Job metadata and rollup statistics.

```sql
CREATE TABLE IF NOT EXISTS `{project}.{dataset}.serper_jobs` (
  job_id STRING NOT NULL,
  keyword STRING NOT NULL,
  state STRING NOT NULL,
  pages INT64 NOT NULL,
  dry_run BOOL NOT NULL DEFAULT FALSE,
  batch_size INT64 NOT NULL DEFAULT 150,
  concurrency INT64 NOT NULL DEFAULT 100,
  status STRING NOT NULL,  -- 'running' | 'done'
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP(),
  started_at TIMESTAMP,
  finished_at TIMESTAMP,
  totals STRUCT<
    zips INT64,
    queries INT64,
    successes INT64,
    failures INT64,
    places INT64,
    credits INT64
  >
)
PARTITION BY DATE(created_at)
OPTIONS(description='Job-level metadata and aggregated statistics');
```

**Primary Key**: `job_id` (UUID)
**Idempotency**: Single INSERT on creation, UPDATEs via `update_job_stats()` are deterministic aggregations

### Table: `serper_queries`

Query-level queue and execution tracking.

```sql
CREATE TABLE IF NOT EXISTS `{project}.{dataset}.serper_queries` (
  job_id STRING NOT NULL,
  zip STRING NOT NULL,
  page INT64 NOT NULL,
  q STRING NOT NULL,
  status STRING NOT NULL,  -- 'queued' | 'processing' | 'success' | 'failed' | 'skipped'
  claim_id STRING,         -- Atomic dequeue identifier
  claimed_at TIMESTAMP,
  api_status INT64,
  results_count INT64,
  credits INT64,
  error STRING,
  ran_at TIMESTAMP
)
PARTITION BY DATE(claimed_at)
CLUSTER BY job_id, status
OPTIONS(description='Query-level queue and execution log');
```

**Composite Primary Key**: `(job_id, zip, page)`
**MERGE Key**: Same as primary key
**Idempotency**: `enqueue_queries()` uses `MERGE ... WHEN NOT MATCHED` - safe to re-run

**Atomic Dequeue Pattern**:
```sql
-- Step 1: Atomically claim batch
UPDATE serper_queries
SET status = 'processing',
    claim_id = @unique_claim_id,
    claimed_at = CURRENT_TIMESTAMP()
WHERE job_id = @job_id
  AND status = 'queued'
  AND (zip, page) IN (
    SELECT zip, page FROM (
      SELECT zip, page, ROW_NUMBER() OVER (ORDER BY zip, page) AS rn
      FROM serper_queries
      WHERE job_id = @job_id AND status = 'queued'
    )
    WHERE rn <= @batch_size
  );

-- Step 2: Fetch only queries with our claim_id
SELECT zip, page, q, claim_id
FROM serper_queries
WHERE job_id = @job_id AND claim_id = @unique_claim_id;
```

### Table: `serper_places`

Place-level results storage with dual JSON columns.

```sql
CREATE TABLE IF NOT EXISTS `{project}.{dataset}.serper_places` (
  ingest_id STRING NOT NULL,
  job_id STRING NOT NULL,
  source STRING NOT NULL,
  source_version STRING NOT NULL,
  ingest_ts TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP(),
  keyword STRING NOT NULL,
  state STRING NOT NULL,
  zip STRING NOT NULL,
  page INT64 NOT NULL,
  place_uid STRING NOT NULL,
  payload JSON,            -- Parsed JSON (may be NULL if parse fails)
  payload_raw STRING,      -- Raw JSON string (always populated)
  api_status INT64,
  api_ms INT64,
  results_count INT64,
  credits INT64,
  error STRING
)
PARTITION BY DATE(ingest_ts)
CLUSTER BY job_id, state
OPTIONS(description='Place-level results with dual JSON columns for resilience');
```

**Composite Primary Key**: `(job_id, place_uid)`
**MERGE Key**: Same as primary key
**Idempotency**: `store_places()` uses `MERGE ... WHEN NOT MATCHED` - safe to re-run

**Dual JSON Column Pattern** (see [ADR-0001](./docs/decisions/0001-safe-parse-json.md)):
```sql
INSERT INTO serper_places (
  ...,
  payload,        -- SAFE.PARSE_JSON(@payload_raw) in MERGE
  payload_raw     -- Raw string, always succeeds
  ...
)
```

**Health Check Query**:
```sql
SELECT
  COUNTIF(payload IS NULL AND payload_raw IS NOT NULL) AS unparsable,
  COUNTIF(payload IS NOT NULL) AS parsed,
  ROUND(100.0 * COUNTIF(payload IS NOT NULL) / COUNT(*), 2) AS success_pct
FROM `{project}.{dataset}.serper_places`
WHERE ingest_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR);
```

**Target**: ≥99.5% parse success rate

---

## Idempotency & Atomicity

### MERGE-Based Idempotency

All write operations use `MERGE` statements to ensure idempotency:

**Enqueue Queries**:
```sql
MERGE serper_queries AS target
USING (SELECT * FROM UNNEST([...]) AS source
ON target.job_id = source.job_id
   AND target.zip = source.zip
   AND target.page = source.page
WHEN NOT MATCHED THEN INSERT (...)
```

**Store Places**:
```sql
MERGE serper_places AS target
USING (SELECT * FROM UNNEST([...]) AS source
ON target.job_id = source.job_id
   AND target.place_uid = source.place_uid
WHEN NOT MATCHED THEN INSERT (...)
```

**Benefit**: Running the same operation multiple times (e.g., due to retries, duplicate messages) produces the same result as running it once.

### Atomic Dequeue Pattern

The `claim_id` pattern prevents race conditions when multiple workers process the same job:

1. **Generate unique `claim_id`**: `f"claim-{timestamp}-{uuid4().hex[:9]}"`
2. **Atomic UPDATE**: BigQuery serializes UPDATEs, ensuring only one worker claims each query
3. **Worker-specific SELECT**: Each worker only fetches queries with its `claim_id`

**Concurrency Safety**:
- Two workers call `dequeue_batch()` simultaneously
- Worker A claims queries 1-150 with `claim-abc`
- Worker B claims queries 151-300 with `claim-xyz`
- No overlap, no lost queries

### Batched Operations (Performance)

To reduce BigQuery round-trips, batched operations use `MERGE + UNNEST`:

**batch_update_query_statuses**:
```sql
MERGE serper_queries AS target
USING (
  SELECT * FROM UNNEST([
    STRUCT('85001' AS zip, 1 AS page, 'success' AS status, 200 AS api_status, ...),
    STRUCT('85002' AS zip, 1 AS page, 'success' AS status, 200 AS api_status, ...),
    ...
  ])
) AS source
ON target.job_id = source.job_id
   AND target.zip = source.zip
   AND target.page = source.page
WHEN MATCHED THEN UPDATE SET ...
```

**Performance Impact**: 8-20x faster than individual UPDATEs (see [Performance Analysis](./docs/performance_analysis.md))

### Early Exit Optimization

`skip_remaining_pages()` implements a defensive optimization:

```python
def skip_remaining_pages(job_id, zip_code, page, results_count):
    # Only skip if page 1 had <10 results
    if page != 1 or results_count >= 10:
        return 0  # No-op

    # Mark pages 2-3 as 'skipped'
    UPDATE serper_queries
    SET status = 'skipped', error = 'early_exit_page1_lt10'
    WHERE job_id = @job_id AND zip = @zip AND page IN (2, 3) AND status = 'queued'
```

**Benefit**: Saves ~66% of API calls for sparse zip codes (rural areas, specific keywords)

---

## Orchestration & Deployment

### Prefect Cloud + GCE Workers

```
┌──────────────────────────────────────────────────────────────┐
│  Prefect Cloud (prefect.io)                                   │
│                                                               │
│  ┌────────────────────────────────────────────────────────┐ │
│  │ Deployments                                            │ │
│  │  - serper-tap/create-job                               │ │
│  │  - serper-tap/process-batches                         │ │
│  └────────────────────────────────────────────────────────┘ │
│                                                               │
│  ┌────────────────────────────────────────────────────────┐ │
│  │ Work Pool: default-pool                                │ │
│  │  Concurrency limit: 10                                 │ │
│  └────────────────────────────────────────────────────────┘ │
└──────────────────────────────────────────────────────────────┘
                             │
                             │ HTTPS + API key
                             ▼
┌──────────────────────────────────────────────────────────────┐
│  GCE VM: serper-tap-worker                                    │
│  (us-central1-a, e2-standard-2)                              │
│                                                               │
│  ┌────────────────────────────────────────────────────────┐ │
│  │ Prefect Worker (systemd service)                       │ │
│  │  - Polls default-pool for flow runs                    │ │
│  │  - Executes flows using ConcurrentTaskRunner           │ │
│  │  - Service account: serper-tap-worker@...             │ │
│  └────────────────────────────────────────────────────────┘ │
│                                                               │
│  Authentication: Application Default Credentials (ADC)       │
│  No keyfiles on disk (see ADR-0002)                          │
└──────────────────────────────────────────────────────────────┘
```

### Application Default Credentials (ADC)

**What**: GCE VMs have an attached service account that provides automatic credentials.

**Benefit** (see [ADR-0002](./docs/decisions/0002-adc-vs-keyfile.md)):
- No keyfiles to manage or rotate
- No secrets in environment variables or on disk
- Automatic credential refresh
- Audit trail via IAM

**Configuration**:
```python
# src/utils/bigquery_client.py
if credentials_path is None or "application_default_credentials.json" in credentials_path:
    credentials, project = google.auth.default(
        scopes=["https://www.googleapis.com/auth/bigquery"]
    )
    client = bigquery.Client(credentials=credentials, project=project)
```

**Deployment**:
```bash
# On GCE VM, no GOOGLE_APPLICATION_CREDENTIALS needed
poetry run serper-process-batches  # Uses ADC automatically
```

### Deployment Architecture

See [`deploy/README.md`](./deploy/README.md) for full deployment guide.

**Key Files**:
- `deploy/01_create_vm.sh` - Provisions GCE VM with service account
- `deploy/02_setup_repo_on_vm.sh` - Clones repo, installs dependencies
- `deploy/03_configure_worker.sh` - Sets up systemd service
- `deploy/04_register_deployment.sh` - Registers flows with Prefect Cloud

**Production Checklist**:
1. ✅ Service account created with BigQuery permissions
2. ✅ VM provisioned with attached service account (ADC)
3. ✅ Poetry installed, dependencies installed
4. ✅ `.env.production` configured (no keyfile path needed)
5. ✅ Prefect worker registered as systemd service
6. ✅ Deployments registered with Prefect Cloud
7. ✅ Health check passes (`serper-health-check`)
8. ✅ Canary job completes (`first_run.sh`)

---

## Performance & Scalability

### Target Metrics

| Metric | Target | Measurement |
|--------|--------|-------------|
| **QPM** | ≥300 queries/minute | 10-minute rolling window |
| **JSON Parse Success** | ≥99.5% | 24-hour window |
| **Early Exit Rate** | ~40-60% | Varies by geography |
| **Cost per 1K Queries** | ~$0.01 | Serper.dev pricing |

### Throughput Analysis

**Single Worker** (concurrency=100, batch_size=150):
- Serper API latency: ~200-400ms per query
- Effective QPM: ~180-250 (network bound)

**Optimizations**:
1. **Batched BigQuery Operations**: 8-20x faster than individual calls
2. **Parallel Query Processing**: Prefect `ConcurrentTaskRunner` with httpx async
3. **Early Exit**: Skips 66% of sparse zip code queries
4. **Job-Level Parallelism**: Multiple jobs can run concurrently

**Achieved Performance** (production):
- QPM: 179-300 (varies by API latency and early exit rate)
- Job completion: 1248 queries in ~6-8 minutes
- BigQuery operations: <1s per batch (150-500 queries)

### Scalability Considerations

**Horizontal Scaling**:
- Add more GCE workers → increase QPM linearly (up to API rate limits)
- Workers coordinate via atomic `claim_id` dequeue
- No shared state besides BigQuery

**Vertical Scaling**:
- Increase `concurrency` setting (default: 100)
- Increase `batch_size` (default: 150, max: 500 due to BigQuery parameter limits)

**Bottlenecks**:
- Serper API rate limits (contact Serper.dev for higher limits)
- BigQuery MERGE parameter limits (10,000 params/query)
- Network bandwidth (GCE → Serper API)

---

## Key Architectural Decisions

### ADR Index

1. **[ADR-0001: SAFE.PARSE_JSON + payload_raw](./docs/decisions/0001-safe-parse-json.md)**
   - **Decision**: Use dual JSON columns (`payload` + `payload_raw`) with `SAFE.PARSE_JSON`
   - **Rationale**: Continue ingestion even if JSON parsing fails; enable forensic analysis
   - **Trade-off**: 2x storage cost for JSON data

2. **[ADR-0002: Application Default Credentials vs Keyfiles](./docs/decisions/0002-adc-vs-keyfile.md)**
   - **Decision**: Use ADC on GCE VMs, avoid keyfiles in production
   - **Rationale**: Better security, no secret management, automatic rotation
   - **Trade-off**: Local dev requires `gcloud auth application-default login`

### Other Key Decisions

**BigQuery as Job Queue**:
- **Pro**: Serverless, infinitely scalable, SQL-based, audit trail, no separate queue service
- **Con**: Higher latency than Redis/RabbitMQ (acceptable for our workload)

**Prefect 2.x for Orchestration**:
- **Pro**: Modern async support, cloud-hosted, great UI, Python-native
- **Con**: Requires separate worker infrastructure (GCE VMs)

**Idempotent MERGE Operations**:
- **Pro**: Retry safety, simpler error handling, no duplicate data
- **Con**: Slightly slower than INSERT (acceptable trade-off)

---

## Operational KPIs

### Health Monitoring

**System Health Check** (`serper-health-check`):
```bash
poetry run serper-health-check --json

{
  "status": "healthy",
  "checks": {
    "bigquery_connection": "ok",
    "prefect_api": "ok",
    "environment_vars": "ok",
    "service_account": "ok"
  },
  "warnings": []
}
```

**Job Progress Monitoring** (`serper-monitor-job`):
```bash
poetry run serper-monitor-job <job_id>

Job: 29ff7292-f104-49b9-a0fb-eb2c044b29de
Status: running
Progress: 1240/1248 queries (99.4%)
Success rate: 99.2%
Places collected: 5428
Cost: $12.40
```

### Performance Queries

**QPM (10-minute window)**:
```sql
SELECT
  ROUND(60.0 * COUNT(*) / NULLIF(TIMESTAMP_DIFF(MAX(ran_at), MIN(ran_at), SECOND), 0), 1) AS qpm
FROM `{project}.{dataset}.serper_queries`
WHERE ran_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 10 MINUTE)
  AND status = 'success';
```

**JSON Parse Health (24-hour)**:
```sql
SELECT
  COUNTIF(payload IS NULL AND payload_raw IS NOT NULL) AS unparsable,
  COUNTIF(payload IS NOT NULL) AS parsed,
  ROUND(100.0 * COUNTIF(payload IS NOT NULL) / COUNT(*), 2) AS success_pct
FROM `{project}.{dataset}.serper_places`
WHERE ingest_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR);
```

**Cost by Day**:
```sql
SELECT
  DATE(created_at) AS date,
  COUNT(*) AS jobs,
  SUM(CAST(totals.queries AS INT64)) AS total_queries,
  ROUND(SUM(CAST(totals.credits AS INT64)) * 0.01, 2) AS cost_usd
FROM `{project}.{dataset}.serper_jobs`
WHERE DATE(created_at) >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
GROUP BY date
ORDER BY date DESC;
```

### Alerting Thresholds

| Condition | Threshold | Action |
|-----------|-----------|--------|
| QPM drops | <250 qpm for 10 min | Check worker health, API rate limits |
| JSON parse failures | <99.0% for 1 hour | Investigate Serper API changes |
| Job stuck | No progress for 30 min | Check worker logs, restart if needed |
| Cost spike | >$50/day unexpected | Review job parameters, check for runaway jobs |

---

## See Also

- [OPERATIONS.md](./OPERATIONS.md) - Operational runbooks and troubleshooting
- [deploy/README.md](./deploy/README.md) - Deployment guide
- [SPECIFICATION.md](./SPECIFICATION.md) - Original technical specification
- [docs/performance_analysis.md](./docs/performance_analysis.md) - Detailed performance benchmarks
- [docs/production_readiness.md](./docs/production_readiness.md) - Production readiness assessment
