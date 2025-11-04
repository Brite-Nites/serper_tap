# SPECIFICATION.md

# Serper Place Scraping Pipeline - Technical Specification

## Overview
A production-grade web scraping pipeline that fetches business listings from Serper.dev for specified keywords and geographies. The system uses a queue-based architecture for reliable, idempotent processing at scale with support for concurrent job execution.

## Architecture Pattern
**Async Job Queue Model**: External systems trigger job creation via API, jobs are enqueued in BigQuery, and a processor flow continuously works through queued items until completion.

## Core Flows

### Flow 1: `create_scraping_job`
**Purpose**: Initialize a new scraping job and start processing  
**Trigger**: External API call (webhook/endpoint)  
**Execution Time**: <10 seconds (returns immediately)

**Steps**:
1. Validate and normalize input parameters (keyword, state, pages, batch_size, concurrency)
2. Create job record in `raw_data.serper_jobs` with status='running'
3. Generate all query combinations (zip codes × pages) and insert into `raw_data.serper_queries` with status='queued'
4. Trigger the processor flow asynchronously
5. Return job_id and initial metadata to caller

**Returns**: `{"job_id": "uuid", "status": "running", "total_queries": 1247}`

### Flow 2: `process_job_batches`
**Purpose**: Continuously process batches of queries until all running jobs are complete  
**Trigger**: Called by create_scraping_job, or can be manually triggered for recovery  
**Execution Pattern**: Self-looping until no work remains

**Steps**:
1. Query for all jobs with status='running'
2. If no running jobs found, exit flow
3. For each running job:
   - Atomically dequeue a batch of queries (using claim_id pattern)
   - Process batch in parallel (call Serper API for each query)
   - Write results to `raw_data.serper_places`
   - Update query status in `raw_data.serper_queries`
   - Update job statistics in `raw_data.serper_jobs`
4. Wait 2-5 seconds (rate limiting politeness)
5. Loop back to step 1

## Data Model (BigQuery)

### Table: `raw_data.serper_jobs`
Job-level metadata and rollup statistics.

```sql
job_id: STRING (PK)
keyword: STRING
state: STRING  
pages: INT64
dry_run: BOOL
concurrency: INT64
status: STRING  # 'running' | 'done' | 'failed'
created_at: TIMESTAMP
started_at: TIMESTAMP
finished_at: TIMESTAMP
totals: STRUCT
  zips: INT64,
  queries: INT64,
  successes: INT64,
  failures: INT64,
  places: INT64,
  credits: INT64
>
```

### Table: `raw_data.serper_queries`
Per-query audit trail and queue management.

```sql
job_id: STRING (FK)
zip: STRING
page: INT64
q: STRING  # query text sent to Serper
status: STRING  # 'queued' | 'processing' | 'success' | 'failed' | 'skipped'
claim_id: STRING  # for atomic batch claiming
claimed_at: TIMESTAMP
api_status: INT64
results_count: INT64
credits: INT64
error: STRING
ran_at: TIMESTAMP

PRIMARY KEY (job_id, zip, page)  # ensures idempotency
```

### Table: `raw_data.serper_places`
Raw place data from Serper API.

```sql
ingest_id: STRING (PK)
job_id: STRING (FK)
source: STRING  # 'serper_places'
source_version: STRING  # 'v1'
ingest_ts: TIMESTAMP
keyword: STRING
state: STRING
zip: STRING
page: INT64
place_uid: STRING  # placeId or cid from Serper
payload: JSON  # full Serper response for this place
api_status: INT64
api_ms: INT64
results_count: INT64
credits: INT64
error: STRING

UNIQUE KEY (job_id, place_uid)  # prevents duplicate places
```

### Table: `reference.geo_zip_all`
Reference data for zip codes (assumed to exist).

```sql
zip: STRING
state: STRING
# ... other geo fields
```

## Critical Implementation Details

### Atomic Dequeue Pattern
To prevent race conditions when multiple processor instances run concurrently, we use a claim_id pattern that atomically marks queries as processing before selecting them.

**Implementation**:
```sql
# Generate unique claim_id per dequeue operation
claim_id = f"claim-{int(time.time())}-{uuid.uuid4().hex[:9]}"

# Atomically claim a batch
UPDATE raw_data.serper_queries
SET status = 'processing', claim_id = claim_id, claimed_at = CURRENT_TIMESTAMP()
WHERE job_id = ? AND status = 'queued'
  AND CONCAT(zip, '-', CAST(page AS STRING)) IN (
    SELECT CONCAT(zip, '-', CAST(page AS STRING)) 
    FROM (
      SELECT zip, page, ROW_NUMBER() OVER (ORDER BY zip, page) as rn
      FROM raw_data.serper_queries
      WHERE job_id = ? AND status = 'queued'
    )
    WHERE rn <= batch_size
  );

# Return only queries this instance claimed
SELECT zip, page, q FROM raw_data.serper_queries
WHERE job_id = ? AND claim_id = claim_id
ORDER BY zip, page;
```

**Why this works**: The UPDATE is atomic. Even if two processor instances run simultaneously, BigQuery will execute the UPDATEs serially. The first UPDATE claims a batch and marks them 'processing'. The second UPDATE only sees queries still marked 'queued', so it claims a different batch. Each instance then SELECTs only queries with its unique claim_id.

### Early Exit Optimization
When processing page 1, if results < 10, mark pages 2-3 as 'skipped' to save API credits.

**Implementation**:
```python
if page == 1 and results_count < 10:
    # This zip code has sparse results, skip remaining pages
    UPDATE raw_data.serper_queries
    SET status = 'skipped', error = 'early_exit_page1_lt10', ran_at = CURRENT_TIMESTAMP()
    WHERE job_id = ? AND zip = ? AND page IN (2, 3) AND status = 'queued'
```

**Why this works**: Most zip codes with <10 results on page 1 won't have anything on pages 2-3. By skipping them, we save 2/3 of API calls for sparse areas.

### Idempotent Operations
All data operations use MERGE to prevent duplicates on retry/restart:

**enqueue_queries**:
```sql
MERGE raw_data.serper_queries t
USING (SELECT ...) s
ON t.job_id = s.job_id AND t.zip = s.zip AND t.page = s.page
WHEN NOT MATCHED THEN INSERT ...
```

**store_places**:
```sql
MERGE raw_data.serper_places t
USING (SELECT ...) s  
ON t.job_id = s.job_id AND t.place_uid = s.place_uid
WHEN NOT MATCHED THEN INSERT ...
```

## Serper API Integration

**Endpoint**: `https://google.serper.dev/places`  
**Rate Limit**: 300 requests/second (high throughput, parallelism is safe)

**Request**:
```json
POST /places
Headers: { "X-API-KEY": "..." }
Body: {
  "q": "85001 bars",
  "page": 1,
  "num": 10
}
```

**Response Structure**:
```json
{
  "places": [
    {
      "position": 1,
      "title": "Bar Name",
      "address": "123 Main St",
      "placeId": "ChIJ...",  // or "cid": "..."
      "phoneNumber": "...",
      // ... more fields
    }
  ],
  "credits": 1
}
```

**Error Handling**:
- **200**: Success, parse `places` array and store each place
- **4xx**: Client error (bad request, invalid API key), mark query as 'failed' with error message
- **5xx**: Server error, retry with exponential backoff
- **Timeout**: Retry with exponential backoff
- **No places returned**: Valid response, mark query as 'success' with results_count=0

## Prefect Integration

### Why Prefect?

Prefect provides the orchestration layer that makes this system production-ready without replacing the core queue-based architecture in BigQuery.

**What Prefect Handles:**
- **Execution orchestration**: Running tasks in parallel, managing dependencies, looping until complete
- **Retry logic**: Automatically retry failed Serper API calls with exponential backoff
- **Observability**: Real-time UI showing which flows are running, which tasks succeeded/failed, execution logs
- **Recovery**: If a flow crashes mid-execution, Prefect knows which tasks completed and can resume from the failure point
- **Scheduling**: Can trigger flows on schedules, via API, or based on events

**What BigQuery Handles:**
- **Business state**: Which queries are queued/processing/complete, what data was scraped
- **Source of truth**: Even if Prefect state is lost, BigQuery tells us exactly what work remains
- **Idempotency**: MERGE operations prevent duplicate data regardless of retries or crashes

**The Interaction:**
- Prefect tracks: "Flow run #123 is processing batch 5 of job abc-456"
- BigQuery tracks: "Queries for zips 85003-85050 are marked 'processing' with claim_id xyz-789"
- If Prefect crashes, restarting queries BigQuery for "what's queued?" and continues
- If BigQuery becomes unavailable, Prefect retries the failed tasks automatically until it recovers

### Prefect Decorators & Patterns

**Flows** (`@flow`): Top-level workflows that orchestrate tasks
- `create_scraping_job`: One flow run per job creation request
- `process_job_batches`: Long-running flow that loops until all jobs complete

**Tasks** (`@task`): Individual units of work with retry logic
- `enqueue_queries_task`: Insert queries into BigQuery
- `dequeue_batch_task`: Atomically claim a batch
- `fetch_serper_place`: Call Serper API (retries=3, retry_delay_seconds=5)
- `store_places_task`: Write results to BigQuery
- `update_job_stats_task`: Update totals in serper_jobs table

**Task Mapping** (`.map()`): Parallel execution pattern
```python
@task(retries=3, retry_delay_seconds=5)
def fetch_serper_place(query: dict) -> dict:
    """Single API call to Serper for one query."""
    response = httpx.post(
        "https://google.serper.dev/places",
        headers={"X-API-KEY": settings.serper_api_key},
        json={
            "q": query["query_text"],
            "page": query["page"],
            "num": 10
        },
        timeout=30.0
    )
    response.raise_for_status()
    return response.json()

@flow
def process_batch(job_id: str, batch_size: int):
    """Process one batch of queries for a job."""
    # Dequeue a batch atomically
    queries = dequeue_batch_task(job_id, batch_size)
    
    if not queries:
        return  # No work to do
    
    # Process all queries in parallel (Prefect handles concurrency)
    results = fetch_serper_place.map(queries)
    
    # Store results
    store_places_task(job_id, results)
    
    # Update job statistics
    update_job_stats_task(job_id)
```

**Self-Looping Flow**: Flow 2 triggers itself until complete
```python
@flow
def process_job_batches():
    """Process batches for all running jobs until none remain."""
    while True:
        # Find all running jobs
        running_jobs = get_running_jobs_task()
        
        if not running_jobs:
            logger.info("No running jobs found, exiting processor")
            break
            
        # Process one batch for each running job
        for job in running_jobs:
            process_batch(job["job_id"], job.get("batch_size", 100))
        
        # Rate limiting / politeness delay
        time.sleep(3)
        
    # Flow exits naturally when no work remains
```

### Deployment Strategy

**Local Development:**
```bash
prefect server start  # Local Prefect UI at http://localhost:4200
python -m src.flows.create_job  # Run flows directly for testing
```

**Production Options:**
1. **Prefect Cloud** (managed service): Zero infrastructure, built-in monitoring
2. **Self-hosted Prefect Server**: Run on your own infrastructure  
3. **Hybrid**: Prefect Cloud for orchestration, workers run in your environment

**Triggering Flows:**
- Via API: `POST /deployments/{deployment_id}/flow_runs` with parameters
- Via Python SDK: `create_scraping_job(keyword="bars", state="AZ", pages=3)`
- Via CLI: `prefect deployment run create-job --param keyword=bars --param state=AZ`

### State Persistence

Prefect stores orchestration state (flow runs, task runs, logs) in its database. Business state lives in BigQuery. This separation means:
- ✅ Prefect crash → BigQuery queue intact, restart processing from queue state
- ✅ BigQuery outage → Prefect retries tasks automatically until BigQuery recovers
- ✅ Complete system restart → Both systems resume from their last known states
- ✅ Manual intervention → Can query BigQuery directly to see job progress, reset failed queries, etc.

## Configuration

### Environment Variables (secrets)
```bash
SERPER_API_KEY=your_api_key_here
GOOGLE_APPLICATION_CREDENTIALS=/path/to/bigquery-credentials.json
BIGQUERY_PROJECT_ID=your-gcp-project-id
PREFECT_API_URL=http://localhost:4200/api  # or Prefect Cloud URL
```

### Application Config (constants or config.yaml)
```python
DEFAULT_BATCH_SIZE = 100
DEFAULT_CONCURRENCY = 20  # Not currently used but reserved for future rate limiting
DEFAULT_PAGES = 3
PROCESSOR_LOOP_DELAY_SECONDS = 3
SERPER_TIMEOUT_SECONDS = 30
MAX_RETRIES_PER_QUERY = 3
RETRY_DELAY_SECONDS = 5
```

## Parallelism Strategy

**Job-level parallelism**: Multiple jobs can process batches simultaneously
- Each job maintains its own queue state in BigQuery
- Atomic dequeue with unique claim_ids ensures no collisions
- Process flow can handle multiple jobs in one loop iteration

**Batch-level parallelism**: Within a batch, process queries in parallel
- Use Prefect's `.map()` to parallelize Serper API calls
- Target: 20-50 concurrent requests per batch (configurable)
- Serper supports 300 req/s, so we're well within rate limits
- Each task has independent retry logic

**Concurrency Control**:
```python
# Prefect automatically handles concurrency when using .map()
# The runtime will spawn multiple task runs in parallel up to limits

# Optional: Add task runner for explicit concurrency control
from prefect.task_runners import ConcurrentTaskRunner

@flow(task_runner=ConcurrentTaskRunner())
def process_batch(job_id: str, batch_size: int):
    results = fetch_serper_place.map(queries)  # Runs in parallel
```

## Project Structure
```
serper-scraper/
├── pyproject.toml              # Dependencies and project metadata
├── .env                        # Local secrets (gitignored)
├── .env.example                # Template for required env vars
├── README.md                   # Project overview and setup instructions
├── SPECIFICATION.md            # This document
├── src/
│   ├── __init__.py
│   ├── flows/
│   │   ├── __init__.py
│   │   ├── create_job.py      # Flow 1: create_scraping_job
│   │   └── process_batches.py # Flow 2: process_job_batches
│   ├── tasks/
│   │   ├── __init__.py
│   │   ├── bigquery_tasks.py  # enqueue, dequeue, store, update stats
│   │   └── serper_tasks.py    # fetch_serper_place
│   ├── utils/
│   │   ├── __init__.py
│   │   ├── bigquery_client.py # BQ connection management
│   │   └── config.py          # Config loading from env vars
│   └── models/
│       ├── __init__.py
│       └── schemas.py         # Pydantic models for job params, validation
└── tests/
    ├── __init__.py
    ├── test_bigquery_tasks.py
    ├── test_serper_tasks.py
    └── test_flows.py
```

## Dependencies
```toml
[tool.poetry.dependencies]
python = "^3.10"
prefect = "^2.14"
google-cloud-bigquery = "^3.13"
httpx = "^0.25"  # Modern async HTTP client for Serper
pydantic = "^2.4"
pydantic-settings = "^2.0"  # For loading config from env vars
```

## Success Criteria

**Correctness**:
- Jobs can be triggered programmatically and return job_id immediately
- Multiple jobs can run concurrently without claiming the same queries
- Failed jobs can be restarted; only incomplete queries are reprocessed
- Zero duplicate places in database regardless of retries/restarts (enforced by MERGE)

**Performance** ⚠️ **[UNDER INVESTIGATION - See PERFORMANCE.md]**:
- **Measured (Oct 30, 2025)**: 48.9 queries/minute (1232 queries in 25.2 minutes)
- **Target**: 333-500 queries/minute (not currently achieved)
- **Gap**: System is 85% below target; investigation ongoing
- **Known issues**:
  - Config aliasing bug: `concurrency` used as `batch_size` (should use separate column)
  - Parallelism not effective: batches taking 8-12x longer than expected
  - Real API latency 5x higher than mock (1s vs 0.2s per query)
- Early exit optimization working: 1.3% of queries skipped in production test

**Reliability**:
- If processor crashes mid-batch, restart picks up from BigQuery queue state
- If individual API calls fail, Prefect retries them automatically (3x with backoff)
- If BigQuery is temporarily unavailable, Prefect retries until it recovers
- All operations are idempotent; running the same operation twice is safe

**Observability**:
- Prefect UI shows real-time flow/task status
- BigQuery tables show current job state, query completion, error messages
- Can query job progress at any time: `SELECT status, totals FROM serper_jobs WHERE job_id = ?`

