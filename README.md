# Serper Place Scraping Pipeline

A production-grade web scraping pipeline that fetches business listings from Serper.dev using a BigQuery-backed queue architecture for reliable, idempotent processing at scale.

## Overview

This pipeline implements an async job queue model where:
- External systems trigger job creation via API
- Jobs are enqueued in BigQuery tables
- Worker processes continuously process queued items until completion
- All operations are idempotent with atomic dequeue guarantees

See [SPECIFICATION.md](./SPECIFICATION.md) for complete technical details.

## Project Status

**Phase 1: BigQuery Foundation Layer** ✅
- ✅ BigQuery schema and table DDL
- ✅ Connection management and configuration
- ✅ Idempotent SQL operations (MERGE patterns)
- ✅ Atomic dequeue with claim_id pattern
- ✅ Type-safe Pydantic models

**Phase 2A: Prefect Task Wrappers + Test Flow** ✅ (Current)
- ✅ Prefect task decorators with retry logic
- ✅ Mock Serper API for testing without spending credits
- ✅ Test flow for single batch processing
- ✅ Helper script to create test jobs

**Phase 2B: Full Production Flows** (Upcoming)
- ⏳ Flow 1: create_scraping_job (full job initialization)
- ⏳ Flow 2: process_job_batches (self-looping processor)
- ⏳ Real Serper API integration
- ⏳ Production error handling and monitoring

## Prerequisites

- Python 3.10 or higher
- Google Cloud Platform account with BigQuery enabled
- BigQuery service account with appropriate permissions
- (Phase 2) Serper.dev API key

## Setup Instructions

### 1. Clone and Install Dependencies

```bash
# Install Poetry if you haven't already
curl -sSL https://install.python-poetry.org | python3 -

# Install project dependencies
poetry install

# Activate the virtual environment
poetry shell
```

### 2. Configure Google Cloud BigQuery

#### Create Service Account
1. Go to [GCP Console](https://console.cloud.google.com)
2. Navigate to IAM & Admin → Service Accounts
3. Create a new service account with these roles:
   - BigQuery Data Editor
   - BigQuery Job User
4. Create and download a JSON key file

#### Create BigQuery Datasets and Tables
```bash
# Set your GCP project ID
export PROJECT_ID="your-gcp-project-id"

# Create the raw_data dataset
bq mk --dataset ${PROJECT_ID}:raw_data

# Create the reference dataset (if it doesn't exist)
bq mk --dataset ${PROJECT_ID}:reference

# Run the schema creation script
bq query --project_id=${PROJECT_ID} < sql/schema.sql
```

Alternatively, run the DDL manually in BigQuery Console:
1. Open [BigQuery Console](https://console.cloud.google.com/bigquery)
2. Copy contents of `sql/schema.sql`
3. Execute in the query editor

### 3. Configure Environment Variables

```bash
# Copy the example environment file
cp .env.example .env

# Edit .env with your credentials
nano .env
```

Update the following values in `.env`:
```bash
GOOGLE_APPLICATION_CREDENTIALS=/path/to/your-service-account-key.json
BIGQUERY_PROJECT_ID=your-gcp-project-id
BIGQUERY_DATASET=raw_data
```

### 4. Verify Setup

Test your BigQuery connection:

```python
from src.utils.bigquery_client import get_bigquery_client

# This should succeed without errors
client = get_bigquery_client()
print(f"Connected to project: {client.project}")
```

## Testing BigQuery Operations (Phase 1)

Since we haven't built the Prefect flows yet, you can test the BigQuery operations directly:

```python
import uuid
from src.models.schemas import JobParams
from src.operations import bigquery_ops

# 1. Create a test job
job_id = str(uuid.uuid4())
params = JobParams(
    keyword="bars",
    state="AZ",
    pages=3,
    batch_size=100,
    concurrency=20
)

result = bigquery_ops.create_job(job_id, params)
print(f"Created job: {result}")

# 2. Get zip codes for Arizona
zips = bigquery_ops.get_zips_for_state("AZ")
print(f"Found {len(zips)} zip codes in AZ")

# 3. Enqueue queries for the first 5 zip codes
queries = []
for zip_code in zips[:5]:
    for page in range(1, params.pages + 1):
        queries.append({
            "zip": zip_code,
            "page": page,
            "q": f"{zip_code} {params.keyword}"
        })

inserted = bigquery_ops.enqueue_queries(job_id, queries)
print(f"Enqueued {inserted} queries")

# 4. Test atomic dequeue
batch = bigquery_ops.dequeue_batch(job_id, batch_size=10)
print(f"Dequeued {len(batch)} queries")
print(f"First query: {batch[0] if batch else 'None'}")

# 5. Test early exit optimization
# Simulate page 1 returning only 5 results
skipped = bigquery_ops.skip_remaining_pages(
    job_id=job_id,
    zip_code=zips[0],
    page=1,
    results_count=5
)
print(f"Skipped {skipped} remaining pages")

# 6. Get job status
status = bigquery_ops.get_job_status(job_id)
print(f"Job status: {status}")
```

## Testing Prefect Integration (Phase 2A)

Phase 2A adds Prefect orchestration with a simple test flow that processes one batch.

### Quick Start: Test the Full Pipeline

```bash
# 1. Create a test job with queued queries
python scripts/setup_test_job.py

# This will output a job_id like: 12345678-1234-1234-1234-123456789abc

# 2. Process one batch of queries using the test flow
python -m src.flows.test_batch <job_id> 10

# 3. Check results in BigQuery Console
# - Queries should move from 'queued' → 'success'
# - Places should appear in serper_places table
# - Job stats should be updated
```

### What the Test Flow Does

The `test_batch_processing` flow demonstrates the full cycle:

1. **Dequeue batch** - Atomically claims queries using claim_id pattern
2. **Parallel processing** - Uses Prefect's `.map()` to process queries concurrently
3. **Mock API calls** - Returns random results (0-10 places) without spending Serper credits
4. **Early exit optimization** - Automatically skips remaining pages for sparse zips
5. **Store results** - Uses idempotent MERGE to prevent duplicates
6. **Update statistics** - Recalculates job totals

### Customizing Test Jobs

```bash
# Create a larger test job
python scripts/setup_test_job.py --state CA --keyword restaurants --zips 10 --pages 3

# Create a minimal test job
python scripts/setup_test_job.py --state NY --keyword cafes --zips 2 --pages 2
```

### Verifying Results

After running the test flow, verify in BigQuery:

```sql
-- Check job status
SELECT status, totals
FROM `{project}.raw_data.serper_jobs`
WHERE job_id = '<job_id>';

-- Check query progression
SELECT status, COUNT(*) as count
FROM `{project}.raw_data.serper_queries`
WHERE job_id = '<job_id>'
GROUP BY status;

-- Check places collected
SELECT COUNT(*) as total_places, COUNT(DISTINCT place_uid) as unique_places
FROM `{project}.raw_data.serper_places`
WHERE job_id = '<job_id>';
```

### Mock vs. Real API

Phase 2A uses a **mock Serper API** that:
- Returns random results (0-10 places per query)
- Doesn't consume real API credits
- Simulates network latency (0.1-0.3 seconds)
- Tests both dense and sparse zip codes

The real Serper API integration will be added in Phase 2B.

## Architecture Highlights

### Atomic Dequeue Pattern
The `dequeue_batch()` function prevents race conditions when multiple workers run concurrently:
1. Generates unique `claim_id` per worker
2. Atomically UPDATEs queries to 'processing' status
3. SELECTs only queries with that worker's claim_id

Even if two workers query simultaneously, BigQuery serializes the UPDATEs, ensuring each worker claims a different batch.

### Idempotency via MERGE
All write operations use `MERGE` statements:
- `enqueue_queries()`: MERGE ON `(job_id, zip, page)` - safe to re-run
- `store_places()`: MERGE ON `(job_id, place_uid)` - prevents duplicate places

This makes retries safe: running the same operation twice produces the same result as running it once.

### Early Exit Optimization
The `skip_remaining_pages()` function is defensive:
- Only marks pages 2-3 as 'skipped' when `page==1 AND results_count<10`
- For any other input combination, it's a no-op returning 0
- Saves ~66% of API calls for sparse zip codes

## Project Structure

```
serper_tap/
├── pyproject.toml              # Dependencies and project metadata
├── .env                        # Local secrets (gitignored)
├── .env.example                # Template for environment variables
├── README.md                   # This file
├── SPECIFICATION.md            # Detailed technical specification
├── sql/
│   └── schema.sql              # BigQuery table DDL
├── scripts/
│   └── setup_test_job.py       # Helper to create test jobs (Phase 2A)
├── src/
│   ├── models/
│   │   └── schemas.py          # Pydantic models for type safety
│   ├── utils/
│   │   ├── config.py           # Environment variable management
│   │   └── bigquery_client.py  # BigQuery connection factory
│   ├── operations/
│   │   └── bigquery_ops.py     # Plain Python functions for SQL operations
│   ├── tasks/                  # Prefect tasks (Phase 2A)
│   │   ├── bigquery_tasks.py   # Task wrappers for BigQuery ops
│   │   └── serper_tasks.py     # Mock Serper API task
│   └── flows/                  # Prefect flows (Phase 2A)
│       └── test_batch.py       # Test flow for single batch processing
└── tests/
    └── test_bigquery_ops.py    # Unit tests (to be implemented)
```

## Next Steps (Phase 2B)

Once Phase 2A is verified:
1. Implement full production flows:
   - `src/flows/create_job.py` - Flow 1: Job initialization with query generation
   - `src/flows/process_batches.py` - Flow 2: Self-looping batch processor
2. Add real Serper API integration (replace mock)
3. Add comprehensive error handling for API failures
4. Set up Prefect deployments for production
5. Add monitoring and alerting

## Troubleshooting

### "Credentials file not found"
Ensure `GOOGLE_APPLICATION_CREDENTIALS` in `.env` points to a valid JSON key file.

### "Permission denied" errors
Verify your service account has these roles:
- BigQuery Data Editor
- BigQuery Job User

### "Table not found" errors
Run the DDL from `sql/schema.sql` in BigQuery Console to create tables.

### Import errors
Make sure you're in the Poetry shell:
```bash
poetry shell
```

## Contributing

See [SPECIFICATION.md](./SPECIFICATION.md) for system design details before making changes.

## License

Proprietary - Brite Nites
