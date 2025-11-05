# Serper Place Scraping Pipeline

A production-grade web scraping pipeline that fetches business listings from Serper.dev using a BigQuery-backed queue architecture for reliable, idempotent processing at scale.

## What It Does

This pipeline implements an async job queue model where:
- External systems trigger job creation via CLI
- Jobs are decomposed into queries and enqueued in BigQuery
- Prefect workers on GCE continuously process queued items
- All operations are idempotent with atomic dequeue guarantees

**Key Features:**
- ✅ 300+ queries/minute throughput
- ✅ Zero data loss with dual JSON columns ([ADR-0001](./docs/decisions/0001-safe-parse-json.md))
- ✅ Secure authentication via Application Default Credentials ([ADR-0002](./docs/decisions/0002-adc-vs-keyfile.md))
- ✅ Early exit optimization saves ~66% of API calls

## Quick Start

### Prerequisites

- Python 3.11
- Google Cloud project with BigQuery enabled
- (Production) GCE VM with attached service account

### Installation

```bash
# Install Poetry
curl -sSL https://install.python-poetry.org | python3 -

# Clone and install dependencies
git clone <repo-url>
cd serper_tap
poetry install

# Activate environment
poetry shell
```

### Configuration

**Local Development:**
```bash
# Option 1: Use gcloud ADC (recommended)
gcloud auth application-default login
gcloud config set project your-project-id

# Option 2: Use keyfile
cp .env.example .env
# Edit .env:
#   GOOGLE_APPLICATION_CREDENTIALS=/path/to/keyfile.json
#   BIGQUERY_PROJECT_ID=your-project
#   BIGQUERY_DATASET=raw_data
```

**Production:** See [deploy/README.md](./deploy/README.md) for GCE deployment guide.

### Create BigQuery Tables

```bash
# Create datasets
bq mk --dataset ${PROJECT_ID}:raw_data
bq mk --dataset ${PROJECT_ID}:reference

# Run schema DDL
bq query --project_id=${PROJECT_ID} < sql/schema.sql
```

### Run a Job

```bash
# Create a job
poetry run serper-create-job \
  --keyword "bars" \
  --state "AZ" \
  --pages 3

# Output: job_id=29ff7292-f104-49b9-a0fb-eb2c044b29de

# Process batches (runs until job completes)
poetry run serper-process-batches

# Monitor progress
poetry run serper-monitor-job 29ff7292-f104-49b9-a0fb-eb2c044b29de
```

### Health Check

```bash
poetry run serper-health-check --json
```

## Documentation

- **[ARCHITECTURE.md](./ARCHITECTURE.md)** - System design, data flow, BigQuery schema, performance
- **[OPERATIONS.md](./OPERATIONS.md)** - Operational runbooks, monitoring, troubleshooting
- **[deploy/README.md](./deploy/README.md)** - Production deployment guide
- **[SPECIFICATION.md](./SPECIFICATION.md)** - Original technical specification

### Architecture Decisions

- **[ADR-0001](./docs/decisions/0001-safe-parse-json.md)** - Dual JSON columns with SAFE.PARSE_JSON
- **[ADR-0002](./docs/decisions/0002-adc-vs-keyfile.md)** - Application Default Credentials vs keyfiles

### Additional Documentation

- [Performance Analysis](./docs/performance_analysis.md) - Benchmarks and optimization details
- [Production Readiness](./docs/production_readiness.md) - Production deployment assessment

## Project Structure

```
serper_tap/
├── src/
│   ├── cli.py                 # Command-line interface (4 commands)
│   ├── models/                # Pydantic schemas
│   ├── utils/                 # Config, BigQuery client, timing
│   ├── operations/            # Plain Python SQL operations
│   ├── tasks/                 # Prefect task wrappers
│   └── flows/                 # Prefect orchestration flows
├── sql/                       # BigQuery schema DDL
├── deploy/                    # GCE deployment scripts
├── scripts/ops/               # Production operator tools
├── scripts/dev/               # Development utilities
├── examples/                  # Example flows (not packaged)
└── docs/                      # Architecture decisions, guides
```

## Troubleshooting

**"Could not determine credentials"**
```bash
gcloud auth application-default login
```

**"Permission denied" on BigQuery**
- Verify service account has `roles/bigquery.dataEditor` and `roles/bigquery.jobUser`

**"Table not found"**
```bash
bq query --project_id=${PROJECT_ID} < sql/schema.sql
```

## Contributing

See [ARCHITECTURE.md](./ARCHITECTURE.md) for system design details before making changes.

## License

Proprietary - Brite Nites
