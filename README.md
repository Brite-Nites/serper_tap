# Serper Place Scraping Pipeline

![CI](https://github.com/brite-nites/serper_tap/workflows/CI/badge.svg)
![Python 3.11](https://img.shields.io/badge/python-3.11-blue.svg)
![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)

Production-grade web scraping pipeline that fetches business listings from Serper.dev using a BigQuery-backed queue architecture.

**Key Features:**
- ✅ 300+ queries/minute throughput
- ✅ Zero data loss with dual JSON columns ([ADR-0001](./docs/decisions/0001-safe-parse-json.md))
- ✅ Secure authentication via Application Default Credentials ([ADR-0002](./docs/decisions/0002-adc-vs-keyfile.md))
- ✅ Early exit optimization saves ~66% of API calls

## Quick Start

### Installation

```bash
# Install Poetry and dependencies
curl -sSL https://install.python-poetry.org | python3 -
poetry install

# Set up pre-commit hooks
poetry run pre-commit install

# Configure authentication (see ADR-0002)
gcloud auth application-default login
gcloud config set project your-project-id
```

### Create BigQuery Tables

```bash
bq mk --dataset ${PROJECT_ID}:raw_data
bq mk --dataset ${PROJECT_ID}:reference
bq query --project_id=${PROJECT_ID} < sql/schema.sql
```

### Run a Job

```bash
# Create and process a job
poetry run serper-create-job --keyword "bars" --state "AZ" --pages 3
poetry run serper-process-batches

# Monitor progress
poetry run serper-monitor-job <job_id>
poetry run serper-health-check --json
```

## Documentation

**Core Documentation:**
- **[ARCHITECTURE.md](./ARCHITECTURE.md)** - System design, data flow, BigQuery schema, performance
- **[OPERATIONS.md](./OPERATIONS.md)** - Operational runbooks, monitoring, troubleshooting
- **[CONTRIBUTING.md](./CONTRIBUTING.md)** - Developer setup and workflow
- **[deploy/README.md](./deploy/README.md)** - Production GCE deployment guide

**Architecture Decisions:**
- **[ADR-0001](./docs/decisions/0001-safe-parse-json.md)** - Dual JSON columns with SAFE.PARSE_JSON
- **[ADR-0002](./docs/decisions/0002-adc-vs-keyfile.md)** - Application Default Credentials vs keyfiles

**Additional Resources:**
- [Performance Analysis](./docs/performance_analysis.md) - Benchmarks and optimization details
- [Production Readiness](./docs/production_readiness.md) - Production deployment assessment

## Project Structure

```
serper_tap/
├── src/                       # Production package
│   ├── cli.py                 # Command-line interface
│   ├── operations/            # BigQuery SQL operations
│   ├── tasks/                 # Prefect task wrappers
│   └── flows/                 # Prefect orchestration flows
├── sql/                       # BigQuery schema DDL
├── deploy/                    # GCE deployment scripts
├── scripts/ops/               # Production operator tools
└── docs/                      # Architecture decisions, guides
```

## Troubleshooting

**Authentication errors:**
```bash
gcloud auth application-default login
```

**Permission errors:** Verify service account has `roles/bigquery.dataEditor` and `roles/bigquery.jobUser`

**Table not found:** Run `bq query --project_id=${PROJECT_ID} < sql/schema.sql`

## License

Proprietary - Brite Nites
