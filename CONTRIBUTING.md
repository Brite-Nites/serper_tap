# Contributing to Serper Tap

Thank you for contributing! This guide will help you get started with development.

## Getting Started

### Prerequisites

- Python 3.11
- Poetry (package manager)
- Google Cloud SDK (for authentication)
- Access to Brite Nites BigQuery project

### Initial Setup

```bash
# Clone the repository
git clone <repo-url>
cd serper_tap

# Install Poetry (if not already installed)
curl -sSL https://install.python-poetry.org | python3 -

# Install dependencies (includes dev tools)
poetry install --with dev

# Activate the virtual environment
poetry shell

# Install pre-commit hooks
pre-commit install

# Configure Google Cloud authentication
gcloud auth application-default login
gcloud config set project brite-nites-data-platform
```

### Environment Setup

Copy `.env.example` to `.env` and configure:

```bash
cp .env.example .env
# Edit .env with your settings:
# - BIGQUERY_PROJECT_ID (required)
# - SERPER_API_KEY (for production API calls)
# - USE_MOCK_API=true (recommended for local dev)
```

**Note:** Per [ADR-0002](./docs/decisions/0002-adc-vs-keyfile.md), prefer Application Default Credentials over keyfiles. Only set `GOOGLE_APPLICATION_CREDENTIALS` if ADC is unavailable.

## Development Workflow

### Before You Push - Checklist

Run these checks locally before pushing changes:

```bash
# 1. Format code with Black
poetry run black src scripts/ops tests

# 2. Lint with Ruff (auto-fix safe issues)
poetry run ruff check --fix src scripts/ops tests

# 3. Type check with Mypy
poetry run mypy src

# 4. Run all checks together (recommended)
poetry run pre-commit run --all-files
```

**Pro tip:** Pre-commit hooks will run these checks automatically on commit. If you see failures, fix them and re-commit.

### Running Tests

```bash
# Run all tests
poetry run pytest

# Run with coverage
poetry run pytest --cov=src --cov-report=html

# Run specific test file
poetry run pytest tests/test_operations.py
```

### Testing Locally

```bash
# Use mock API (free, no credits consumed)
export USE_MOCK_API=true

# Create a test job
poetry run serper-create-job --keyword "test" --state "CA" --pages 1

# Process batches (will use mock data)
poetry run serper-process-batches

# Monitor job
poetry run serper-monitor-job <job_id>

# Check system health
poetry run serper-health-check --json
```

## Code Quality Standards

### Formatting

- **Black** is the single source of truth for code formatting
- Line length: 100 characters
- Target: Python 3.11

### Linting

- **Ruff** handles linting and import sorting
- Enabled rules: E (errors), W (warnings), F (pyflakes), I (import sorting), UP (pyupgrade)
- Run `ruff check --fix` to auto-fix safe issues

### Type Checking

- **Mypy** for type checking (src/ only)
- Use type hints for function signatures
- Configuration in `pyproject.toml`

### Pre-commit Hooks

The following hooks run automatically on commit:

1. **Hygiene**: trailing whitespace, EOF newlines, YAML/TOML syntax
2. **Black**: code formatting (enforced)
3. **Ruff**: linting and import sorting (enforced)
4. **Mypy**: type checking (enforced)

To skip hooks (not recommended):
```bash
git commit --no-verify
```

## Project Structure

```
serper_tap/
├── src/                       # Production package (packaged in wheel)
│   ├── cli.py                 # Command-line interface
│   ├── models/                # Pydantic data models
│   ├── utils/                 # Config, BigQuery client
│   ├── operations/            # BigQuery SQL operations
│   │   ├── job_ops.py         # Job lifecycle
│   │   ├── query_ops.py       # Query queue management
│   │   └── place_ops.py       # Place storage
│   ├── tasks/                 # Prefect task wrappers
│   └── flows/                 # Prefect orchestration flows
├── scripts/ops/               # Production operator tools (packaged)
├── scripts/dev/               # Development utilities (NOT packaged)
├── examples/                  # Example flows (NOT packaged)
├── tests/                     # Unit and integration tests
├── sql/                       # BigQuery schema DDL
├── deploy/                    # GCE deployment scripts
└── docs/                      # Architecture decisions, guides
```

**Important:** Code in `examples/` and `scripts/dev/` is excluded from the production wheel. Only production-ready code belongs in `src/` and `scripts/ops/`.

## Making Changes

### 1. Read the Architecture

Before making changes, review:
- **[ARCHITECTURE.md](./ARCHITECTURE.md)** - System design and data flow
- **[ADRs](./docs/decisions/)** - Architecture decision records
- **[OPERATIONS.md](./OPERATIONS.md)** - Operational considerations

### 2. Create a Feature Branch

```bash
git checkout -b feature/your-feature-name
```

### 3. Make Your Changes

- Keep changes focused and atomic
- Write descriptive commit messages
- Add tests for new functionality
- Update documentation as needed

### 4. Run Pre-Push Checklist

```bash
# Format, lint, and type check
poetry run black src scripts/ops tests
poetry run ruff check --fix src scripts/ops tests
poetry run mypy src

# Run tests
poetry run pytest

# Verify pre-commit passes
poetry run pre-commit run --all-files
```

### 5. Commit and Push

```bash
git add .
git commit -m "Add feature: brief description"
git push origin feature/your-feature-name
```

### 6. Create a Pull Request

- Provide a clear description of the change
- Reference any related issues
- Ensure CI passes (GitHub Actions will run checks)

## Common Tasks

### Adding a New Operation

1. Add function to appropriate module in `src/operations/`:
   - `job_ops.py` - Job lifecycle (create, update, mark done)
   - `query_ops.py` - Query queue (enqueue, dequeue, update status)
   - `place_ops.py` - Place storage (store results)

2. Add type hints and docstring

3. Export from `src/operations/__init__.py`

4. Add tests in `tests/`

### Adding a New CLI Command

1. Add command to `src/cli.py` using Typer

2. Test manually:
   ```bash
   poetry run <command-name> --help
   ```

3. Update CLI documentation in ARCHITECTURE.md or OPERATIONS.md

### Adding a New Prefect Flow

1. Create flow in `src/flows/`

2. Wrap operations in tasks (see `src/tasks/`)

3. For production flows, add deployment config

4. For example/demo flows, place in `examples/` (excluded from package)

## Troubleshooting Development Issues

### Pre-commit Hook Failures

If pre-commit hooks fail:
```bash
# View what changed
git diff

# Auto-fix formatting and safe linting issues
poetry run black src scripts/ops tests
poetry run ruff check --fix src scripts/ops tests

# Re-stage and commit
git add .
git commit
```

### Import Errors

If you see import errors after pulling changes:
```bash
# Reinstall dependencies
poetry install --with dev

# Verify installation
poetry run python -c "import src; print('OK')"
```

### Authentication Issues

```bash
# Re-authenticate with Google Cloud
gcloud auth application-default login

# Verify credentials
gcloud auth list
```

## Deprecation Policy

### Deprecated: `src/operations/bigquery_ops.py`

This module is deprecated. Use the new focused modules instead:

```python
# Old (deprecated)
from src.operations.bigquery_ops import create_job

# New (preferred)
from src.operations import create_job
```

See the module header for migration details and removal timeline.

## Getting Help

- **Documentation**: Start with [ARCHITECTURE.md](./ARCHITECTURE.md) and [OPERATIONS.md](./OPERATIONS.md)
- **ADRs**: Check [docs/decisions/](./docs/decisions/) for design rationale
- **Issues**: Search existing issues or create a new one
- **Code Review**: Tag maintainers in pull requests for guidance

## License

Proprietary - Brite Nites
