# Test Suite

Comprehensive test suite for the Serper scraping pipeline.

## Running Tests

### Run all tests
```bash
pytest
```

### Run with verbose output
```bash
pytest -v
```

### Run specific test file
```bash
pytest tests/test_config.py
```

### Run specific test class
```bash
pytest tests/test_cost_tracking.py::TestEstimateJobCost
```

### Run specific test
```bash
pytest tests/test_cost_tracking.py::TestEstimateJobCost::test_small_job
```

### Run tests matching a pattern
```bash
pytest -k "budget"
```

### Run tests with coverage
```bash
pytest --cov=src --cov-report=html
```

This generates an HTML coverage report in `htmlcov/index.html`.

## Test Organization

### `conftest.py`
Shared fixtures and pytest configuration:
- `mock_env_vars`: Sets required environment variables for all tests
- `mock_bigquery_client`: Mocks BigQuery client
- `sample_*`: Sample data fixtures for testing

### `test_config.py`
Tests for configuration management:
- Settings validation
- JobParams validation
- Default value application

### `test_cost_tracking.py`
Tests for cost tracking and budget management:
- Daily credit usage calculation
- Job cost estimation
- Budget validation
- Budget status checking

### `test_health.py`
Tests for health check utilities:
- Configuration health checks
- BigQuery connection health checks
- Overall system health checks

### `test_timing.py`
Tests for timing/performance instrumentation:
- Timing context manager
- Log threshold filtering
- Elapsed time tracking

## Test Markers

Tests can be marked with categories:

```python
@pytest.mark.unit
def test_something():
    pass

@pytest.mark.integration
def test_integration():
    pass

@pytest.mark.slow
def test_slow_operation():
    pass
```

Run only specific markers:
```bash
pytest -m unit      # Run only unit tests
pytest -m "not slow"  # Skip slow tests
```

## Writing New Tests

### Basic test structure
```python
def test_feature_name():
    """Test description."""
    # Arrange
    input_data = {...}

    # Act
    result = function_under_test(input_data)

    # Assert
    assert result == expected_value
```

### Using fixtures
```python
def test_with_fixture(sample_job_params):
    """Test using a shared fixture."""
    params = JobParams(**sample_job_params)
    assert params.keyword == "bars"
```

### Mocking BigQuery
```python
def test_bigquery_operation(mock_execute_query, sample_bigquery_row):
    """Test BigQuery operations with mocks."""
    mock_row = sample_bigquery_row(field1="value1", field2="value2")
    mock_execute_query.return_value = [mock_row]

    result = function_that_queries_bigquery()

    assert result == expected
```

## Continuous Integration

Tests are designed to run in CI environments without external dependencies:
- All database operations are mocked
- Environment variables are set by fixtures
- No network calls to external APIs

## Test Coverage Goals

Target coverage by module:
- Configuration: 100%
- Cost tracking: 95%+
- Health checks: 95%+
- Core utilities: 90%+

Run coverage report:
```bash
pytest --cov=src --cov-report=term-missing
```

This shows which lines are not covered by tests.
