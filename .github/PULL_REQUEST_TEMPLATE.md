## Description

<!-- Provide a clear and concise description of what this PR does -->

## Type of Change

<!-- Mark the relevant option with an 'x' -->

- [ ] Bug fix (non-breaking change which fixes an issue)
- [ ] New feature (non-breaking change which adds functionality)
- [ ] Breaking change (fix or feature that would cause existing functionality to not work as expected)
- [ ] Documentation update
- [ ] Refactoring (no functional changes)
- [ ] Performance improvement
- [ ] Deployment/infrastructure change

## Related Issues

<!-- Link to related issues, e.g., "Fixes #123" or "Related to #456" -->

Fixes #

## Changes Made

<!-- Provide a bulleted list of changes -->

-
-
-

## Testing

<!-- Describe the testing you've done -->

### Manual Testing

- [ ] Tested locally with mock API
- [ ] Tested locally with real Serper API
- [ ] Tested on GCE VM (production-like environment)

### Automated Testing

- [ ] Unit tests added/updated
- [ ] Integration tests added/updated
- [ ] All tests pass locally

**Test commands run:**
```bash
poetry run pytest -v
```

## Code Quality Checklist

<!-- All items must be checked before merging -->

- [ ] Code follows the style guidelines (Black, Ruff)
- [ ] I have run `poetry run black src scripts/ops tests`
- [ ] I have run `poetry run ruff check --fix src scripts/ops tests`
- [ ] I have run `poetry run mypy src`
- [ ] I have run `poetry run pytest -v`
- [ ] Pre-commit hooks pass (`poetry run pre-commit run --all-files`)
- [ ] No sensitive data (API keys, credentials) is committed

## Documentation

<!-- Mark all that apply -->

- [ ] Updated ARCHITECTURE.md (if system design changed)
- [ ] Updated OPERATIONS.md (if operational procedures changed)
- [ ] Updated CONTRIBUTING.md (if dev workflow changed)
- [ ] Updated README.md (if user-facing features changed)
- [ ] Added/updated docstrings for new/modified functions
- [ ] Created/updated ADR if architectural decision was made

## Database Changes

<!-- If this PR modifies BigQuery schema or data, check applicable items -->

- [ ] No database changes
- [ ] Schema changes are backward compatible
- [ ] Migration script added to `sql/migrations/`
- [ ] Tested migration on dev BigQuery project
- [ ] Updated `sql/schema.sql` with new DDL

## Deployment Impact

<!-- Describe any deployment considerations -->

- [ ] No deployment changes required
- [ ] Requires environment variable changes (documented in PR description)
- [ ] Requires BigQuery schema migration (documented above)
- [ ] Requires GCE VM restart
- [ ] Requires Prefect worker restart

**Deployment notes:**
<!-- Add any special deployment instructions here -->

## Performance Impact

<!-- If applicable, describe performance implications -->

- [ ] No performance impact
- [ ] Performance improvement (describe below)
- [ ] Potential performance regression (describe mitigation below)

**Performance notes:**

## Breaking Changes

<!-- If this is a breaking change, describe the impact and migration path -->

**Impact:**

**Migration path:**

## Screenshots/Logs

<!-- If applicable, add screenshots or log outputs to help explain your changes -->

## Reviewer Notes

<!-- Add any notes for reviewers (areas of focus, specific concerns, etc.) -->

## Post-Merge Checklist

<!-- Items to complete after merging -->

- [ ] Monitor production metrics for 24 hours
- [ ] Update operational runbooks if procedures changed
- [ ] Close related issues
- [ ] Announce changes to team (if user-facing)
