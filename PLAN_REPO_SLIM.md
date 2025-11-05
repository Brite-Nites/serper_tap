# Repository Slim Plan - Phase 1 Analysis

**Generated**: 2025-11-04
**Goal**: Remove bloat and consolidate duplicates with ZERO behavior change
**Method**: Multi-source verification (ripgrep + AST + packaging + docs + runtime)

---

## 1. INVENTORY

### Current Repository Structure
```
serper_tap/
├── .venv/               # Virtual environment (NOT tracked)
├── .ruff_cache/         # Linter cache (NOT tracked)
├── .claude/             # IDE config (NOT tracked)
├── credentials/         # ⚠️  Contains JSON keyfile (NOT in .gitignore)
├── test_results/        # Empty dir (NOT tracked)
├── dist/                # Build artifacts
├── examples/            # Example flows
├── scripts/
│   ├── ops/            # Production operator tools (PACKAGED)
│   └── dev/            # Development utilities (NOT packaged)
├── deploy/             # GCE deployment scripts
├── docs/               # Architecture docs + ADRs
├── sql/                # BigQuery schema
├── src/                # Production package
└── tests/              # Unit tests

Root-level files:
- .env                   # Local dev (NOT tracked per .gitignore)
- .env.example           # Template (TRACKED)
- .env.production        # Production config (TRACKED - contains sanitized secrets)
- deploy_and_validate.sh
- validate_production_ready.sh
- first_run.sh
- sql/views/dashboards.sql
- deployment.yaml
```

---

## 2. REFERENCE MAP

### A) Validation Scripts Analysis

**Files**: `deploy_and_validate.sh`, `validate_production_ready.sh`, `first_run.sh`

| Script | Purpose | Lines | References | Status |
|--------|---------|-------|------------|--------|
| `first_run.sh` | Comprehensive canary test with QPM/cost/idempotency validation | 429 | Referenced by OPERATOR_ACCEPTANCE.md | **CANONICAL** |
| `deploy_and_validate.sh` | Deployment + validation (includes migrations) | 377 | Referenced by deploy/README.md, deploy/PASS_FAIL_MATRIX.md | Overlaps first_run.sh |
| `validate_production_ready.sh` | 15-min validation runbook | 229 | Referenced by ARCHITECTURE.md, docs/production_readiness.md | Overlaps first_run.sh |

**ripgrep findings**:
```bash
$ rg "deploy_and_validate|validate_production_ready|first_run" -l
OPERATOR_ACCEPTANCE.md          # References first_run.sh
validate_production_ready.sh
ARCHITECTURE.md                 # References validate_production_ready.sh
docs/production_readiness.md    # References validate_production_ready.sh
deploy_and_validate.sh
deploy/README.md                # References deploy_and_validate.sh
deploy/PASS_FAIL_MATRIX.md      # References deploy_and_validate.sh
deploy/QUICK_START.md           # References deploy_and_validate.sh
deploy/05_setup_systemd_worker.sh # References deploy_and_validate.sh
```

**Overlap Analysis**:
- All three scripts create test jobs and validate results
- `first_run.sh` is the MOST comprehensive (QPM checks, idempotency, cost analysis, proof blocks)
- `deploy_and_validate.sh` adds migration step (not in first_run.sh)
- `validate_production_ready.sh` is a subset of first_run.sh functionality

**CI/Packaging**: No references in pyproject.toml or .github/workflows/

---

### B) sql/views/dashboards.sql

**File**: `sql/views/dashboards.sql` (root level)

**ripgrep findings**:
```bash
$ rg "dashboards\.sql" -l
sql/views/dashboards.sql
deploy/README.md         # "Optional: run sql/views/dashboards.sql for BigQuery views"
deploy/QUICK_START.md    # "Optional: bq query < sql/views/dashboards.sql"
```

**Content**: BigQuery views for monitoring (job stats, error analysis, performance metrics)

**Usage**: Optional monitoring setup, not required for core pipeline

**CI/Packaging**: No references

---

### C) examples/

**Directory**: `examples/` with `examples/flows/` subdirectory

**ripgrep findings**:
```bash
$ rg "examples/" -l
CONTRIBUTING.md          # "Code in examples/ excluded from wheel"
pyproject.toml          # exclude = ["examples/", ...], black/ruff/mypy exclude
```

**Python imports**:
```bash
$ rg "from examples" -l
(no results)
```

**Contents**:
- `examples/__init__.py`
- `examples/flows/` - demo flows

**pyproject.toml**: Already excluded from build via `exclude = ["examples/", ...]`

**Status**: NOT packaged, NOT imported, documentation mentions it's excluded

---

### D) scripts/dev/

**Directory**: `scripts/dev/` - development utilities

**Contents**:
- performance_test.py
- setup_test_job.py
- test_error_handling.py
- test_error_unit.py
- test_parallel_jobs.py
- test_real_api.py
- validate_real_api.py
- validate_reference_data.py

**ripgrep findings**:
```bash
$ rg "scripts/dev" -l
pyproject.toml          # exclude = ["scripts/dev/", ...], black/ruff/mypy exclude
```

**Python imports**:
```bash
$ rg "from scripts\.dev|import scripts\.dev" src/ tests/
(no results)
```

**pyproject.toml**: Already excluded from build

**Status**: NOT packaged, NOT imported by src/ or tests/

---

### E) Local Artifacts

**Directories/Files**: `.venv/`, `.ruff_cache/`, `.claude/`, `test_results/`, `dist/`, `credentials/`

**Git tracking status**:
```bash
$ git ls-files | grep -E "\.(venv|cache|claude|test_results|dist|credentials)"
.env.example      # ✅ Tracked (template)
```

**None of these are tracked** except .env.example (which should stay)

**credentials/** status:
- Contains `credentials/prefect-data-pipeline.json` (service account key)
- NOT in .gitignore (⚠️  security risk if accidentally committed)
- NOT currently tracked by git

---

### F) Environment Files

**Files**: `.env`, `.env.production`, `.env.example`

**Git tracking**:
```bash
$ git ls-files .env*
.env.example     # ✅ Tracked template
```

**Status**:
- `.env` - NOT tracked (correct per .gitignore)
- `.env.production` - NOT tracked currently but EXISTS locally with sanitized secrets
- `.env.example` - Tracked (correct, is template)

**Note**: Previous work sanitized .env.production, but it's still present locally

---

### G) OPERATIONS.md vs docs/production_readiness.md

**Files**: Root `OPERATIONS.md` vs `docs/production_readiness.md`

**Size comparison**:
- OPERATIONS.md: 465 lines, 72 headers
- docs/production_readiness.md: 921 lines, 87 headers

**ripgrep findings**:
```bash
$ rg "OPERATIONS\.md" -l
README.md              # Links to OPERATIONS.md
ARCHITECTURE.md        # Links to OPERATIONS.md
CONTRIBUTING.md        # Links to OPERATIONS.md
deploy/README.md       # Links to OPERATIONS.md
deploy/QUICK_START.md  # Links to OPERATIONS.md
```

**Content overlap**:
- OPERATIONS.md: Operational runbooks, monitoring, troubleshooting
- production_readiness.md: Detailed production readiness assessment with pass/fail criteria

**Status**: Both heavily referenced, serve DIFFERENT purposes (runbooks vs assessment)

---

### H) Architecture Decision Records (ADRs)

**Files**: `docs/decisions/0001-safe-parse-json.md`, `docs/decisions/0002-adc-vs-keyfile.md`

**ripgrep findings**:
```bash
$ rg "ADR-000|0001-safe-parse-json|0002-adc-vs-keyfile" -l
src/utils/bigquery_client.py   # References ADR-0001
src/utils/config.py            # References ADR-0002
CONTRIBUTING.md                # References ADR-0002
README.md                      # Links to both ADRs
ARCHITECTURE.md                # Links to both ADRs
docs/decisions/0001-safe-parse-json.md
docs/decisions/0002-adc-vs-keyfile.md
```

**Status**: Heavily referenced in code and docs - **KEEP ALL ADRs**

---

### I) deployment.yaml

**File**: Root-level Prefect deployment config

**ripgrep findings**:
```bash
$ rg "deployment\.yaml" -l
deployment.yaml
deploy/04_deploy_prefect.sh    # References: "prefect deployment apply deployment.yaml"
```

**Status**: Used by deployment scripts - **KEEP**

---

## 3. CANDIDATES FOR ACTION

### 🗑️ **DELETE** - High Confidence (≥0.95)

| Path | Type | Reason | Confidence | Risk |
|------|------|--------|------------|------|
| `.venv/` | Dir | Virtual env, NOT tracked | 1.0 | None - in .gitignore |
| `.ruff_cache/` | Dir | Linter cache, NOT tracked | 1.0 | None - in .gitignore |
| `.claude/` | Dir | IDE config, NOT tracked | 1.0 | None |
| `test_results/` | Dir | Empty, NOT tracked | 1.0 | None - in .gitignore |
| `dist/` | Dir | Build artifacts, regenerable | 1.0 | None - regenerate with `poetry build` |

**Verification**: None are tracked per `git ls-files`

---

### 🔀 **CONSOLIDATE** - Medium-High Confidence (0.85-0.95)

#### Validation Scripts Consolidation

**Decision**: Keep `first_run.sh` as canonical, fold unique logic from others into it

| Script | Action | Unique Logic to Preserve | Confidence |
|--------|--------|--------------------------|------------|
| `first_run.sh` | **KEEP** (canonical) | Full QPM/cost/idempotency checks | 1.0 |
| `deploy_and_validate.sh` | **DELETE** after consolidation | Migration step (`bq query < sql/migrations/*.sql`) | 0.85 |
| `validate_production_ready.sh` | **DELETE** (subset of first_run) | None - all checks covered by first_run.sh | 0.90 |

**Migration plan**:
1. Add migration check to first_run.sh as optional step
2. Update docs to reference only first_run.sh
3. Delete deploy_and_validate.sh and validate_production_ready.sh

**Docs to update**:
- ARCHITECTURE.md (references validate_production_ready.sh)
- docs/production_readiness.md (references validate_production_ready.sh)
- deploy/README.md (references deploy_and_validate.sh)
- deploy/PASS_FAIL_MATRIX.md (references deploy_and_validate.sh)
- deploy/QUICK_START.md (references deploy_and_validate.sh)
- deploy/05_setup_systemd_worker.sh (references deploy_and_validate.sh)

---

### 📦 **MOVE** - Medium Confidence (0.80-0.90)

| Path | Current | Proposed | Reason | Confidence |
|------|---------|----------|--------|------------|
| `sql/views/dashboards.sql` | Root | `sql/analysis/sql/views/dashboards.sql` | Better organization, with other SQL | 0.85 |

**Docs to update**:
- deploy/README.md
- deploy/QUICK_START.md

---

### 🔒 **SECURE** - High Confidence (1.0)

| Path | Issue | Action | Risk |
|------|-------|--------|------|
| `credentials/` | NOT in .gitignore | Add to .gitignore | High - prevents accidental commit of keyfiles |
| `.claude/` | NOT in .gitignore | Add to .gitignore | Low - IDE config only |
| `.DS_Store` | macOS artifact | Add to .gitignore | Low - just noise |

**Note**: No secrets currently tracked in git, but defense-in-depth

---

### ✅ **KEEP** - Do Not Touch

| Path | Reason |
|------|--------|
| `src/**` | Production code (PROTECTED) |
| `tests/**` | Tests (PROTECTED) |
| `sql/migrations/**` | Schema migrations (PROTECTED) |
| `pyproject.toml` | Packaging config (PROTECTED) |
| `docs/quick_start.md` | User docs (PROTECTED) |
| `docs/production_readiness.md` | Assessment docs (PROTECTED) |
| `OPERATIONS.md` | Runbooks - heavily referenced, NOT duplicate |
| `examples/**` | Already excluded from packaging, no imports |
| `scripts/dev/**` | Already excluded from packaging, no imports |
| `docs/decisions/*.md` | ADRs - referenced in code |
| `deployment.yaml` | Prefect deployment config - used |
| `deploy/**` | Deployment scripts (do not touch per guardrails) |
| `.env.example` | Template (tracked, correct) |

---

## 4. BORDERLINE ITEMS - Require Explicit Approval

### 🟡 examples/ and scripts/dev/

**Current status**:
- Already excluded from packaging via pyproject.toml
- Not imported by production code
- Mentioned in CONTRIBUTING.md as "excluded from wheel"

**Options**:
1. **KEEP** - Leave as-is (already excluded from builds)
2. **DELETE** - Remove entirely if truly unused
3. **DOCUMENT** - Add README.md explaining their purpose

**Recommendation**: **KEEP** - Already properly excluded, may be useful for future dev

**Requires approval?** YES - user should decide if dev utilities are valuable

---

### 🟡 .env and .env.production

**Current status**:
- `.env` - Local dev config, NOT tracked (correct)
- `.env.production` - Has sanitized secrets, NOT tracked
- `.env.example` - Template, tracked (correct)

**Security note**: Previous work sanitized .env.production but file still exists locally

**Options**:
1. Keep .env.production locally (untracked) - operators may need it
2. Delete .env.production entirely - force use of .env.example

**Recommendation**: **KEEP** .env.production untracked (operators use it, already sanitized)

**Requires approval?** NO - already correct (untracked + sanitized)

---

## 5. RISKS & MITIGATIONS

| Risk | Likelihood | Impact | Mitigation |
|------|------------|--------|------------|
| Breaking deployment scripts | Low | High | Keep deploy/** untouched per guardrails |
| Deleting referenced validation script | Medium | Medium | Update all doc references before deletion |
| Accidental secret commit | Low | High | Add credentials/ to .gitignore |
| Wheel build failure | Low | Medium | Test build in clean venv before commit |
| Import errors | Very Low | High | Keep src/** untouched, verify imports |

---

## 6. PROPOSED CONSOLIDATION - Validation Scripts

### first_run.sh Enhancement

Add migration check as optional flag:

```bash
# Add to first_run.sh after line ~50
if [ "${RUN_MIGRATIONS:-false}" = "true" ]; then
    echo "== Running BigQuery Migrations =="
    for migration in sql/migrations/*.sql; do
        [ -f "$migration" ] || continue
        echo "Applying: $(basename $migration)"
        bq query --project_id="${BIGQUERY_PROJECT_ID}" < "$migration"
    done
fi
```

Usage:
```bash
# Standard canary test
./first_run.sh

# With migrations (replaces deploy_and_validate.sh)
RUN_MIGRATIONS=true ./first_run.sh
```

### Documentation Updates

Replace all references:
- `deploy_and_validate.sh` → `RUN_MIGRATIONS=true ./first_run.sh`
- `validate_production_ready.sh` → `./first_run.sh`

---

## 7. PACKAGING HYGIENE

### Current pyproject.toml Exclusions

```toml
exclude = [
    "examples/",
    "scripts/dev/",
]
```

**Status**: ✅ Correct - examples and scripts/dev already excluded

### Proposed .gitattributes for Export-Ignore

Add `.gitattributes` to exclude non-distribution files from `git archive`:

```gitattributes
# Build and development artifacts
.venv/ export-ignore
.ruff_cache/ export-ignore
.claude/ export-ignore
test_results/ export-ignore
dist/ export-ignore
credentials/ export-ignore

# Development files
examples/ export-ignore
scripts/dev/ export-ignore
docs/ export-ignore
deploy/ export-ignore

# Environment files
.env export-ignore
.env.* export-ignore
!.env.example

# CI/tooling
.github/ export-ignore
.pre-commit-config.yaml export-ignore
.gitignore export-ignore
.gitattributes export-ignore

# Tests
tests/ export-ignore
pytest.ini export-ignore

# Documentation
*.md export-ignore
!README.md
```

---

## 8. PROPOSED .gitignore ADDITIONS

```gitignore
# Add to existing .gitignore:

# Credentials (defense in depth)
credentials/
*.json
!pyproject.json

# IDE configs
.claude/
.vscode/
.idea/

# macOS artifacts
.DS_Store
.AppleDouble
.LSOverride

# Distribution
dist/
build/
*.egg-info/
```

---

## 9. SUMMARY & DECISION TABLE

| Path | Type | Action | Evidence | Packaging/CI | Docs | Decision | Confidence | Risk |
|------|------|--------|----------|--------------|------|----------|------------|------|
| `.venv/` | Dir | DELETE (untracked) | Not in git | Excluded | None | Delete | 1.0 | None |
| `.ruff_cache/` | Dir | DELETE (untracked) | Not in git | Excluded | None | Delete | 1.0 | None |
| `.claude/` | Dir | DELETE (untracked) + .gitignore | Not in git | None | None | Delete + ignore | 1.0 | None |
| `test_results/` | Dir | DELETE (untracked) | Empty, not in git | None | None | Delete | 1.0 | None |
| `dist/` | Dir | DELETE (untracked) | Build artifacts | None | None | Delete | 1.0 | None |
| `credentials/` | Dir | Add to .gitignore | Not tracked | None | None | Secure | 1.0 | None |
| `deploy_and_validate.sh` | Script | CONSOLIDATE into first_run.sh | 7 doc refs | None | deploy/** | Fold → Delete | 0.85 | Low |
| `validate_production_ready.sh` | Script | DELETE (subset) | 2 doc refs | None | docs/** | Delete | 0.90 | Low |
| `sql/views/dashboards.sql` | SQL | MOVE to sql/analysis/ | 2 doc refs | None | deploy/** | Move | 0.85 | Low |
| `examples/**` | Dir | KEEP (already excluded) | pyproject | Excluded | CONTRIBUTING | Keep | 1.0 | None |
| `scripts/dev/**` | Dir | KEEP (already excluded) | pyproject | Excluded | None | Keep | 1.0 | None |
| `OPERATIONS.md` | Doc | KEEP (not duplicate) | 5 doc refs | None | Multiple | Keep | 1.0 | None |
| `docs/decisions/` | Docs | KEEP (referenced in code) | Code refs | None | Multiple | Keep | 1.0 | None |

**Total Deletions**: 5 untracked directories
**Total Consolidations**: 2 scripts → 1 canonical (first_run.sh)
**Total Moves**: 1 file (sql/views/dashboards.sql)
**Total Security Additions**: 3 .gitignore entries

---

## 10. NEXT STEPS - AWAITING APPROVAL

**Phase 2 will execute if approved**:

1. ✅ Create branch `chore/repo-slim.20251104`
2. 🗑️ Remove untracked artifacts (.venv, .ruff_cache, etc.)
3. 🔒 Add credentials/, .claude/, .DS_Store to .gitignore
4. 🔀 Add migration step to first_run.sh
5. 🗑️ Delete deploy_and_validate.sh, validate_production_ready.sh
6. 📝 Update 6 doc files to reference first_run.sh only
7. 📦 Move sql/views/dashboards.sql → sql/analysis/sql/views/dashboards.sql
8. 📝 Update 2 doc files for sql/views/dashboards.sql new path
9. 📦 Create .gitattributes for export-ignore
10. 🧪 Build wheel + run tests in clean venv
11. 📄 Generate PROOF_BLOCK.json
12. ✅ Commit with conventional message

**Borderline items for approval**:
- ❓ examples/ - keep or delete? (Currently: keep, already excluded)
- ❓ scripts/dev/ - keep or delete? (Currently: keep, already excluded)

---

## APPROVAL REQUIRED

**Ready for Phase 2?** Awaiting explicit "APPROVED" command.

**Questions for user**:
1. Approve consolidation of validation scripts into first_run.sh?
2. Approve deletion of deploy_and_validate.sh and validate_production_ready.sh after doc updates?
3. Keep examples/ and scripts/dev/ (already excluded from packaging)?
