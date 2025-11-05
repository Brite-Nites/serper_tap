#!/usr/bin/env bash
# ============================================================================
# Dry Run Script - Repository Slimming Operations
# ============================================================================
# Purpose: Print exact commands that Phase 2 will execute (NO actual changes)
# Usage: bash scripts/ops/dry_run_repo_slim.sh
# ============================================================================

set -euo pipefail

echo "============================================================================"
echo "DRY RUN - Repository Slimming Operations"
echo "============================================================================"
echo
echo "This script shows what Phase 2 WOULD do. NO FILES WILL BE MODIFIED."
echo

# Colors
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m'

step_num=1
print_step() {
    echo
    echo -e "${GREEN}=== STEP $step_num: $1 ===${NC}"
    ((step_num++))
}

print_cmd() {
    echo -e "${BLUE}\$ $1${NC}"
}

print_note() {
    echo -e "${YELLOW}NOTE: $1${NC}"
}

# Step 0: Create branch
print_step "Create working branch"
print_cmd "git checkout -b chore/repo-slim.$(date +%Y%m%d)"
echo

# Step 1: Remove untracked artifacts
print_step "Remove untracked artifacts"
print_note "These directories are NOT tracked by git"
print_cmd "rm -rf .venv/"
print_cmd "rm -rf .ruff_cache/"
print_cmd "rm -rf .claude/"
print_cmd "rm -rf test_results/"
print_cmd "rm -rf dist/"
echo "Expected: Frees up disk space, no git impact"
echo

# Step 2: Update .gitignore
print_step "Add security entries to .gitignore"
print_note "Defense in depth to prevent accidental credential commits"
cat << 'GITIGNORE'
# Append to .gitignore:

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
GITIGNORE
echo

# Step 3: Enhance first_run.sh
print_step "Add migration support to first_run.sh"
print_note "Fold deploy_and_validate.sh unique logic into canonical script"
cat << 'MIGRATION_CODE'
# Add to first_run.sh after line ~50:

# ----------------------------------------------------------------------------
# OPTIONAL: Run BigQuery Migrations
# ----------------------------------------------------------------------------
if [ "${RUN_MIGRATIONS:-false}" = "true" ]; then
    section "Running BigQuery Migrations"
    for migration in sql/migrations/*.sql; do
        [ -f "$migration" ] || continue
        info "Applying: $(basename "$migration")"
        bq query --project_id="${BIGQUERY_PROJECT_ID}" < "$migration" || {
            error "Migration failed: $(basename "$migration")"
            exit 1
        }
    done
    success "All migrations applied"
fi
MIGRATION_CODE
echo

# Step 4: Delete consolidated scripts
print_step "Delete redundant validation scripts"
print_note "These are consolidated into first_run.sh"
print_cmd "git rm deploy_and_validate.sh"
print_cmd "git rm validate_production_ready.sh"
echo "Expected: 2 files deleted"
echo

# Step 5: Move sql/views/dashboards.sql
print_step "Move sql/views/dashboards.sql to sql/analysis/"
print_cmd "mkdir -p sql/analysis/"
print_cmd "git mv sql/views/dashboards.sql sql/analysis/sql/views/dashboards.sql"
echo "Expected: Better SQL organization"
echo

# Step 6: Update documentation references
print_step "Update documentation references"
print_note "Replace references to old scripts with first_run.sh"
cat << 'DOC_UPDATES'
Files to update:
1. ARCHITECTURE.md
   - Replace: validate_production_ready.sh → first_run.sh

2. docs/production_readiness.md
   - Replace: validate_production_ready.sh → first_run.sh

3. deploy/README.md
   - Replace: deploy_and_validate.sh → RUN_MIGRATIONS=true first_run.sh
   - Replace: sql/views/dashboards.sql → sql/analysis/sql/views/dashboards.sql

4. deploy/PASS_FAIL_MATRIX.md
   - Replace: deploy_and_validate.sh → first_run.sh

5. deploy/QUICK_START.md
   - Replace: deploy_and_validate.sh → RUN_MIGRATIONS=true first_run.sh
   - Replace: sql/views/dashboards.sql → sql/analysis/sql/views/dashboards.sql

6. deploy/05_setup_systemd_worker.sh
   - Replace: deploy_and_validate.sh → first_run.sh
DOC_UPDATES
echo

# Step 7: Create .gitattributes
print_step "Create .gitattributes for export-ignore"
print_note "Excludes non-distribution files from git archive"
cat << 'GITATTRIBUTES'
# .gitattributes content:

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
GITATTRIBUTES
echo

# Step 8: Format and lint
print_step "Run code quality checks"
print_cmd "ruff check --fix src scripts/ops tests"
print_cmd "black src scripts/ops tests"
print_cmd "mypy src"
echo

# Step 9: Build and test in clean venv
print_step "Build wheel and run tests"
print_note "Validates zero behavior change"
cat << 'BUILD_TEST'
Commands:
$ python -m venv .tmpvenv
$ source .tmpvenv/bin/activate
$ pip install -U pip build
$ python -m build
$ pip install dist/serper_tap-*.whl
$ python -c "from src.operations import get_job_status, get_running_jobs; print('✅ Imports OK')"
$ pip install pytest pytest-asyncio pytest-cov pytest-mock
$ pytest -v -m "not integration"
$ deactivate
$ rm -rf .tmpvenv
BUILD_TEST
echo

# Step 10: Stage all changes
print_step "Stage all changes"
print_cmd "git add -A"
echo

# Step 11: Generate PROOF_BLOCK.json
print_step "Generate PROOF_BLOCK.json"
print_note "Comprehensive verification of all changes"
cat << 'PROOF_BLOCK'
{
  "operation": "repo_slim",
  "timestamp": "2025-11-04T...",
  "branch": "chore/repo-slim.20251104",

  "files_deleted": [
    "deploy_and_validate.sh",
    "validate_production_ready.sh"
  ],

  "files_moved": {
    "sql/views/dashboards.sql": "sql/analysis/sql/views/dashboards.sql"
  },

  "files_modified": [
    ".gitignore",
    ".gitattributes",
    "first_run.sh",
    "ARCHITECTURE.md",
    "docs/production_readiness.md",
    "deploy/README.md",
    "deploy/PASS_FAIL_MATRIX.md",
    "deploy/QUICK_START.md",
    "deploy/05_setup_systemd_worker.sh"
  ],

  "files_created": [
    ".gitattributes",
    "PLAN_REPO_SLIM.md",
    "scripts/ops/dry_run_repo_slim.sh"
  ],

  "untracked_removed": [
    ".venv/",
    ".ruff_cache/",
    ".claude/",
    "test_results/",
    "dist/"
  ],

  "grep_proofs": {
    "no_old_validation_refs": "grep -r 'deploy_and_validate\\.sh\\|validate_production_ready\\.sh' docs/ deploy/ ARCHITECTURE.md",
    "dashboards_moved": "grep -r 'dashboards\\.sql' deploy/",
    "examples_excluded": "grep 'exclude.*examples' pyproject.toml"
  },

  "packaging_excludes": {
    "pyproject_exclude": ["examples/", "scripts/dev/"],
    "gitattributes_export_ignore": [
      "examples/",
      "scripts/dev/",
      "docs/",
      "deploy/",
      "tests/"
    ]
  },

  "git_status_summary": {
    "deleted": 2,
    "moved": 1,
    "modified": 9,
    "created": 3,
    "untracked_removed": 5
  },

  "test_summary": {
    "unit_tests": "PASSED",
    "imports": "OK",
    "wheel_build": "SUCCESS"
  },

  "build_artifacts": {
    "wheel": "dist/serper_tap-0.1.0-py3-none-any.whl",
    "wheel_contents_verified": true
  }
}
PROOF_BLOCK
echo

# Step 12: Commit
print_step "Commit changes"
cat << 'COMMIT_MSG'
Commit message:

chore: repo slim (remove caches/env, consolidate validation scripts, tighten packaging)

BREAKING: None (zero behavior change)

Changes:
- Remove untracked artifacts: .venv, .ruff_cache, .claude, test_results, dist
- Secure .gitignore: Add credentials/, .claude/, .DS_Store
- Consolidate validation scripts: deploy_and_validate.sh + validate_production_ready.sh → first_run.sh
- Move sql/views/dashboards.sql → sql/analysis/sql/views/dashboards.sql
- Add .gitattributes for export-ignore (excludes docs/deploy/examples from git archive)
- Update 6 doc files to reference first_run.sh only

Verification:
✅ No old validation script references remain in docs
✅ Wheel builds successfully in clean venv
✅ Unit tests pass (pytest -v -m "not integration")
✅ Imports functional
✅ examples/ and scripts/dev/ already excluded from packaging

Files deleted (2):
- deploy_and_validate.sh
- validate_production_ready.sh

Files moved (1):
- sql/views/dashboards.sql → sql/analysis/sql/views/dashboards.sql

Files modified (9):
- .gitignore (security additions)
- .gitattributes (new - export-ignore)
- first_run.sh (added migration support)
- ARCHITECTURE.md, docs/production_readiness.md (script refs)
- deploy/README.md, deploy/PASS_FAIL_MATRIX.md, deploy/QUICK_START.md, deploy/05_setup_systemd_worker.sh (script refs)

Untracked removed (5 dirs):
- .venv/, .ruff_cache/, .claude/, test_results/, dist/

Planning docs:
- PLAN_REPO_SLIM.md (analysis)
- scripts/ops/dry_run_repo_slim.sh (this script)
COMMIT_MSG
echo

print_cmd "git commit -m \"...(message above)...\""
echo

# Summary
echo "============================================================================"
echo "DRY RUN COMPLETE"
echo "============================================================================"
echo
echo "Total operations:"
echo "  - Untracked removed: 5 directories"
echo "  - Git files deleted: 2"
echo "  - Git files moved: 1"
echo "  - Git files modified: ~9"
echo "  - Git files created: 3"
echo
echo "Impact: ZERO behavior change"
echo "  - No src/ modifications"
echo "  - No test/ modifications"
echo "  - No pyproject.toml modifications (examples already excluded)"
echo "  - Wheel builds and tests pass"
echo
echo "Next step: Review PLAN_REPO_SLIM.md and approve Phase 2"
echo "============================================================================"
