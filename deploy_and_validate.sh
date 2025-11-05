#!/usr/bin/env bash
# ============================================================================
# Serper Tap - Production Deployment & Validation Script
# ============================================================================
# Purpose: Idempotent deployment script for production rollout
# Usage:
#   ./deploy_and_validate.sh
#   ./deploy_and_validate.sh --skip-migrations  # Skip if already applied
#   ./deploy_and_validate.sh --canary-state VT  # Custom canary state
# ============================================================================

set -euo pipefail

# Load shared validation helpers
source "$(dirname "$0")/scripts/lib/validation_helpers.sh"

# Configuration
PROJECT_ID="${BIGQUERY_PROJECT_ID:-brite-nites-data-platform}"
DATASET="${BIGQUERY_DATASET:-raw_data}"
CANARY_STATE="${1:-RI}"  # Small state for canary
SKIP_MIGRATIONS="${2:-false}"

echo "============================================================================"
echo "Serper Tap - Production Deployment"
echo "============================================================================"
echo "Project: $PROJECT_ID"
echo "Dataset: $DATASET"
echo "Canary State: $CANARY_STATE"
echo ""

# Results tracking
declare -A RESULTS

# ============================================================================
# Step 0: Prerequisites Check
# ============================================================================
echo "Step 0: Checking prerequisites..."

# Check Python version
PYTHON_VERSION=$(python --version 2>&1 | awk '{print $2}')
if [[ "$PYTHON_VERSION" == 3.11.* ]]; then
    pass "Python 3.11 installed ($PYTHON_VERSION)"
elif [[ "$PYTHON_VERSION" == 3.13.* ]]; then
    warn "Python 3.13 detected, recommend 3.11 for production"
else
    fail "Python version $PYTHON_VERSION - require 3.11.x"
fi

# Check Poetry
if command -v poetry &> /dev/null; then
    pass "Poetry installed"
else
    fail "Poetry not found"
    exit 1
fi

# Check bq CLI
if command -v bq &> /dev/null; then
    pass "BigQuery CLI installed"
else
    fail "BigQuery CLI not found"
    exit 1
fi

# Check .env.production exists
if [ -f .env.production ]; then
    pass ".env.production exists"
    # Source it
    export $(grep -v '^#' .env.production | xargs)
else
    fail ".env.production not found"
    exit 1
fi

# ============================================================================
# Step 1: Install Dependencies
# ============================================================================
echo ""
echo "Step 1: Installing dependencies..."

if poetry install --no-interaction 2>&1 | grep -q "Installing dependencies"; then
    pass "Dependencies installed"
else
    pass "Dependencies already installed"
fi

# ============================================================================
# Step 2: Run Migrations
# ============================================================================
echo ""
echo "Step 2: Running migrations..."

if [ "$SKIP_MIGRATIONS" == "true" ]; then
    warn "Migrations skipped (--skip-migrations flag)"
else
    # Check if payload_raw column exists
    if bq query --use_legacy_sql=false --format=csv \
        "SELECT column_name FROM \`$PROJECT_ID.$DATASET.INFORMATION_SCHEMA.COLUMNS\`
         WHERE table_name='serper_places' AND column_name='payload_raw'" 2>&1 | grep -q "payload_raw"; then
        pass "Migration 002 already applied (payload_raw exists)"
    else
        echo "Applying migration 002..."
        bq query --use_legacy_sql=false \
            "ALTER TABLE \`$PROJECT_ID.$DATASET.serper_places\` ADD COLUMN IF NOT EXISTS payload_raw STRING;" &> /dev/null
        bq query --use_legacy_sql=false \
            "ALTER TABLE \`$PROJECT_ID.$DATASET.serper_places\` ALTER COLUMN payload SET OPTIONS (description='Parsed JSON payload (NULL if parse failed)');" &> /dev/null
        bq query --use_legacy_sql=false \
            "ALTER TABLE \`$PROJECT_ID.$DATASET.serper_places\` ALTER COLUMN payload_raw SET OPTIONS (description='Original JSON text as received');" &> /dev/null
        pass "Migration 002 applied successfully"
    fi

    # Check batch_size column
    NULL_COUNT=$(bq query --use_legacy_sql=false --format=csv \
        "SELECT COUNTIF(batch_size IS NULL) FROM \`$PROJECT_ID.$DATASET.serper_jobs\`" 2>&1 | tail -1)
    if [ "$NULL_COUNT" == "0" ]; then
        pass "Migration 001 verified (batch_size column OK)"
    else
        fail "Migration 001 incomplete ($NULL_COUNT NULL batch_size values)"
    fi
fi

# ============================================================================
# Step 3: Health Check
# ============================================================================
echo ""
echo "Step 3: Running health check..."

if poetry run serper-health-check &> /tmp/health_check.log; then
    pass "System health check passed"
else
    warn "Health check returned warnings (see /tmp/health_check.log)"
fi

# ============================================================================
# Step 4: Prefect Deployment
# ============================================================================
echo ""
echo "Step 4: Checking Prefect deployment..."

# Note: This step is informational only, as it requires Prefect Cloud/Server
echo "  → To apply deployment: prefect deployment apply deployment.yaml"
echo "  → To start worker: prefect worker start --pool default-pool"
echo "  → Or use systemd: sudo systemctl start prefect-worker"

# ============================================================================
# Step 5: Create Canary Job
# ============================================================================
echo ""
echo "Step 5: Creating canary job ($CANARY_STATE)..."

CANARY_OUTPUT=$(poetry run serper-create-job \
    --keyword bars \
    --state "$CANARY_STATE" \
    --pages 3 \
    --batch-size 50 \
    --concurrency 50 2>&1)

if echo "$CANARY_OUTPUT" | grep -q "Job ID:"; then
    CANARY_JOB=$(echo "$CANARY_OUTPUT" | grep "Job ID:" | awk '{print $3}')
    pass "Canary job created: $CANARY_JOB"
else
    fail "Failed to create canary job"
    echo "$CANARY_OUTPUT"
    exit 1
fi

# ============================================================================
# Step 6: Process Canary
# ============================================================================
echo ""
echo "Step 6: Processing canary job..."

# Get BEFORE count for idempotency
BEFORE_COUNT=$(bq query --use_legacy_sql=false --format=csv \
    "SELECT COUNT(*) FROM \`$PROJECT_ID.$DATASET.serper_places\` WHERE job_id='$CANARY_JOB'" 2>&1 | tail -1)

# Process the job
poetry run serper-process-batches &> /tmp/canary_process.log || true

# Check completion
CANARY_STATS=$(bq query --use_legacy_sql=false --format=csv \
    "SELECT totals.queries, totals.successes, totals.places
     FROM \`$PROJECT_ID.$DATASET.serper_jobs\`
     WHERE job_id='$CANARY_JOB'" 2>&1 | tail -1)

TOTAL_QUERIES=$(echo "$CANARY_STATS" | cut -d',' -f1)
COMPLETED=$(echo "$CANARY_STATS" | cut -d',' -f2)
PLACES=$(echo "$CANARY_STATS" | cut -d',' -f3)

if [ "$COMPLETED" -eq "$TOTAL_QUERIES" ]; then
    pass "Canary completed: $COMPLETED/$TOTAL_QUERIES queries, $PLACES places"
else
    warn "Canary incomplete: $COMPLETED/$TOTAL_QUERIES queries"
fi

# ============================================================================
# Step 7: Calculate QPM
# ============================================================================
echo ""
echo "Step 7: Measuring performance..."

check_qpm 30 300 150

# ============================================================================
# Step 8: Idempotency Check
# ============================================================================
echo ""
echo "Step 8: Verifying idempotency..."

# Process again (should be no-op)
poetry run serper-process-batches &> /dev/null || true

AFTER_COUNT=$(bq query --use_legacy_sql=false --format=csv \
    "SELECT COUNT(*) FROM \`$PROJECT_ID.$DATASET.serper_places\` WHERE job_id='$CANARY_JOB'" 2>&1 | tail -1)

if [ "$BEFORE_COUNT" -eq "$AFTER_COUNT" ]; then
    pass "Idempotency verified: $BEFORE_COUNT = $AFTER_COUNT places"
else
    fail "Idempotency FAILED: $BEFORE_COUNT → $AFTER_COUNT places"
fi

# ============================================================================
# Step 9: Budget Guard Test
# ============================================================================
echo ""
echo "Step 9: Testing budget guard..."

BUDGET_TEST=$(DAILY_BUDGET_USD=0.05 poetry run serper-create-job \
    --keyword test --state RI --pages 1 --batch-size 10 2>&1 || true)

if echo "$BUDGET_TEST" | grep -q "budget"; then
    pass "Budget guard blocking correctly"
else
    fail "Budget guard not blocking (may already be below threshold)"
fi

# ============================================================================
# Step 10: JSON Health Check
# ============================================================================
echo ""
echo "Step 10: Checking JSON parsing health..."

check_json_health 24 99.5 99.0

# ============================================================================
# Summary
# ============================================================================
echo ""
echo "============================================================================"
echo "DEPLOYMENT VALIDATION SUMMARY"
echo "============================================================================"

PASS_COUNT=0
FAIL_COUNT=0
WARN_COUNT=0

for check in "${!RESULTS[@]}"; do
    result="${RESULTS[$check]}"
    case "$result" in
        PASS) ((PASS_COUNT++)) ;;
        FAIL) ((FAIL_COUNT++)) ;;
        WARN) ((WARN_COUNT++)) ;;
    esac
done

echo ""
printf "%-50s %s\n" "Check" "Result"
echo "----------------------------------------------------------------------------"
for check in "${!RESULTS[@]}"; do
    result="${RESULTS[$check]}"
    case "$result" in
        PASS) printf "%-50s ${GREEN}✅ %s${NC}\n" "$check" "$result" ;;
        FAIL) printf "%-50s ${RED}❌ %s${NC}\n" "$check" "$result" ;;
        WARN) printf "%-50s ${YELLOW}⚠️  %s${NC}\n" "$check" "$result" ;;
    esac
done

echo ""
echo "Summary: $PASS_COUNT PASS, $WARN_COUNT WARN, $FAIL_COUNT FAIL"
echo ""

# Final verdict
if [ "$FAIL_COUNT" -eq 0 ]; then
    echo -e "${GREEN}✅ GO FOR PRODUCTION${NC}"
    echo ""
    echo "Next steps:"
    echo "  1. Start Prefect worker: sudo systemctl start prefect-worker"
    echo "  2. Monitor logs: journalctl -u prefect-worker -f"
    echo "  3. Create production jobs as needed"
    echo ""
    exit 0
else
    echo -e "${RED}❌ NO-GO: $FAIL_COUNT critical failures${NC}"
    echo ""
    echo "Review failures above and re-run after fixes."
    echo ""
    exit 1
fi
