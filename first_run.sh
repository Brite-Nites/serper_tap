#!/usr/bin/env bash
# ============================================================================
# First Run / Canary Validation Script
# ============================================================================
# LEARN: Why a canary test?
# - Validates end-to-end pipeline before production load
# - Measures real-world performance (QPM, latency, cost)
# - Verifies idempotency (critical for BigQuery MERGE correctness)
# - Provides proof block for stakeholder sign-off
# ============================================================================

set -euo pipefail

# Load shared validation helpers
source "$(dirname "$0")/scripts/lib/validation_helpers.sh"

# ----------------------------------------------------------------------------
# CONFIGURATION
# ----------------------------------------------------------------------------
REPO_DIR="/opt/serper_tap"
cd "$REPO_DIR" || {
    echo "❌ Cannot find repo directory: $REPO_DIR"
    exit 1
}

# Load environment
if [ ! -f ".env.production" ]; then
    echo "❌ .env.production not found"
    exit 1
fi

export $(grep -v '^#' .env.production | grep -v '^$' | xargs)

# Canary job parameters (small state for quick validation)
CANARY_KEYWORD="bars"
CANARY_STATE="AZ"
CANARY_PAGES="3"
CANARY_BATCH_SIZE="150"
CANARY_CONCURRENCY="100"

# Thresholds
QPM_TARGET=300
QPM_WARN=250
JSON_SUCCESS_TARGET=99.5
JSON_SUCCESS_WARN=99.0

echo "============================================================================"
echo "FIRST RUN / CANARY VALIDATION"
echo "============================================================================"
echo "Project: $BIGQUERY_PROJECT_ID"
echo "Dataset: $BIGQUERY_DATASET"
echo "Canary:  $CANARY_KEYWORD × $CANARY_STATE × $CANARY_PAGES pages"
echo ""

# Results tracking
declare -A RESULTS
EXIT_CODE=0

# Override fail() to also set EXIT_CODE
fail() {
    echo -e "${RED}❌ FAIL${NC}: $1"
    RESULTS["$1"]="FAIL"
    EXIT_CODE=1
}

# ----------------------------------------------------------------------------
# Optional: Run BigQuery Migrations
# ----------------------------------------------------------------------------
if [ "${RUN_MIGRATIONS:-false}" = "true" ]; then
    echo "============================================================================"
    echo "APPLYING BIGQUERY MIGRATIONS (RUN_MIGRATIONS=true)"
    echo "============================================================================"
    if [ -d "sql/migrations" ]; then
        for migration in sql/migrations/*.sql; do
            [ -f "$migration" ] || continue
            echo "Applying: $(basename "$migration")"
            bq query --project_id="${BIGQUERY_PROJECT_ID}" < "$migration" || {
                echo "❌ Migration failed: $(basename "$migration")"
                exit 1
            }
        done
        echo -e "${GREEN}✅ All migrations applied${NC}"
    else
        echo "No sql/migrations directory found; skipping."
    fi
    echo ""
fi

# ----------------------------------------------------------------------------
# Step 0: Tooling Validation
# ----------------------------------------------------------------------------
echo "Step 0: Validating tooling..."

PYTHON_VERSION=$(poetry run python --version 2>&1 | awk '{print $2}')
POETRY_VERSION=$(poetry --version 2>&1 | awk '{print $3}')
BQ_VERSION=$(bq version 2>&1 | head -1 | awk '{print $2}')
PREFECT_VERSION=$(poetry run prefect version 2>&1)

echo "  Python:  $PYTHON_VERSION"
echo "  Poetry:  $POETRY_VERSION"
echo "  bq:      $BQ_VERSION"
echo "  Prefect: $PREFECT_VERSION"

if [[ "$PYTHON_VERSION" == 3.11.* ]]; then
    pass "Python 3.11.x"
elif [[ "$PYTHON_VERSION" == 3.13.* ]]; then
    warn "Python 3.13 detected (recommend 3.11 for production)"
else
    fail "Python version: $PYTHON_VERSION (require 3.11.x)"
fi

# ----------------------------------------------------------------------------
# Step 1: Re-run Migrations (Idempotent)
# ----------------------------------------------------------------------------
echo ""
echo "Step 1: Verifying migrations..."

# Check payload_raw column
PAYLOAD_RAW_EXISTS=$(bq query --use_legacy_sql=false --format=csv \
    "SELECT column_name
     FROM \`$BIGQUERY_PROJECT_ID.$BIGQUERY_DATASET.INFORMATION_SCHEMA.COLUMNS\`
     WHERE table_name='serper_places' AND column_name='payload_raw'" 2>&1 | tail -1)

if [ "$PAYLOAD_RAW_EXISTS" == "payload_raw" ]; then
    pass "Migration 002: payload_raw column exists"
else
    fail "Migration 002: payload_raw column MISSING"
fi

# ----------------------------------------------------------------------------
# Step 2: Verify Prefect Deployment
# ----------------------------------------------------------------------------
echo ""
echo "Step 2: Verifying Prefect deployment..."

if poetry run prefect deployment ls 2>&1 | grep -q "$PREFECT_DEPLOYMENT_NAME"; then
    pass "Prefect deployment registered: $PREFECT_DEPLOYMENT_NAME"
    DEPLOYMENT_PRESENT="yes"
else
    fail "Prefect deployment NOT found: $PREFECT_DEPLOYMENT_NAME"
    DEPLOYMENT_PRESENT="no"
fi

# ----------------------------------------------------------------------------
# Step 3: Check Worker Status
# ----------------------------------------------------------------------------
echo ""
echo "Step 3: Checking worker status..."

if sudo systemctl is-active --quiet prefect-worker; then
    pass "Worker systemd service: ONLINE"
    WORKER_STATUS="ONLINE"
else
    warn "Worker systemd service: OFFLINE"
    echo "  Start with: sudo systemctl start prefect-worker"
    WORKER_STATUS="OFFLINE"
fi

# ----------------------------------------------------------------------------
# Step 4: Health Check
# ----------------------------------------------------------------------------
echo ""
echo "Step 4: Running health check..."

if poetry run serper-health-check --json > /tmp/health_check.json 2>&1; then
    HEALTH_STATUS=$(jq -r '.status' /tmp/health_check.json 2>/dev/null || echo "unknown")
    if [ "$HEALTH_STATUS" == "healthy" ]; then
        pass "System health check: $HEALTH_STATUS"
    else
        warn "System health check: $HEALTH_STATUS"
        echo "  Details: $(cat /tmp/health_check.json)"
    fi
else
    warn "Health check returned warnings (see /tmp/health_check.json)"
fi

# ----------------------------------------------------------------------------
# Step 5: Create Canary Job
# ----------------------------------------------------------------------------
echo ""
echo "Step 5: Creating canary job..."

CANARY_OUTPUT=$(poetry run serper-create-job \
    --keyword "$CANARY_KEYWORD" \
    --state "$CANARY_STATE" \
    --pages "$CANARY_PAGES" \
    --batch-size "$CANARY_BATCH_SIZE" \
    --concurrency "$CANARY_CONCURRENCY" 2>&1)

if echo "$CANARY_OUTPUT" | grep -q "Job ID:"; then
    CANARY_JOB=$(echo "$CANARY_OUTPUT" | grep "Job ID:" | awk '{print $3}')
    pass "Canary job created: $CANARY_JOB"
else
    fail "Failed to create canary job"
    echo "$CANARY_OUTPUT"
    echo ""
    echo "===== BEGIN_SERPER_TAP_PROOF ====="
    echo '{"status": "FAILED", "error": "canary_creation_failed"}'
    echo "===== END_SERPER_TAP_PROOF ====="
    exit 1
fi

# ----------------------------------------------------------------------------
# Step 6: Process Canary (Get BEFORE count for idempotency)
# ----------------------------------------------------------------------------
echo ""
echo "Step 6: Processing canary job..."

# Get BEFORE count
BEFORE_COUNT=$(bq query --use_legacy_sql=false --format=csv \
    "SELECT COUNT(*) FROM \`$BIGQUERY_PROJECT_ID.$BIGQUERY_DATASET.serper_places\`
     WHERE job_id='$CANARY_JOB'" 2>&1 | tail -1)

echo "  Places before processing: $BEFORE_COUNT"

# Process the job
poetry run serper-process-batches > /tmp/canary_process.log 2>&1 || true

# Wait a moment for stats to update
sleep 5

# Get job stats
CANARY_STATS=$(bq query --use_legacy_sql=false --format=csv \
    "SELECT totals.queries, totals.successes, totals.failures, totals.places, totals.credits
     FROM \`$BIGQUERY_PROJECT_ID.$BIGQUERY_DATASET.serper_jobs\`
     WHERE job_id='$CANARY_JOB'" 2>&1 | tail -1)

TOTAL_QUERIES=$(echo "$CANARY_STATS" | cut -d',' -f1)
COMPLETED=$(echo "$CANARY_STATS" | cut -d',' -f2)
FAILED=$(echo "$CANARY_STATS" | cut -d',' -f3)
PLACES=$(echo "$CANARY_STATS" | cut -d',' -f4)
CREDITS=$(echo "$CANARY_STATS" | cut -d',' -f5)

echo "  Total queries:  $TOTAL_QUERIES"
echo "  Completed:      $COMPLETED"
echo "  Failed:         $FAILED"
echo "  Places found:   $PLACES"
echo "  Credits used:   $CREDITS"

if [ "$COMPLETED" -eq "$TOTAL_QUERIES" ]; then
    pass "Canary completed: $COMPLETED/$TOTAL_QUERIES queries"
else
    warn "Canary incomplete: $COMPLETED/$TOTAL_QUERIES queries ($FAILED failed)"
fi

# ----------------------------------------------------------------------------
# Step 7: Measure QPM (10-minute window)
# ----------------------------------------------------------------------------
echo ""
echo "Step 7: Measuring performance (QPM)..."

QPM_VALUE=$(check_qpm 10 "$QPM_TARGET" "$QPM_WARN")

# ----------------------------------------------------------------------------
# Step 8: Check JSON Parse Health
# ----------------------------------------------------------------------------
echo ""
echo "Step 8: Checking JSON parse health (24h)..."

PARSE_PCT=$(check_json_health 24 "$JSON_SUCCESS_TARGET" "$JSON_SUCCESS_WARN")

# ----------------------------------------------------------------------------
# Step 9: Check Cost Today
# ----------------------------------------------------------------------------
echo ""
echo "Step 9: Checking cost today..."

COST_TODAY=$(bq query --use_legacy_sql=false --format=csv \
    "SELECT ROUND(SUM(CAST(totals.credits AS INT64)) * $COST_PER_CREDIT, 2) as cost_usd
     FROM \`$BIGQUERY_PROJECT_ID.$BIGQUERY_DATASET.serper_jobs\`
     WHERE DATE(created_at) = CURRENT_DATE()" 2>&1 | tail -1)

if [ -n "$COST_TODAY" ] && [ "$COST_TODAY" != "null" ]; then
    echo "  Cost today: \$${COST_TODAY}"

    if (( $(echo "$COST_TODAY <= $DAILY_BUDGET_USD" | bc -l) )); then
        pass "Cost within budget: \$${COST_TODAY} / \$${DAILY_BUDGET_USD}"
    else
        fail "Cost EXCEEDED budget: \$${COST_TODAY} / \$${DAILY_BUDGET_USD}"
    fi
else
    COST_TODAY="0.00"
    echo "  Cost today: \$0.00"
fi

# ----------------------------------------------------------------------------
# Step 10: Idempotency Check
# ----------------------------------------------------------------------------
echo ""
echo "Step 10: Verifying idempotency..."

# Re-run processor (should be no-op)
poetry run serper-process-batches > /dev/null 2>&1 || true

# Get AFTER count
AFTER_COUNT=$(bq query --use_legacy_sql=false --format=csv \
    "SELECT COUNT(*) FROM \`$BIGQUERY_PROJECT_ID.$BIGQUERY_DATASET.serper_places\`
     WHERE job_id='$CANARY_JOB'" 2>&1 | tail -1)

echo "  Places before: $BEFORE_COUNT"
echo "  Places after:  $AFTER_COUNT"

if [ "$BEFORE_COUNT" -eq "$AFTER_COUNT" ]; then
    pass "Idempotency verified: $BEFORE_COUNT = $AFTER_COUNT places"
    IDEMPOTENT="true"
else
    fail "Idempotency FAILED: $BEFORE_COUNT → $AFTER_COUNT places"
    IDEMPOTENT="false"
fi

# ----------------------------------------------------------------------------
# Generate Proof Block
# ----------------------------------------------------------------------------
echo ""
echo "============================================================================"
echo "GENERATING PROOF BLOCK"
echo "============================================================================"
echo ""

# Get VM metadata
VM_NAME=$(hostname)
ZONE=$(curl -s -H "Metadata-Flavor: Google" http://metadata.google.internal/computeMetadata/v1/instance/zone 2>/dev/null | awk -F/ '{print $NF}' || echo "unknown")
TIMESTAMP=$(date -u +"%Y-%m-%dT%H:%M:%SZ")

# Build proof JSON
PROOF_JSON=$(cat <<EOF
{
  "vm_name": "$VM_NAME",
  "zone": "$ZONE",
  "python": "$PYTHON_VERSION",
  "poetry": "$POETRY_VERSION",
  "prefect_deployment": "$DEPLOYMENT_PRESENT",
  "worker_status": "$WORKER_STATUS",
  "canary_job_id": "$CANARY_JOB",
  "qpm_10m": $QPM_VALUE,
  "json_success_pct_24h": $PARSE_PCT,
  "cost_usd_today": $COST_TODAY,
  "idempotent": $IDEMPOTENT,
  "timestamp_utc": "$TIMESTAMP"
}
EOF
)

# Save to file
echo "$PROOF_JSON" > /opt/serper_tap/deployment_proof.json

# Print proof block
echo "===== BEGIN_SERPER_TAP_PROOF ====="
echo "$PROOF_JSON"
echo "===== END_SERPER_TAP_PROOF ====="

# ----------------------------------------------------------------------------
# Summary
# ----------------------------------------------------------------------------
echo ""
echo "============================================================================"
echo "VALIDATION SUMMARY"
echo "============================================================================"
echo ""

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

echo "Results: $PASS_COUNT PASS, $WARN_COUNT WARN, $FAIL_COUNT FAIL"
echo ""

if [ "$FAIL_COUNT" -eq 0 ]; then
    echo -e "${GREEN}✅ GO FOR PRODUCTION${NC}"
    echo ""
    echo "Proof block saved to: /opt/serper_tap/deployment_proof.json"
    echo ""
    echo "To retrieve proof later:"
    echo "  cat /opt/serper_tap/deployment_proof.json"
    echo ""
else
    echo -e "${RED}❌ NO-GO: $FAIL_COUNT critical failures${NC}"
    echo ""
    echo "Review failures above and fix before production use."
    echo ""
fi

exit $EXIT_CODE
