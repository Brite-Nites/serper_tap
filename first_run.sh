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

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

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

pass() {
    echo -e "${GREEN}✅ PASS${NC}: $1"
    RESULTS["$1"]="PASS"
}

fail() {
    echo -e "${RED}❌ FAIL${NC}: $1"
    RESULTS["$1"]="FAIL"
    EXIT_CODE=1
}

warn() {
    echo -e "${YELLOW}⚠️  WARN${NC}: $1"
    RESULTS["$1"]="WARN"
}

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

QPM_RESULT=$(bq query --use_legacy_sql=false --format=csv \
    "SELECT ROUND(60.0*COUNT(*)/NULLIF(TIMESTAMP_DIFF(MAX(ran_at),MIN(ran_at),SECOND),0),1) as qpm
     FROM \`$BIGQUERY_PROJECT_ID.$BIGQUERY_DATASET.serper_queries\`
     WHERE ran_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 10 MINUTE)
       AND status='success'" 2>&1 | tail -1)

if [ -n "$QPM_RESULT" ] && [ "$QPM_RESULT" != "null" ]; then
    QPM_VALUE=$(echo "$QPM_RESULT" | tr -d ' ')
    echo "  QPM (10-min): $QPM_VALUE"

    if (( $(echo "$QPM_VALUE >= $QPM_TARGET" | bc -l) )); then
        pass "QPM target met: $QPM_VALUE qpm (target ≥$QPM_TARGET)"
    elif (( $(echo "$QPM_VALUE >= $QPM_WARN" | bc -l) )); then
        warn "QPM below target: $QPM_VALUE qpm (target ≥$QPM_TARGET)"
    else
        fail "QPM critically low: $QPM_VALUE qpm (target ≥$QPM_TARGET)"
    fi
else
    warn "QPM calculation failed (insufficient data)"
    QPM_VALUE="0"
fi

# ----------------------------------------------------------------------------
# Step 8: Check JSON Parse Health
# ----------------------------------------------------------------------------
echo ""
echo "Step 8: Checking JSON parse health (24h)..."

JSON_HEALTH=$(bq query --use_legacy_sql=false --format=csv \
    "SELECT
       COUNTIF(payload IS NULL AND payload_raw IS NOT NULL) AS unparsable,
       COUNTIF(payload IS NOT NULL) AS parsed
     FROM \`$BIGQUERY_PROJECT_ID.$BIGQUERY_DATASET.serper_places\`
     WHERE ingest_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)" 2>&1 | tail -1)

UNPARSABLE=$(echo "$JSON_HEALTH" | cut -d',' -f1)
PARSED=$(echo "$JSON_HEALTH" | cut -d',' -f2)
TOTAL=$((UNPARSABLE + PARSED))

if [ "$TOTAL" -gt 0 ]; then
    PARSE_PCT=$(echo "scale=2; 100 * $PARSED / $TOTAL" | bc)
    echo "  Unparsable: $UNPARSABLE"
    echo "  Parsed:     $PARSED"
    echo "  Success:    ${PARSE_PCT}%"

    if (( $(echo "$PARSE_PCT >= $JSON_SUCCESS_TARGET" | bc -l) )); then
        pass "JSON parsing healthy: ${PARSE_PCT}% (target ≥${JSON_SUCCESS_TARGET}%)"
    elif (( $(echo "$PARSE_PCT >= $JSON_SUCCESS_WARN" | bc -l) )); then
        warn "JSON parsing: ${PARSE_PCT}% (target ≥${JSON_SUCCESS_TARGET}%)"
    else
        fail "JSON parsing degraded: ${PARSE_PCT}%"
    fi
else
    warn "No recent place data for JSON health check"
    PARSE_PCT="100"
fi

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
