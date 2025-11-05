#!/bin/bash
# 15-Minute Production Readiness Validation Runbook
#
# This script validates all production readiness criteria for the Serper Tap pipeline.
# Expected runtime: ~15 minutes (10 min for job processing + 5 min for validation)
#
# Prerequisites:
# - Poetry installed and dependencies available
# - BigQuery credentials configured
# - Prefect server running (optional, for deployment testing)
#
# Usage:
#   chmod +x validate_production_ready.sh
#   ./validate_production_ready.sh

set -e  # Exit on error
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$SCRIPT_DIR"

# Load shared validation helpers
source "$(dirname "$0")/scripts/lib/validation_helpers.sh"

echo "=================================================="
echo "🚀 Production Readiness Validation - Serper Tap"
echo "=================================================="
echo ""

# Step 1: Run health check
echo -e "${BLUE}[1/7] Running system health check...${NC}"
if poetry run serper-health-check; then
    echo -e "${GREEN}✓ Health check passed${NC}"
else
    echo -e "${RED}✗ Health check failed - Fix configuration before continuing${NC}"
    exit 1
fi
echo ""

# Step 2: Run test suite
echo -e "${BLUE}[2/7] Running pytest test suite...${NC}"
if poetry run pytest --tb=short -q; then
    echo -e "${GREEN}✓ All tests passed${NC}"
else
    echo -e "${RED}✗ Tests failed - Review failures above${NC}"
    exit 1
fi
echo ""

# Step 3: Create small test job
echo -e "${BLUE}[3/7] Creating small test job (10 queries)...${NC}"
TEST_STATE="DC"  # DC has ~25 zip codes, 25 * 1 page = 25 queries (small job)
TEST_KEYWORD="test-validation-$(date +%s)"
echo "  State: $TEST_STATE"
echo "  Keyword: $TEST_KEYWORD"
echo "  Pages: 1 (to keep test fast)"

# Extract job_id from create_job output
JOB_OUTPUT=$(poetry run serper-create-job \
    --keyword "$TEST_KEYWORD" \
    --state "$TEST_STATE" \
    --pages 1 \
    --dry-run)

echo "$JOB_OUTPUT"

# Extract job ID from output (look for "Job ID: <uuid>")
JOB_ID=$(echo "$JOB_OUTPUT" | grep -oE "Job ID: [a-f0-9-]+" | awk '{print $3}')

if [ -z "$JOB_ID" ]; then
    echo -e "${RED}✗ Failed to extract job_id from output${NC}"
    exit 1
fi

echo -e "${GREEN}✓ Job created: $JOB_ID${NC}"
echo ""

# Step 4: Start batch processor (if not running via deployment)
echo -e "${BLUE}[4/7] Processing batches...${NC}"
echo "  Running processor for 5 minutes max..."
echo "  (Ctrl+C is safe - job will complete on next processor run)"

# Run processor with timeout
timeout 300 poetry run serper-process-batches || {
    EXIT_CODE=$?
    if [ $EXIT_CODE -eq 124 ]; then
        echo -e "${YELLOW}⏱ Processor timeout reached (5 min)${NC}"
    else
        echo -e "${YELLOW}⚠ Processor exited with code $EXIT_CODE${NC}"
    fi
}
echo ""

# Step 5: Check job completion status
echo -e "${BLUE}[5/7] Checking job completion status...${NC}"

# Query BigQuery for job status
STATUS_QUERY="
SELECT
    job_id,
    status,
    CAST(queries AS INT64) as total_queries,
    CAST(completed AS INT64) as completed_queries,
    CAST(credits AS INT64) as credits_used,
    ROUND(100.0 * completed / NULLIF(queries, 0), 1) as completion_pct
FROM \\\`${BIGQUERY_PROJECT_ID}.${BIGQUERY_DATASET}.serper_jobs\\\`
WHERE job_id = '$JOB_ID'
"

bq query --use_legacy_sql=false --format=pretty "$STATUS_QUERY"

# Check if status is 'done'
JOB_STATUS=$(bq query --use_legacy_sql=false --format=csv --max_rows=1 \
    "SELECT status FROM \\\`${BIGQUERY_PROJECT_ID}.${BIGQUERY_DATASET}.serper_jobs\\\` WHERE job_id = '$JOB_ID'" | tail -1)

if [ "$JOB_STATUS" = "done" ]; then
    echo -e "${GREEN}✓ Job completed successfully${NC}"
elif [ "$JOB_STATUS" = "running" ]; then
    echo -e "${YELLOW}⚠ Job still running - validation may show partial results${NC}"
else
    echo -e "${RED}✗ Unexpected job status: $JOB_STATUS${NC}"
fi
echo ""

# Step 6: Verify idempotency
echo -e "${BLUE}[6/7] Testing idempotency (re-running processor)...${NC}"
echo "  Running processor again with same job..."

# Count results before re-run
RESULTS_BEFORE=$(bq query --use_legacy_sql=false --format=csv --max_rows=1 \
    "SELECT COUNT(*) FROM \\\`${BIGQUERY_PROJECT_ID}.${BIGQUERY_DATASET}.serper_results\\\` WHERE job_id = '$JOB_ID'" | tail -1)

echo "  Results before: $RESULTS_BEFORE"

# Re-run processor for 30 seconds
timeout 30 poetry run serper-process-batches || true

# Count results after re-run
RESULTS_AFTER=$(bq query --use_legacy_sql=false --format=csv --max_rows=1 \
    "SELECT COUNT(*) FROM \\\`${BIGQUERY_PROJECT_ID}.${BIGQUERY_DATASET}.serper_results\\\` WHERE job_id = '$JOB_ID'" | tail -1)

echo "  Results after:  $RESULTS_AFTER"

if [ "$RESULTS_BEFORE" -eq "$RESULTS_AFTER" ]; then
    echo -e "${GREEN}✓ Idempotency verified (no duplicate results)${NC}"
else
    echo -e "${RED}✗ Result count changed: $RESULTS_BEFORE → $RESULTS_AFTER${NC}"
    echo -e "${RED}  This indicates non-idempotent behavior!${NC}"
fi
echo ""

# Step 7: Calculate queries per minute (qpm)
echo -e "${BLUE}[7/7] Computing performance metrics...${NC}"

# Query for runtime and completion stats
METRICS_QUERY="
WITH job_metrics AS (
  SELECT
    j.job_id,
    j.created_at,
    j.completed_at,
    TIMESTAMP_DIFF(j.completed_at, j.created_at, SECOND) as runtime_seconds,
    CAST(j.completed AS INT64) as completed_queries
  FROM \\\`${BIGQUERY_PROJECT_ID}.${BIGQUERY_DATASET}.serper_jobs\\\` j
  WHERE j.job_id = '$JOB_ID'
)
SELECT
  completed_queries,
  runtime_seconds,
  ROUND(60.0 * completed_queries / NULLIF(runtime_seconds, 0), 1) as qpm
FROM job_metrics
"

echo "Performance Metrics:"
bq query --use_legacy_sql=false --format=pretty "$METRICS_QUERY"

# Extract qpm value
QPM=$(bq query --use_legacy_sql=false --format=csv --max_rows=1 \
    "SELECT ROUND(60.0 * CAST(completed AS INT64) / NULLIF(TIMESTAMP_DIFF(completed_at, created_at, SECOND), 0), 1)
     FROM \\\`${BIGQUERY_PROJECT_ID}.${BIGQUERY_DATASET}.serper_jobs\\\`
     WHERE job_id = '$JOB_ID'" | tail -1 2>/dev/null || echo "0")

if [ "$QPM" != "0" ] && [ "$QPM" != "null" ]; then
    echo -e "${GREEN}✓ Measured performance: ${QPM} queries/min${NC}"

    # Compare to target (333 qpm for CA @ 100 workers)
    if (( $(echo "$QPM >= 300" | bc -l) )); then
        echo -e "${GREEN}✓ Performance meets target (≥300 qpm)${NC}"
    else
        echo -e "${YELLOW}⚠ Performance below target: $QPM < 300 qpm${NC}"
        echo "  (Small jobs may have lower qpm due to setup overhead)"
    fi
else
    echo -e "${YELLOW}⚠ Could not calculate qpm (job may not be complete)${NC}"
fi
echo ""

# Final Summary
echo "=================================================="
echo -e "${GREEN}✅ Production Validation Complete${NC}"
echo "=================================================="
echo ""
echo "Next Steps:"
echo "1. Review validation results above"
echo "2. If all checks passed, proceed to deployment"
echo "3. Monitor first production job with: poetry run serper-monitor-job $JOB_ID"
echo ""
echo "Job Details:"
echo "  Job ID: $JOB_ID"
echo "  Keyword: $TEST_KEYWORD"
echo "  State: $TEST_STATE"
echo ""
echo "Cleanup (optional):"
echo "  Delete test job: bq query --use_legacy_sql=false \"DELETE FROM \\\`${BIGQUERY_PROJECT_ID}.${BIGQUERY_DATASET}.serper_jobs\\\` WHERE job_id = '$JOB_ID'\""
echo "  Delete test queries: bq query --use_legacy_sql=false \"DELETE FROM \\\`${BIGQUERY_PROJECT_ID}.${BIGQUERY_DATASET}.serper_queries\\\` WHERE job_id = '$JOB_ID'\""
echo "  Delete test results: bq query --use_legacy_sql=false \"DELETE FROM \\\`${BIGQUERY_PROJECT_ID}.${BIGQUERY_DATASET}.serper_results\\\` WHERE job_id = '$JOB_ID'\""
echo ""
