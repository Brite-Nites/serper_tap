#!/usr/bin/env bash
# ============================================================================
# Serper Tap - Shared Validation Helpers Library
# ============================================================================
# Purpose: Common functions and utilities for validation and deployment scripts
#
# This library provides:
# - Color-coded output formatting (RED, GREEN, YELLOW, NC)
# - Helper functions for test results (pass(), fail(), warn())
# - BigQuery queries for performance metrics (QPM)
# - BigQuery queries for data quality checks (JSON parse health)
#
# Usage:
#   source "$(dirname "$0")/scripts/lib/validation_helpers.sh"
#
# Note: This library is shared across:
#   - deploy_and_validate.sh
#   - validate_production_ready.sh
#   - first_run.sh
# ============================================================================

# ----------------------------------------------------------------------------
# Color Constants
# ----------------------------------------------------------------------------
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# ----------------------------------------------------------------------------
# Test Result Helper Functions
# ----------------------------------------------------------------------------
# These functions provide consistent output formatting and result tracking
# across all validation scripts. They update the global RESULTS associative
# array which should be declared in the calling script.

pass() {
    echo -e "${GREEN}✅ PASS${NC}: $1"
    RESULTS["$1"]="PASS"
}

fail() {
    echo -e "${RED}❌ FAIL${NC}: $1"
    RESULTS["$1"]="FAIL"
}

warn() {
    echo -e "${YELLOW}⚠️  WARN${NC}: $1"
    RESULTS["$1"]="WARN"
}

# ----------------------------------------------------------------------------
# BigQuery Query: Queries Per Minute (QPM)
# ----------------------------------------------------------------------------
# Calculates the query processing rate over a specified time window.
#
# Usage:
#   qpm_result=$(get_qpm_query 30)  # 30-minute window
#   qpm_value=$(bq query --use_legacy_sql=false --format=csv "$qpm_result" 2>&1 | tail -1)
#
# Parameters:
#   $1 - Time window in minutes (default: 30)
#   $2 - Project ID (uses $PROJECT_ID or $BIGQUERY_PROJECT_ID if not provided)
#   $3 - Dataset (uses $DATASET or $BIGQUERY_DATASET if not provided)
#
# Returns:
#   SQL query string that calculates QPM
get_qpm_query() {
    local time_window="${1:-30}"
    local project_id="${2:-${PROJECT_ID:-${BIGQUERY_PROJECT_ID}}}"
    local dataset="${3:-${DATASET:-${BIGQUERY_DATASET}}}"

    cat <<EOF
SELECT ROUND(60.0*COUNT(*)/NULLIF(TIMESTAMP_DIFF(MAX(ran_at),MIN(ran_at),SECOND),0),1) as qpm
FROM \`${project_id}.${dataset}.serper_queries\`
WHERE ran_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL ${time_window} MINUTE)
  AND status='success'
EOF
}

# ----------------------------------------------------------------------------
# BigQuery Query: JSON Parse Health Check
# ----------------------------------------------------------------------------
# Checks the percentage of successfully parsed JSON payloads vs. unparsable ones
# over a specified time window (default: 24 hours).
#
# Usage:
#   json_health_query=$(get_json_health_query 24)
#   json_result=$(bq query --use_legacy_sql=false --format=csv "$json_health_query" 2>&1 | tail -1)
#   unparsable=$(echo "$json_result" | cut -d',' -f1)
#   parsed=$(echo "$json_result" | cut -d',' -f2)
#
# Parameters:
#   $1 - Time window in hours (default: 24)
#   $2 - Project ID (uses $PROJECT_ID or $BIGQUERY_PROJECT_ID if not provided)
#   $3 - Dataset (uses $DATASET or $BIGQUERY_DATASET if not provided)
#
# Returns:
#   SQL query string that returns: unparsable, parsed
get_json_health_query() {
    local time_window="${1:-24}"
    local project_id="${2:-${PROJECT_ID:-${BIGQUERY_PROJECT_ID}}}"
    local dataset="${3:-${DATASET:-${BIGQUERY_DATASET}}}"

    cat <<EOF
SELECT
  COUNTIF(payload IS NULL AND payload_raw IS NOT NULL) AS unparsable,
  COUNTIF(payload IS NOT NULL) AS parsed
FROM \`${project_id}.${dataset}.serper_places\`
WHERE ingest_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL ${time_window} HOUR)
EOF
}

# ----------------------------------------------------------------------------
# Helper: Execute QPM Check and Report Results
# ----------------------------------------------------------------------------
# Executes the QPM query and reports results using pass/fail/warn functions.
#
# Usage:
#   check_qpm 30 300 250  # 30-min window, target 300, warn at 250
#
# Parameters:
#   $1 - Time window in minutes (default: 30)
#   $2 - Target QPM threshold for PASS (default: 300)
#   $3 - Warning QPM threshold (default: 150)
check_qpm() {
    local time_window="${1:-30}"
    local target="${2:-300}"
    local warn_threshold="${3:-150}"
    local project_id="${PROJECT_ID:-${BIGQUERY_PROJECT_ID}}"
    local dataset="${DATASET:-${BIGQUERY_DATASET}}"

    local qpm_query=$(get_qpm_query "$time_window" "$project_id" "$dataset")
    local qpm_result=$(bq query --use_legacy_sql=false --format=csv "$qpm_query" 2>&1 | tail -1)

    if [ -n "$qpm_result" ] && [ "$qpm_result" != "null" ]; then
        local qpm_value=$(echo "$qpm_result" | tr -d ' ')
        if (( $(echo "$qpm_value >= $target" | bc -l) )); then
            pass "QPM target met: $qpm_value qpm (target ≥$target)"
        elif (( $(echo "$qpm_value >= $warn_threshold" | bc -l) )); then
            warn "QPM below target: $qpm_value qpm (target ≥$target)"
        else
            fail "QPM critically low: $qpm_value qpm (target ≥$target)"
        fi
        echo "$qpm_value"
    else
        warn "QPM calculation failed (insufficient data)"
        echo "0"
    fi
}

# ----------------------------------------------------------------------------
# Helper: Execute JSON Health Check and Report Results
# ----------------------------------------------------------------------------
# Executes the JSON health query and reports results using pass/fail/warn functions.
#
# Usage:
#   check_json_health 24 99.5 99.0  # 24-hour window, target 99.5%, warn at 99.0%
#
# Parameters:
#   $1 - Time window in hours (default: 24)
#   $2 - Target success percentage for PASS (default: 99.5)
#   $3 - Warning success percentage (default: 99.0)
check_json_health() {
    local time_window="${1:-24}"
    local target="${2:-99.5}"
    local warn_threshold="${3:-99.0}"
    local project_id="${PROJECT_ID:-${BIGQUERY_PROJECT_ID}}"
    local dataset="${DATASET:-${BIGQUERY_DATASET}}"

    local json_query=$(get_json_health_query "$time_window" "$project_id" "$dataset")
    local json_health=$(bq query --use_legacy_sql=false --format=csv "$json_query" 2>&1 | tail -1)

    local unparsable=$(echo "$json_health" | cut -d',' -f1)
    local parsed=$(echo "$json_health" | cut -d',' -f2)
    local total=$((unparsable + parsed))

    if [ "$total" -gt 0 ]; then
        local parse_pct=$(echo "scale=2; 100 * $parsed / $total" | bc)
        if (( $(echo "$parse_pct >= $target" | bc -l) )); then
            pass "JSON parsing healthy: $parse_pct% success ($unparsable failures preserved)"
        elif (( $(echo "$parse_pct >= $warn_threshold" | bc -l) )); then
            warn "JSON parsing: $parse_pct% success ($unparsable failures)"
        else
            fail "JSON parsing degraded: $parse_pct%"
        fi
        echo "$parse_pct"
    else
        warn "No recent place data for JSON health check"
        echo "100"
    fi
}
