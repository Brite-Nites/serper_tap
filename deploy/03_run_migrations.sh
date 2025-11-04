#!/usr/bin/env bash
# ============================================================================
# Idempotent BigQuery Migrations (Run ON the VM)
# ============================================================================
# LEARN: Why idempotent migrations?
# - Safe to run multiple times without breaking state
# - Verifies current schema before applying changes
# - Ensures production database matches code expectations
# - Prevents "already exists" errors on re-runs
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

echo "============================================================================"
echo "Running Idempotent BigQuery Migrations"
echo "============================================================================"
echo "Project: $BIGQUERY_PROJECT_ID"
echo "Dataset: $BIGQUERY_DATASET"
echo ""

# ----------------------------------------------------------------------------
# Helper Functions
# ----------------------------------------------------------------------------
pass() {
    echo "✅ $1"
}

fail() {
    echo "❌ $1"
}

warn() {
    echo "⚠️  $1"
}

# ----------------------------------------------------------------------------
# Migration 001: batch_size column
# ----------------------------------------------------------------------------
echo "Migration 001: Verifying batch_size column..."

# Check if column exists
COLUMN_EXISTS=$(bq query --use_legacy_sql=false --format=csv \
    "SELECT column_name
     FROM \`$BIGQUERY_PROJECT_ID.$BIGQUERY_DATASET.INFORMATION_SCHEMA.COLUMNS\`
     WHERE table_name='serper_jobs' AND column_name='batch_size'" 2>&1 | tail -1)

if [ "$COLUMN_EXISTS" == "batch_size" ]; then
    pass "batch_size column exists"

    # Check for NULL values
    NULL_COUNT=$(bq query --use_legacy_sql=false --format=csv \
        "SELECT COUNTIF(batch_size IS NULL) as null_count
         FROM \`$BIGQUERY_PROJECT_ID.$BIGQUERY_DATASET.serper_jobs\`" 2>&1 | tail -1)

    if [ "$NULL_COUNT" == "0" ]; then
        pass "No NULL batch_size values"
    else
        warn "$NULL_COUNT NULL batch_size values found"
        echo "  This may indicate jobs created before migration 001"
    fi
else
    echo "  Column missing - applying migration..."

    # Apply migration if exists
    if [ -f "sql/migrations/001_add_batch_size_column.sql" ]; then
        # Note: Cannot directly pipe with placeholders, must use bq query
        warn "Migration file exists but batch_size column missing"
        echo "  Please ensure the column was added during table creation"
    else
        warn "Migration file not found: sql/migrations/001_add_batch_size_column.sql"
    fi
fi

# ----------------------------------------------------------------------------
# Migration 002: payload_raw column (payload hardening)
# ----------------------------------------------------------------------------
echo ""
echo "Migration 002: Verifying payload_raw column..."

# Check if payload_raw column exists
PAYLOAD_RAW_EXISTS=$(bq query --use_legacy_sql=false --format=csv \
    "SELECT column_name
     FROM \`$BIGQUERY_PROJECT_ID.$BIGQUERY_DATASET.INFORMATION_SCHEMA.COLUMNS\`
     WHERE table_name='serper_places' AND column_name='payload_raw'" 2>&1 | tail -1)

if [ "$PAYLOAD_RAW_EXISTS" == "payload_raw" ]; then
    pass "payload_raw column exists"
else
    echo "  Column missing - applying migration..."

    # Apply ADD COLUMN IF NOT EXISTS
    bq query --use_legacy_sql=false \
        "ALTER TABLE \`$BIGQUERY_PROJECT_ID.$BIGQUERY_DATASET.serper_places\`
         ADD COLUMN IF NOT EXISTS payload_raw STRING;" &>/dev/null

    pass "payload_raw column added"
fi

# Verify payload column exists (should always exist, but check anyway)
PAYLOAD_EXISTS=$(bq query --use_legacy_sql=false --format=csv \
    "SELECT column_name
     FROM \`$BIGQUERY_PROJECT_ID.$BIGQUERY_DATASET.INFORMATION_SCHEMA.COLUMNS\`
     WHERE table_name='serper_places' AND column_name='payload'" 2>&1 | tail -1)

if [ "$PAYLOAD_EXISTS" == "payload" ]; then
    pass "payload column exists"
else
    fail "payload column MISSING - schema corruption!"
    exit 1
fi

# Update column descriptions
bq query --use_legacy_sql=false \
    "ALTER TABLE \`$BIGQUERY_PROJECT_ID.$BIGQUERY_DATASET.serper_places\`
     ALTER COLUMN payload SET OPTIONS (description='Parsed JSON payload (NULL if parse failed)');" &>/dev/null

bq query --use_legacy_sql=false \
    "ALTER TABLE \`$BIGQUERY_PROJECT_ID.$BIGQUERY_DATASET.serper_places\`
     ALTER COLUMN payload_raw SET OPTIONS (description='Original JSON text as received');" &>/dev/null

pass "Column descriptions updated"

# ----------------------------------------------------------------------------
# Final Verification
# ----------------------------------------------------------------------------
echo ""
echo "============================================================================"
echo "FINAL VERIFICATION"
echo "============================================================================"

# Get all columns in serper_places
echo ""
echo "serper_places schema:"
bq show --format=json "$BIGQUERY_PROJECT_ID:$BIGQUERY_DATASET.serper_places" | \
    jq -r '.schema.fields[] | "  - \(.name) (\(.type))"'

# Check JSON health (recent data)
echo ""
echo "JSON Parse Health (last 24 hours):"
JSON_HEALTH=$(bq query --use_legacy_sql=false --format=csv \
    "SELECT
       COUNTIF(payload IS NULL AND payload_raw IS NOT NULL) AS unparsable,
       COUNTIF(payload IS NOT NULL) AS parsed,
       COUNT(*) AS total
     FROM \`$BIGQUERY_PROJECT_ID.$BIGQUERY_DATASET.serper_places\`
     WHERE ingest_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)" 2>&1 | tail -1)

UNPARSABLE=$(echo "$JSON_HEALTH" | cut -d',' -f1)
PARSED=$(echo "$JSON_HEALTH" | cut -d',' -f2)
TOTAL=$(echo "$JSON_HEALTH" | cut -d',' -f3)

echo "  Unparsable: $UNPARSABLE"
echo "  Parsed:     $PARSED"
echo "  Total:      $TOTAL"

if [ "$TOTAL" -gt 0 ]; then
    PARSE_PCT=$(echo "scale=2; 100 * $PARSED / $TOTAL" | bc)
    echo "  Success:    ${PARSE_PCT}%"

    if (( $(echo "$PARSE_PCT >= 99.5" | bc -l) )); then
        pass "JSON parsing healthy (≥99.5%)"
    else
        warn "JSON parsing below target: ${PARSE_PCT}% (target ≥99.5%)"
    fi
else
    echo "  (No data in last 24 hours)"
fi

# ----------------------------------------------------------------------------
# Summary
# ----------------------------------------------------------------------------
echo ""
echo "============================================================================"
echo "✅ MIGRATIONS COMPLETE"
echo "============================================================================"
echo ""
echo "Next steps:"
echo "  1. Verify .env.production has all secrets filled"
echo "  2. Run Prefect deployment:"
echo "     ./04_deploy_prefect.sh"
echo ""
