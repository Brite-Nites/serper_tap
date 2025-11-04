#!/usr/bin/env bash
# ============================================================================
# Emergency Rollback Script
# ============================================================================
# LEARN: Why have a rollback plan?
# - Critical production issues require quick reversion
# - Automated rollback reduces human error under pressure
# - Preserves data while reverting code/config
# - Can be executed in <5 minutes
# ============================================================================

set -euo pipefail

# ----------------------------------------------------------------------------
# CONFIGURATION
# ----------------------------------------------------------------------------
REPO_DIR="/opt/serper_tap"
SERVICE_NAME="prefect-worker"

# Get previous stable version (you must specify this!)
ROLLBACK_TAG="${1:-}"

if [ -z "$ROLLBACK_TAG" ]; then
    echo "❌ Usage: $0 <rollback-tag>"
    echo ""
    echo "Available recent tags:"
    cd "$REPO_DIR" && git tag -l | tail -10
    echo ""
    echo "Example:"
    echo "  $0 v1.2.3"
    echo "  $0 rollback-20250104"
    echo ""
    exit 1
fi

echo "============================================================================"
echo "EMERGENCY ROLLBACK"
echo "============================================================================"
echo "Target: $ROLLBACK_TAG"
echo "Repo:   $REPO_DIR"
echo ""

read -p "Are you sure you want to rollback to $ROLLBACK_TAG? (yes/no): " CONFIRM
if [ "$CONFIRM" != "yes" ]; then
    echo "Rollback cancelled"
    exit 0
fi

# ----------------------------------------------------------------------------
# Step 1: Stop Worker
# ----------------------------------------------------------------------------
echo ""
echo "Step 1: Stopping Prefect worker..."

if sudo systemctl is-active --quiet "$SERVICE_NAME"; then
    sudo systemctl stop "$SERVICE_NAME"
    echo "✓ Worker stopped"
else
    echo "  Worker already stopped"
fi

# ----------------------------------------------------------------------------
# Step 2: Finish In-Flight Jobs (Optional)
# ----------------------------------------------------------------------------
echo ""
echo "Step 2: Finishing in-flight jobs..."
echo "  Skipping (worker is stopped, jobs will resume on restart)"

# Optional: If you want to complete in-flight jobs before rollback:
# cd "$REPO_DIR" && poetry run serper-process-batches

# ----------------------------------------------------------------------------
# Step 3: Checkout Rollback Version
# ----------------------------------------------------------------------------
echo ""
echo "Step 3: Checking out $ROLLBACK_TAG..."

cd "$REPO_DIR" || exit 1

# Verify tag exists
if ! git rev-parse "$ROLLBACK_TAG" &>/dev/null; then
    echo "❌ Tag not found: $ROLLBACK_TAG"
    echo ""
    echo "Available tags:"
    git tag -l | tail -20
    exit 1
fi

# Checkout the tag
git fetch --all --tags
git checkout "$ROLLBACK_TAG"

echo "✓ Checked out $ROLLBACK_TAG"

# ----------------------------------------------------------------------------
# Step 4: Reinstall Dependencies
# ----------------------------------------------------------------------------
echo ""
echo "Step 4: Reinstalling dependencies..."

poetry install --no-interaction

echo "✓ Dependencies reinstalled"

# ----------------------------------------------------------------------------
# Step 5: Reapply Deployment
# ----------------------------------------------------------------------------
echo ""
echo "Step 5: Reapplying Prefect deployment..."

# Load environment
export $(grep -v '^#' .env.production | grep -v '^$' | xargs)

# Reapply deployment
poetry run prefect deployment apply deployment.yaml

echo "✓ Deployment reapplied"

# ----------------------------------------------------------------------------
# Step 6: Restart Worker
# ----------------------------------------------------------------------------
echo ""
echo "Step 6: Restarting Prefect worker..."

sudo systemctl start "$SERVICE_NAME"

sleep 3

if sudo systemctl is-active --quiet "$SERVICE_NAME"; then
    echo "✓ Worker restarted and ACTIVE"
else
    echo "❌ Worker failed to start"
    echo ""
    echo "Check logs:"
    echo "  sudo journalctl -u $SERVICE_NAME -n 100"
    exit 1
fi

# ----------------------------------------------------------------------------
# Step 7: Verify Service
# ----------------------------------------------------------------------------
echo ""
echo "Step 7: Verifying service..."

sudo systemctl status "$SERVICE_NAME" --no-pager -l | head -15

# ----------------------------------------------------------------------------
# Summary
# ----------------------------------------------------------------------------
echo ""
echo "============================================================================"
echo "✅ ROLLBACK COMPLETE"
echo "============================================================================"
echo ""
echo "Rolled back to: $ROLLBACK_TAG"
echo "Worker status:  ACTIVE"
echo ""
echo "Next steps:"
echo "  1. Monitor logs for 5-10 minutes:"
echo "     sudo journalctl -u $SERVICE_NAME -f"
echo ""
echo "  2. Check Prefect Cloud for worker health"
echo ""
echo "  3. If stable, investigate root cause of issue that required rollback"
echo ""
echo "  4. To return to latest:"
echo "     cd $REPO_DIR"
echo "     git checkout main"
echo "     poetry install"
echo "     poetry run prefect deployment apply deployment.yaml"
echo "     sudo systemctl restart $SERVICE_NAME"
echo ""
