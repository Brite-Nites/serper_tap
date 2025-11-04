#!/usr/bin/env bash
# ============================================================================
# Prefect Cloud Deployment Setup (Run ON the VM)
# ============================================================================
# LEARN: Why Prefect Cloud?
# - Centralized orchestration and monitoring
# - Flow run history and logs accessible from web UI
# - Deployments register flow metadata (not code execution location)
# - Worker on VM pulls work from Prefect Cloud and executes locally
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
echo "Prefect Cloud Deployment Setup"
echo "============================================================================"
echo "API URL: $PREFECT_API_URL"
echo "Deployment: $PREFECT_DEPLOYMENT_NAME"
echo ""

# ----------------------------------------------------------------------------
# Step 1: Verify Prefect API Key
# ----------------------------------------------------------------------------
echo "Step 1: Verifying Prefect API key..."

if [ -z "${PREFECT_API_KEY:-}" ]; then
    echo "❌ PREFECT_API_KEY not set in .env.production"
    echo ""
    echo "To fix:"
    echo "  1. Go to Prefect Cloud: https://app.prefect.cloud/"
    echo "  2. Navigate to: Settings → API Keys"
    echo "  3. Create or copy an API key"
    echo "  4. Edit .env.production and set:"
    echo "     PREFECT_API_KEY=pnu_your_key_here"
    exit 1
fi

echo "✓ PREFECT_API_KEY is set"

# ----------------------------------------------------------------------------
# Step 2: Authenticate with Prefect Cloud
# ----------------------------------------------------------------------------
echo ""
echo "Step 2: Authenticating with Prefect Cloud..."

# Login with API key
if poetry run prefect cloud login -k "$PREFECT_API_KEY" 2>&1 | grep -q "Authenticated"; then
    echo "✓ Authenticated with Prefect Cloud"
else
    echo "❌ Authentication failed"
    echo "  Check your PREFECT_API_KEY and PREFECT_API_URL in .env.production"
    exit 1
fi

# Verify connection
echo ""
echo "Prefect Cloud workspace info:"
poetry run prefect config view | grep -E "(PREFECT_API_URL|PREFECT_API_KEY)" || true

# ----------------------------------------------------------------------------
# Step 3: Apply Deployment
# ----------------------------------------------------------------------------
echo ""
echo "Step 3: Applying deployment.yaml..."

if [ ! -f "deployment.yaml" ]; then
    echo "❌ deployment.yaml not found"
    echo "  Expected location: $REPO_DIR/deployment.yaml"
    exit 1
fi

# Apply deployment
poetry run prefect deployment apply deployment.yaml

echo "✓ Deployment applied"

# ----------------------------------------------------------------------------
# Step 4: Verify Deployment
# ----------------------------------------------------------------------------
echo ""
echo "Step 4: Verifying deployment registration..."

# List all deployments
echo ""
echo "All deployments:"
poetry run prefect deployment ls

# Check for our specific deployment
if poetry run prefect deployment ls | grep -q "$PREFECT_DEPLOYMENT_NAME"; then
    echo ""
    echo "✓ Deployment found: $PREFECT_DEPLOYMENT_NAME"
else
    echo ""
    echo "⚠️  Deployment not found in list"
    echo "   Expected: $PREFECT_DEPLOYMENT_NAME"
fi

# ----------------------------------------------------------------------------
# Step 5: Check Work Pools
# ----------------------------------------------------------------------------
echo ""
echo "Step 5: Checking work pools..."

poetry run prefect work-pool ls || {
    echo "⚠️  No work pools found"
    echo "   You may need to create 'default-pool' in Prefect Cloud UI"
}

# ----------------------------------------------------------------------------
# Summary
# ----------------------------------------------------------------------------
echo ""
echo "============================================================================"
echo "✅ PREFECT DEPLOYMENT COMPLETE"
echo "============================================================================"
echo ""
echo "Next steps:"
echo "  1. Set up systemd worker service:"
echo "     ./05_setup_systemd_worker.sh"
echo ""
echo "  2. OR manually start a worker (for testing):"
echo "     cd $REPO_DIR"
echo "     poetry run prefect worker start --pool default-pool"
echo ""
echo "Deployment details:"
echo "  Name:       $PREFECT_DEPLOYMENT_NAME"
echo "  Entrypoint: src/flows/process_batches.py:process_job_batches"
echo "  Work Pool:  default-pool"
echo ""
