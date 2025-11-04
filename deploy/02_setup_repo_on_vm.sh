#!/usr/bin/env bash
# ============================================================================
# Repo Setup Script for GCE VM (Run this ON the VM via SSH)
# ============================================================================
# LEARN: Why this setup?
# - Python 3.11.x for compatibility (repo requires ~3.11, not 3.13)
# - Poetry for dependency management and venv isolation
# - /opt/serper_tap as standard system service location
# - ADC (Application Default Credentials) eliminates key file management
# ============================================================================

set -euo pipefail

# ----------------------------------------------------------------------------
# CONFIGURATION
# ----------------------------------------------------------------------------
REPO_URL="https://github.com/Brite-Nites/serper_tap.git"
REPO_DIR="/opt/serper_tap"
PYTHON_VERSION="3.11"

echo "============================================================================"
echo "Setting up serper_tap repository on GCE VM"
echo "============================================================================"
echo "Repo URL:  $REPO_URL"
echo "Target:    $REPO_DIR"
echo "Python:    $PYTHON_VERSION"
echo ""

# ----------------------------------------------------------------------------
# Step 1: System Dependencies
# ----------------------------------------------------------------------------
echo "Step 1: Installing system dependencies..."

sudo apt-get update -qq
sudo apt-get install -y \
    software-properties-common \
    build-essential \
    git \
    jq \
    curl \
    wget

echo "✓ Base packages installed"

# Install Python 3.11
if ! command -v python3.11 &>/dev/null; then
    echo "Installing Python 3.11..."
    sudo add-apt-repository -y ppa:deadsnakes/ppa
    sudo apt-get update -qq
    sudo apt-get install -y python3.11 python3.11-venv python3.11-dev
    echo "✓ Python 3.11 installed"
else
    echo "✓ Python 3.11 already installed"
fi

# Install Poetry
if ! command -v poetry &>/dev/null; then
    echo "Installing Poetry..."
    curl -sSL https://install.python-poetry.org | python3.11 -
    export PATH="$HOME/.local/bin:$PATH"
    echo 'export PATH="$HOME/.local/bin:$PATH"' >> ~/.bashrc
    echo "✓ Poetry installed"
else
    echo "✓ Poetry already installed"
fi

# Verify installations
echo ""
echo "Verifying installations:"
python3.11 --version
poetry --version

# ----------------------------------------------------------------------------
# Step 2: Clone Repository
# ----------------------------------------------------------------------------
echo ""
echo "Step 2: Cloning repository..."

if [ -d "$REPO_DIR" ]; then
    echo "⚠️  Directory already exists: $REPO_DIR"
    echo "   To re-clone, first: sudo rm -rf $REPO_DIR"
else
    sudo mkdir -p "$REPO_DIR"
    sudo chown "$USER:$USER" "$REPO_DIR"
    git clone "$REPO_URL" "$REPO_DIR"
    echo "✓ Repository cloned"
fi

cd "$REPO_DIR"

# ----------------------------------------------------------------------------
# Step 3: Poetry Environment Setup
# ----------------------------------------------------------------------------
echo ""
echo "Step 3: Setting up Poetry environment..."

# Configure Poetry to use Python 3.11
poetry env use python3.11

# Install dependencies
poetry install --no-interaction --no-ansi

echo "✓ Dependencies installed"

# Verify environment
echo ""
echo "Poetry environment info:"
poetry env info

# ----------------------------------------------------------------------------
# Step 4: Create .env.production
# ----------------------------------------------------------------------------
echo ""
echo "Step 4: Creating .env.production..."

cat > "$REPO_DIR/.env.production" <<'EOF'
# ============================================================================
# Serper Tap - Production Environment Configuration (GCE VM)
# ============================================================================
# SECURITY: This file contains secrets. Never commit to git.
# ============================================================================

# --------------------------------------------------------------------------
# GCP Authentication & BigQuery
# --------------------------------------------------------------------------
# NOTE: Using Application Default Credentials (ADC) via VM service account
# GOOGLE_APPLICATION_CREDENTIALS is NOT set - the VM's attached SA is used
BIGQUERY_PROJECT_ID=brite-nites-data-platform
BIGQUERY_DATASET=raw_data

# --------------------------------------------------------------------------
# Serper API
# --------------------------------------------------------------------------
USE_MOCK_API=false
SERPER_API_KEY=YOUR_SERPER_API_KEY_HERE

# --------------------------------------------------------------------------
# Cost Controls
# --------------------------------------------------------------------------
DAILY_BUDGET_USD=100
COST_PER_CREDIT=0.01
BUDGET_SOFT_THRESHOLD_PCT=80
BUDGET_HARD_THRESHOLD_PCT=100

# --------------------------------------------------------------------------
# Processing Parameters (Tuning Knobs)
# --------------------------------------------------------------------------
PROCESSOR_MAX_WORKERS=100
DEFAULT_BATCH_SIZE=150
DEFAULT_CONCURRENCY=150
DEFAULT_PAGES=3
EARLY_EXIT_THRESHOLD=10

# --------------------------------------------------------------------------
# Prefect Cloud Configuration
# --------------------------------------------------------------------------
PREFECT_API_URL=https://api.prefect.cloud/api/accounts/YOUR_ACCOUNT_ID/workspaces/YOUR_WORKSPACE_ID
PREFECT_API_KEY=YOUR_PREFECT_API_KEY_HERE
PREFECT_DEPLOYMENT_NAME=process-job-batches/production
EOF

echo "✓ .env.production created"

# ----------------------------------------------------------------------------
# Step 5: Verify ADC Authentication
# ----------------------------------------------------------------------------
echo ""
echo "Step 5: Verifying Application Default Credentials..."

# Test BigQuery access
if bq ls --project_id=brite-nites-data-platform raw_data &>/dev/null; then
    echo "✓ BigQuery access verified via ADC"
else
    echo "❌ BigQuery access FAILED"
    echo "   Check service account permissions on the VM"
    exit 1
fi

# Test Python ADC
poetry run python -c "
import google.auth
credentials, project = google.auth.default()
print(f'✓ ADC credentials loaded: {credentials.service_account_email}')
print(f'  Project: {project}')
" || {
    echo "❌ Python ADC check failed"
    exit 1
}

# ----------------------------------------------------------------------------
# Summary
# ----------------------------------------------------------------------------
echo ""
echo "============================================================================"
echo "✅ REPO SETUP COMPLETE"
echo "============================================================================"
echo ""
echo "⚠️  MANUAL STEPS REQUIRED:"
echo ""
echo "1. Edit .env.production and update these values:"
echo "   - SERPER_API_KEY (get from https://serper.dev/dashboard)"
echo "   - PREFECT_API_URL (from Prefect Cloud workspace settings)"
echo "   - PREFECT_API_KEY (from Prefect Cloud → Settings → API Keys)"
echo ""
echo "   Run:"
echo "   nano $REPO_DIR/.env.production"
echo ""
echo "2. After updating .env.production, continue with:"
echo "   cd $REPO_DIR/deploy"
echo "   ./03_run_migrations.sh"
echo ""
echo "Current status:"
echo "  Repo:        $REPO_DIR"
echo "  Python:      $(poetry run python --version)"
echo "  Poetry env:  $(poetry env info --path)"
echo "  ADC:         ✓ Verified"
echo ""
