#!/usr/bin/env bash
# ============================================================================
# Systemd Worker Service Setup (Run ON the VM)
# ============================================================================
# LEARN: Why systemd?
# - Auto-start worker on VM boot (survives reboots)
# - Automatic restart on crashes (resilience)
# - Centralized logging via journald
# - Standard service management (start/stop/status/logs)
# - Security hardening (user isolation, filesystem restrictions)
# ============================================================================

set -euo pipefail

# ----------------------------------------------------------------------------
# CONFIGURATION
# ----------------------------------------------------------------------------
REPO_DIR="/opt/serper_tap"
SERVICE_NAME="prefect-worker"
SERVICE_FILE="/etc/systemd/system/${SERVICE_NAME}.service"
SERVICE_USER="svc-prefect"

echo "============================================================================"
echo "Setting up systemd worker service"
echo "============================================================================"
echo "Service:  $SERVICE_NAME"
echo "User:     $SERVICE_USER"
echo "Location: $SERVICE_FILE"
echo ""

# ----------------------------------------------------------------------------
# Step 1: Create Service User
# ----------------------------------------------------------------------------
echo "Step 1: Creating service user..."

if id "$SERVICE_USER" &>/dev/null; then
    echo "✓ User already exists: $SERVICE_USER"
else
    sudo useradd --system \
        --home-dir /opt/serper_tap \
        --shell /bin/bash \
        --comment "Prefect Worker Service Account" \
        "$SERVICE_USER"
    echo "✓ User created: $SERVICE_USER"
fi

# Install Poetry for service user
echo "Installing Poetry for $SERVICE_USER..."
sudo -u "$SERVICE_USER" bash <<'POETRY_INSTALL'
if ! command -v poetry &>/dev/null; then
    curl -sSL https://install.python-poetry.org | python3.11 -
    echo 'export PATH="$HOME/.local/bin:$PATH"' >> ~/.bashrc
fi
POETRY_INSTALL

echo "✓ Poetry installed for $SERVICE_USER"

# ----------------------------------------------------------------------------
# Step 2: Set Permissions
# ----------------------------------------------------------------------------
echo ""
echo "Step 2: Setting file permissions..."

sudo chown -R "$SERVICE_USER:$SERVICE_USER" "$REPO_DIR"
sudo chmod 755 "$REPO_DIR"
sudo chmod 600 "$REPO_DIR/.env.production"

echo "✓ Permissions set"

# ----------------------------------------------------------------------------
# Step 3: Install Systemd Service Unit
# ----------------------------------------------------------------------------
echo ""
echo "Step 3: Installing systemd service unit..."

if [ ! -f "$REPO_DIR/deploy/prefect-worker.service" ]; then
    echo "❌ Service file not found: $REPO_DIR/deploy/prefect-worker.service"
    exit 1
fi

sudo cp "$REPO_DIR/deploy/prefect-worker.service" "$SERVICE_FILE"
sudo chmod 644 "$SERVICE_FILE"

echo "✓ Service unit installed: $SERVICE_FILE"

# ----------------------------------------------------------------------------
# Step 4: Reload Systemd
# ----------------------------------------------------------------------------
echo ""
echo "Step 4: Reloading systemd daemon..."

sudo systemctl daemon-reload

echo "✓ Systemd reloaded"

# ----------------------------------------------------------------------------
# Step 5: Enable and Start Service
# ----------------------------------------------------------------------------
echo ""
echo "Step 5: Enabling and starting service..."

# Enable (auto-start on boot)
sudo systemctl enable "$SERVICE_NAME"
echo "✓ Service enabled (auto-start on boot)"

# Start the service
sudo systemctl start "$SERVICE_NAME"
echo "✓ Service started"

# Wait a moment for startup
sleep 3

# ----------------------------------------------------------------------------
# Step 6: Verify Service Status
# ----------------------------------------------------------------------------
echo ""
echo "Step 6: Verifying service status..."

if sudo systemctl is-active --quiet "$SERVICE_NAME"; then
    echo "✓ Service is ACTIVE"
else
    echo "❌ Service is NOT active"
    echo ""
    echo "Check logs with:"
    echo "  sudo journalctl -u $SERVICE_NAME -n 50"
    exit 1
fi

# Show status
echo ""
echo "Service status:"
sudo systemctl status "$SERVICE_NAME" --no-pager -l | head -20

# ----------------------------------------------------------------------------
# Summary
# ----------------------------------------------------------------------------
echo ""
echo "============================================================================"
echo "✅ SYSTEMD WORKER SERVICE SETUP COMPLETE"
echo "============================================================================"
echo ""
echo "Service management commands:"
echo "  Status:   sudo systemctl status $SERVICE_NAME"
echo "  Stop:     sudo systemctl stop $SERVICE_NAME"
echo "  Start:    sudo systemctl start $SERVICE_NAME"
echo "  Restart:  sudo systemctl restart $SERVICE_NAME"
echo "  Logs:     sudo journalctl -u $SERVICE_NAME -f"
echo ""
echo "Next steps:"
echo "  1. Monitor logs for a few minutes:"
echo "     sudo journalctl -u $SERVICE_NAME -f"
echo ""
echo "  2. Verify worker connected to Prefect Cloud"
echo "     (Check Prefect Cloud UI → Work Pools → default-pool)"
echo ""
echo "  3. Run canary test:"
echo "     cd $REPO_DIR"
echo "     ./first_run.sh"
echo ""
