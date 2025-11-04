#!/usr/bin/env bash
# ============================================================================
# GCE VM Provisioning for serper_tap Production Deployment
# ============================================================================
# LEARN: Why GCE?
# - Always-on VM for running Prefect worker (survives local machine shutdown)
# - Attached service account for seamless BigQuery auth (no key files)
# - Systemd manages worker lifecycle (auto-restart, logging, boot persistence)
# ============================================================================

set -euo pipefail

# ----------------------------------------------------------------------------
# CONFIGURATION - Customize these values
# ----------------------------------------------------------------------------
PROJECT_ID="brite-nites-data-platform"
ZONE="us-central1-a"  # Change to your preferred zone
VM_NAME="serper-tap-worker"
MACHINE_TYPE="e2-standard-2"  # 2 vCPU, 8GB RAM
DISK_SIZE_GB="50"
BOOT_DISK_TYPE="pd-standard"
NETWORK="default"
SUBNET="default"
SERVICE_ACCOUNT_NAME="serper-tap-sa"
SERVICE_ACCOUNT_EMAIL="${SERVICE_ACCOUNT_NAME}@${PROJECT_ID}.iam.gserviceaccount.com"

echo "============================================================================"
echo "Provisioning GCE VM for serper_tap"
echo "============================================================================"
echo "Project:        $PROJECT_ID"
echo "Zone:           $ZONE"
echo "VM Name:        $VM_NAME"
echo "Machine Type:   $MACHINE_TYPE"
echo "Service Acct:   $SERVICE_ACCOUNT_EMAIL"
echo ""

# ----------------------------------------------------------------------------
# Step 1: Create Service Account (if doesn't exist)
# ----------------------------------------------------------------------------
echo "Step 1: Creating service account..."

if gcloud iam service-accounts describe "$SERVICE_ACCOUNT_EMAIL" --project="$PROJECT_ID" &>/dev/null; then
    echo "✓ Service account already exists: $SERVICE_ACCOUNT_EMAIL"
else
    gcloud iam service-accounts create "$SERVICE_ACCOUNT_NAME" \
        --project="$PROJECT_ID" \
        --display-name="Serper Tap Worker Service Account" \
        --description="Service account for serper_tap Prefect worker with BigQuery access"

    echo "✓ Service account created: $SERVICE_ACCOUNT_EMAIL"
fi

# ----------------------------------------------------------------------------
# Step 2: Grant BigQuery Permissions
# ----------------------------------------------------------------------------
echo ""
echo "Step 2: Granting BigQuery permissions..."

# BigQuery Job User (project-level - required to run queries)
gcloud projects add-iam-policy-binding "$PROJECT_ID" \
    --member="serviceAccount:$SERVICE_ACCOUNT_EMAIL" \
    --role="roles/bigquery.jobUser" \
    --condition=None

echo "✓ Granted BigQuery Job User"

# BigQuery Data Editor (project-level for simplicity)
# Note: In production, you may want to scope this to dataset-level only
gcloud projects add-iam-policy-binding "$PROJECT_ID" \
    --member="serviceAccount:$SERVICE_ACCOUNT_EMAIL" \
    --role="roles/bigquery.dataEditor" \
    --condition=None

echo "✓ Granted BigQuery Data Editor"

# ----------------------------------------------------------------------------
# Step 3: Create Compute Engine VM
# ----------------------------------------------------------------------------
echo ""
echo "Step 3: Creating Compute Engine VM..."

if gcloud compute instances describe "$VM_NAME" --zone="$ZONE" --project="$PROJECT_ID" &>/dev/null; then
    echo "✓ VM already exists: $VM_NAME"
    echo "  To recreate, first delete it:"
    echo "  gcloud compute instances delete $VM_NAME --zone=$ZONE --project=$PROJECT_ID"
else
    gcloud compute instances create "$VM_NAME" \
        --project="$PROJECT_ID" \
        --zone="$ZONE" \
        --machine-type="$MACHINE_TYPE" \
        --network-interface="network-tier=PREMIUM,stack-type=IPV4_ONLY,subnet=$SUBNET" \
        --metadata="enable-oslogin=true" \
        --maintenance-policy="MIGRATE" \
        --provisioning-model="STANDARD" \
        --service-account="$SERVICE_ACCOUNT_EMAIL" \
        --scopes="https://www.googleapis.com/auth/cloud-platform" \
        --create-disk="auto-delete=yes,boot=yes,device-name=$VM_NAME,image=projects/ubuntu-os-cloud/global/images/family/ubuntu-2204-lts,mode=rw,size=$DISK_SIZE_GB,type=projects/$PROJECT_ID/zones/$ZONE/diskTypes/$BOOT_DISK_TYPE" \
        --no-shielded-secure-boot \
        --shielded-vtpm \
        --shielded-integrity-monitoring \
        --labels="app=serper-tap,env=production" \
        --reservation-affinity="any"

    echo "✓ VM created: $VM_NAME"
fi

# ----------------------------------------------------------------------------
# Step 4: Wait for VM to be ready
# ----------------------------------------------------------------------------
echo ""
echo "Step 4: Waiting for VM to be ready..."
sleep 15

# Get external IP
EXTERNAL_IP=$(gcloud compute instances describe "$VM_NAME" \
    --zone="$ZONE" \
    --project="$PROJECT_ID" \
    --format="get(networkInterfaces[0].accessConfigs[0].natIP)")

echo "✓ VM is ready"
echo "  External IP: $EXTERNAL_IP"

# ----------------------------------------------------------------------------
# Summary
# ----------------------------------------------------------------------------
echo ""
echo "============================================================================"
echo "✅ GCE VM PROVISIONED SUCCESSFULLY"
echo "============================================================================"
echo ""
echo "Next steps:"
echo "  1. SSH into the VM:"
echo "     gcloud compute ssh $VM_NAME --zone=$ZONE --project=$PROJECT_ID"
echo ""
echo "  2. Or from your local machine (if you have gcloud configured):"
echo "     ssh -i ~/.ssh/google_compute_engine $USER@$EXTERNAL_IP"
echo ""
echo "  3. Run the setup script on the VM:"
echo "     ./02_setup_repo_on_vm.sh"
echo ""
echo "VM Details:"
echo "  Name:           $VM_NAME"
echo "  Zone:           $ZONE"
echo "  External IP:    $EXTERNAL_IP"
echo "  Service Acct:   $SERVICE_ACCOUNT_EMAIL"
echo "  Machine Type:   $MACHINE_TYPE"
echo ""
