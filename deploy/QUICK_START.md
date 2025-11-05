# Serper Tap - GCE Deployment Quick Start

## TL;DR - Complete Deployment in 6 Steps

This guide gets your serper_tap pipeline running on GCE in ~20 minutes.

---

## Prerequisites Checklist

Before starting, have these ready:

- [ ] GCP project access (`brite-nites-data-platform`)
- [ ] `gcloud` CLI installed and authenticated
- [ ] Prefect Cloud workspace URL
- [ ] Prefect Cloud API key
- [ ] Serper.dev API key

---

## Step-by-Step Deployment

### Step 1: Provision VM (Local Machine)

**Duration**: 2-3 minutes

```bash
# Clone repo locally first (if not already done)
git clone <YOUR_REPO_URL> serper_tap
cd serper_tap/deploy

# Edit VM settings if needed (optional)
nano 01_provision_gce_vm.sh
# Change ZONE, VM_NAME, MACHINE_TYPE as desired

# Run provisioning
./01_provision_gce_vm.sh
```

**Output**: VM external IP and SSH command

---

### Step 2: SSH into VM

```bash
# Use the SSH command from Step 1 output, OR:
gcloud compute ssh serper-tap-worker \
  --zone=us-central1-a \
  --project=brite-nites-data-platform
```

**All remaining steps run ON THE VM**

---

### Step 3: Setup Repository (On VM)

**Duration**: 5-7 minutes

```bash
# Update REPO_URL first!
cd /opt/serper_tap/deploy
nano 02_setup_repo_on_vm.sh
# Change: REPO_URL="https://github.com/YOUR_ORG/serper_tap.git"

# Run setup
./02_setup_repo_on_vm.sh
```

**Manual step**: Edit `.env.production`

```bash
nano /opt/serper_tap/.env.production
```

Update these 3 lines:
```bash
SERPER_API_KEY=your_actual_serper_key
PREFECT_API_URL=https://api.prefect.cloud/api/accounts/YOUR_ACCOUNT/workspaces/YOUR_WORKSPACE
PREFECT_API_KEY=pnu_your_prefect_key
```

Save and exit (`Ctrl+O`, `Enter`, `Ctrl+X`)

---

### Step 4: Run Migrations (On VM)

**Duration**: 30 seconds

```bash
cd /opt/serper_tap/deploy
./03_run_migrations.sh
```

**Verify**: Should see "✅ MIGRATIONS COMPLETE"

---

### Step 5: Deploy to Prefect Cloud (On VM)

**Duration**: 1 minute

```bash
cd /opt/serper_tap/deploy
./04_deploy_prefect.sh
```

**Verify**: Check Prefect Cloud UI → Deployments → should see `process-job-batches/production`

---

### Step 6: Start Worker Service (On VM)

**Duration**: 2 minutes

```bash
cd /opt/serper_tap/deploy
./05_setup_systemd_worker.sh
```

**Verify**: Worker should be ACTIVE

```bash
sudo systemctl status prefect-worker
```

---

### Step 7: Run Canary Test (On VM)

**Duration**: 5-10 minutes

```bash
cd /opt/serper_tap
./first_run.sh
```

**IMPORTANT**: At the end, you'll see:

```
===== BEGIN_SERPER_TAP_PROOF =====
{
  "vm_name": "serper-tap-worker",
  ...
}
===== END_SERPER_TAP_PROOF =====
```

**Copy this entire block** (including the sentinel lines) for verification.

---

## Verification Checklist

After `first_run.sh` completes, verify:

- [ ] QPM ≥ 300 (or ≥ 250 with warning)
- [ ] JSON success rate ≥ 99.5%
- [ ] Cost within DAILY_BUDGET_USD
- [ ] Idempotency = true
- [ ] Worker status = ONLINE
- [ ] Deployment present = yes

**If all checks pass**: ✅ **GO FOR PRODUCTION**

---

## Quick Reference Commands

### Service Management
```bash
# Start/stop/restart worker
sudo systemctl start prefect-worker
sudo systemctl stop prefect-worker
sudo systemctl restart prefect-worker

# View logs
sudo journalctl -u prefect-worker -f
```

### Create Jobs
```bash
cd /opt/serper_tap

# Example job
poetry run serper-create-job \
  --keyword bars \
  --state CA \
  --pages 3 \
  --batch-size 150
```

### Monitor Jobs
```bash
poetry run serper-monitor-job <JOB_ID>
```

### Health Check
```bash
poetry run serper-health-check
```

---

## Troubleshooting

### Worker won't start

```bash
# Check logs
sudo journalctl -u prefect-worker -n 50

# Common fixes:
cd /opt/serper_tap
cat .env.production | grep -v '^#'  # Verify env vars
poetry env info  # Verify venv
poetry install  # Reinstall deps
sudo systemctl restart prefect-worker
```

### BigQuery access denied

```bash
# Verify service account
bq ls --project_id=brite-nites-data-platform raw_data

# Check VM service account
curl -H "Metadata-Flavor: Google" \
  http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/email
```

### Prefect auth failed

```bash
# Re-authenticate
cd /opt/serper_tap
export $(grep -v '^#' .env.production | xargs)
poetry run prefect cloud login -k "$PREFECT_API_KEY"
```

---

## Emergency Rollback

```bash
cd /opt/serper_tap/deploy

# List available versions
git tag -l | tail -10

# Rollback to previous version
./rollback.sh v1.2.3
```

---

## Next Steps

1. **Monitor for 24 hours**
   ```bash
   sudo journalctl -u prefect-worker -f
   ```

2. **Check Prefect Cloud UI**
   - Go to: https://app.prefect.cloud/
   - Verify worker is ONLINE in Work Pools

3. **Create production jobs**
   ```bash
   cd /opt/serper_tap
   poetry run serper-create-job --keyword bars --state TX --pages 3
   ```

4. **Review full documentation**
   - `deploy/README.md` - Comprehensive deployment guide
   - `OPERATIONS.md` - Daily operations and troubleshooting
   - `sql/views/dashboards.sql` - Monitoring queries

---

## Files Generated

All deployment artifacts are in `/opt/serper_tap/deploy/`:

- `01_provision_gce_vm.sh` - VM creation
- `02_setup_repo_on_vm.sh` - Repo setup
- `03_run_migrations.sh` - Database migrations
- `04_deploy_prefect.sh` - Prefect Cloud setup
- `05_setup_systemd_worker.sh` - Worker service
- `prefect-worker.service` - Systemd unit
- `rollback.sh` - Emergency rollback
- `PASS_FAIL_MATRIX.md` - Validation template
- `README.md` - Full documentation
- `QUICK_START.md` - This file

Plus at repo root:
- `first_run.sh` - Canary validation
- `deployment_proof.json` - Generated proof (after first_run.sh)

---

## Support

**Issues?** See:
- `deploy/README.md` - Troubleshooting section
- `OPERATIONS.md` - Detailed runbooks
- GitHub Issues: <YOUR_REPO_URL>/issues

---

**Ready to deploy?** Start with Step 1 above!
