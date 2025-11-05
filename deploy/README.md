# Serper Tap - GCE Production Deployment Guide

## Overview

This directory contains all scripts and configuration files needed to deploy the serper_tap Prefect + BigQuery pipeline to Google Compute Engine (GCE) for production use.

**Deployment Target**: Ubuntu 22.04 LTS VM on GCE
**Orchestration**: Prefect Cloud
**Database**: BigQuery (`brite-nites-data-platform.raw_data`)
**Authentication**: VM-attached service account (ADC)
**Process Management**: systemd

---

## Prerequisites

Before starting, ensure you have:

1. **GCP Project Access**: `brite-nites-data-platform` with permission to:
   - Create service accounts
   - Create Compute Engine VMs
   - Grant BigQuery permissions

2. **Prefect Cloud Account**:
   - Workspace created
   - API key generated
   - Workspace URL available

3. **Serper API Key**:
   - Active account at https://serper.dev/
   - API key accessible

4. **Local Tools** (for running scripts):
   - `gcloud` CLI configured with project authentication
   - `git` for cloning the repository
   - SSH access to GCP

---

## Deployment Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                      Prefect Cloud                          │
│  (Orchestration, UI, Flow Registry, Work Pool Coordination) │
└────────────────────┬────────────────────────────────────────┘
                     │
                     │ HTTPS API
                     │
┌────────────────────▼────────────────────────────────────────┐
│              GCE VM (serper-tap-worker)                      │
│  ┌──────────────────────────────────────────────────────┐  │
│  │  systemd Service: prefect-worker                      │  │
│  │  ├─ Poetry venv (Python 3.11)                        │  │
│  │  ├─ Prefect Worker (polls Prefect Cloud for work)    │  │
│  │  └─ Executes: process_job_batches flow              │  │
│  └──────────────────────────────────────────────────────┘  │
│                                                              │
│  Service Account (ADC): serper-tap-sa@...                  │
│  ├─ BigQuery Job User                                       │
│  └─ BigQuery Data Editor                                    │
└────────────────────┬────────────────────────────────────────┘
                     │
                     │ BigQuery API
                     │
┌────────────────────▼────────────────────────────────────────┐
│         BigQuery (brite-nites-data-platform.raw_data)       │
│  Tables: serper_jobs, serper_queries, serper_places         │
└─────────────────────────────────────────────────────────────┘
```

---

## Deployment Steps

### Step 1: Provision GCE VM

**Script**: `01_provision_gce_vm.sh`

Creates:
- Service account: `serper-tap-sa` with BigQuery permissions
- GCE VM: Ubuntu 22.04, e2-standard-2 (2 vCPU, 8GB RAM)
- Attached service account for ADC authentication

**Run locally** (on your machine with `gcloud` configured):

```bash
cd /path/to/serper_tap/deploy
./01_provision_gce_vm.sh
```

**Customization**: Edit variables at top of script:
- `ZONE`: VM location (default: `us-central1-a`)
- `VM_NAME`: Instance name (default: `serper-tap-worker`)
- `MACHINE_TYPE`: VM size (default: `e2-standard-2`)

**Duration**: ~2-3 minutes

**Output**: VM external IP and SSH command

---

### Step 2: Setup Repository on VM

**Script**: `02_setup_repo_on_vm.sh`

Installs:
- Python 3.11, Poetry, git, jq, BigQuery CLI
- Clones repository to `/opt/serper_tap`
- Creates `.env.production` template
- Verifies ADC authentication

**Run on VM** (SSH into VM first):

```bash
# SSH into VM
gcloud compute ssh serper-tap-worker --zone=us-central1-a --project=brite-nites-data-platform

# On VM: download and run setup script
cd /opt/serper_tap/deploy
./02_setup_repo_on_vm.sh
```

**Manual step required**: Edit `.env.production` to add:
- `SERPER_API_KEY`
- `PREFECT_API_URL`
- `PREFECT_API_KEY`

```bash
nano /opt/serper_tap/.env.production
```

**Duration**: ~5-7 minutes

---

### Step 3: Run Migrations

**Script**: `03_run_migrations.sh`

Verifies/applies:
- Migration 001: `batch_size` column
- Migration 002: `payload_raw` column (JSON hardening)

**Run on VM**:

```bash
cd /opt/serper_tap/deploy
./03_run_migrations.sh
```

**Idempotent**: Safe to run multiple times

**Duration**: ~30 seconds

---

### Step 4: Deploy to Prefect Cloud

**Script**: `04_deploy_prefect.sh`

Actions:
- Authenticates with Prefect Cloud using API key
- Applies `deployment.yaml` to register flow
- Verifies deployment appears in Prefect Cloud

**Run on VM**:

```bash
cd /opt/serper_tap/deploy
./04_deploy_prefect.sh
```

**Verification**: Check Prefect Cloud UI → Deployments → `process-job-batches/production`

**Duration**: ~1 minute

---

### Step 5: Setup Systemd Worker

**Script**: `05_setup_systemd_worker.sh`
**Service Unit**: `prefect-worker.service`

Actions:
- Creates `svc-prefect` system user
- Installs systemd service unit
- Enables auto-start on boot
- Starts worker service

**Run on VM**:

```bash
cd /opt/serper_tap/deploy
./05_setup_systemd_worker.sh
```

**Verification**:
```bash
sudo systemctl status prefect-worker
sudo journalctl -u prefect-worker -f
```

**Duration**: ~2 minutes

---

### Step 6: Run Canary Validation

**Script**: `/opt/serper_tap/first_run.sh`

Validates:
- Tooling versions (Python, Poetry, bq, Prefect)
- Migrations applied correctly
- Prefect deployment registered
- Worker online and processing
- Creates real canary job (bars × AZ)
- Measures QPM ≥ 300 target
- Verifies JSON parse health ≥ 99.5%
- Checks cost within budget
- Confirms idempotency

**Run on VM**:

```bash
cd /opt/serper_tap
./first_run.sh
```

**Duration**: ~5-10 minutes (depends on canary job size)

**Output**: Proof block between sentinels:
```
===== BEGIN_SERPER_TAP_PROOF =====
{
  "vm_name": "serper-tap-worker",
  "zone": "us-central1-a",
  ...
}
===== END_SERPER_TAP_PROOF =====
```

**IMPORTANT**: Copy this entire proof block (including sentinels) for stakeholder sign-off.

---

## Post-Deployment

### Verify Worker in Prefect Cloud

1. Go to Prefect Cloud UI: https://app.prefect.cloud/
2. Navigate to: **Work Pools** → `default-pool`
3. Confirm worker status: **ONLINE**

### Monitor Logs

```bash
# Follow worker logs in real-time
sudo journalctl -u prefect-worker -f

# View last 100 lines
sudo journalctl -u prefect-worker -n 100

# Filter for errors
sudo journalctl -u prefect-worker | grep -i error
```

### Create Production Jobs

```bash
cd /opt/serper_tap

# Example: Scrape all bars in California
poetry run serper-create-job \
  --keyword bars \
  --state CA \
  --pages 3 \
  --batch-size 150 \
  --concurrency 150
```

### Monitor Job Progress

```bash
# Get job ID from create-job output, then:
poetry run serper-monitor-job <JOB_ID> --interval 5
```

---

## Operational Commands

### Service Management

```bash
# Start worker
sudo systemctl start prefect-worker

# Stop worker
sudo systemctl stop prefect-worker

# Restart worker
sudo systemctl restart prefect-worker

# Check status
sudo systemctl status prefect-worker

# View logs
sudo journalctl -u prefect-worker -f
```

### Health Checks

```bash
cd /opt/serper_tap

# System health check
poetry run serper-health-check

# JSON output for scripting
poetry run serper-health-check --json
```

### Manual Processing

```bash
# Process all running jobs (useful for testing)
poetry run serper-process-batches
```

---

## Troubleshooting

### Worker Not Starting

**Check logs**:
```bash
sudo journalctl -u prefect-worker -n 100 --no-pager
```

**Common issues**:
- `.env.production` missing or has invalid values
- Prefect API key expired or incorrect
- Poetry venv not properly initialized

**Fix**:
```bash
cd /opt/serper_tap
poetry env info  # Verify venv path
cat .env.production | grep -v '^#'  # Check env vars
poetry run prefect cloud login -k "$PREFECT_API_KEY"  # Re-auth
```

### BigQuery Access Denied

**Verify service account**:
```bash
bq ls --project_id=brite-nites-data-platform raw_data

# If fails, check VM metadata
curl -H "Metadata-Flavor: Google" \
  http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/email
```

**Fix**: Ensure service account has correct roles (see Step 1)

### High 429 Rate Limits

**Check error rate**:
```bash
bq query --use_legacy_sql=false '
SELECT api_status, COUNT(*) as count
FROM `brite-nites-data-platform.raw_data.serper_queries`
WHERE ran_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 10 MINUTE)
GROUP BY api_status
ORDER BY count DESC'
```

**Fix**: Reduce concurrency in `.env.production`:
```bash
DEFAULT_CONCURRENCY=100  # Down from 150
DEFAULT_BATCH_SIZE=100   # Down from 150
```

Restart worker: `sudo systemctl restart prefect-worker`

### Slow QPM (< 300)

**Check current QPM**:
```bash
bq query --use_legacy_sql=false --format=csv '
SELECT ROUND(60.0*COUNT(*)/NULLIF(TIMESTAMP_DIFF(MAX(ran_at),MIN(ran_at),SECOND),0),1) as qpm
FROM `brite-nites-data-platform.raw_data.serper_queries`
WHERE ran_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 10 MINUTE)
  AND status="success"' | tail -1
```

**Fix**: Increase workers in `.env.production`:
```bash
PROCESSOR_MAX_WORKERS=150  # Up from 100
```

---

## Emergency Rollback

If a deployment causes critical issues:

```bash
cd /opt/serper_tap/deploy

# List available tags/versions
git tag -l | tail -10

# Rollback to previous stable version
./rollback.sh v1.2.3
```

**Duration**: < 5 minutes

---

## Files in This Directory

| File | Purpose |
|------|---------|
| `01_provision_gce_vm.sh` | Create VM and service account |
| `02_setup_repo_on_vm.sh` | Install dependencies and clone repo |
| `03_run_migrations.sh` | Apply BigQuery schema migrations |
| `04_deploy_prefect.sh` | Register deployment with Prefect Cloud |
| `05_setup_systemd_worker.sh` | Install and start systemd service |
| `prefect-worker.service` | Systemd unit file |
| `rollback.sh` | Emergency rollback to previous version |
| `PASS_FAIL_MATRIX.md` | Validation checklist template |
| `README.md` | This file |

---

## Security Notes

### Secrets Management

**DO NOT** commit these files to git:
- `.env.production` (contains API keys)
- `deployment_proof.json` (may contain sensitive IDs)

Add to `.gitignore`:
```
.env.production
deployment_proof.json
```

### Service Account Permissions

The `serper-tap-sa` service account has:
- ✅ BigQuery Job User (project-level)
- ✅ BigQuery Data Editor (project-level)

**Principle of least privilege**: In production, consider scoping Data Editor to `raw_data` dataset only.

### VM Security

Hardening applied in systemd service:
- Runs as non-root user (`svc-prefect`)
- `NoNewPrivileges=true`
- `PrivateTmp=true`
- `ProtectSystem=strict`
- Read-only access to sensitive paths

---

## Cost Estimation

**VM Cost** (e2-standard-2, us-central1):
- On-demand: ~$49/month (~$0.067/hour)
- Committed use (1 year): ~$35/month

**BigQuery Cost**:
- Query processing: ~$5/TB scanned
- Storage: ~$20/TB/month (active), ~$10/TB/month (long-term)

**Serper API Cost**:
- $0.01/credit
- Controlled by `DAILY_BUDGET_USD` (default: $100/day)

**Total Estimated**: $100-150/month (VM + BigQuery + controlled API spend)

---

## Next Steps

After successful deployment:

1. **Monitor for 24 hours**
   - Check worker logs: `sudo journalctl -u prefect-worker -f`
   - Verify QPM stays ≥ 300
   - Monitor cost dashboard in BigQuery

2. **Set up alerting** (optional but recommended)
   - Prefect Cloud: Flow run failure notifications
   - GCP Monitoring: VM health, BigQuery quota alerts

3. **Create first production job**
   - Start with a small state (e.g., RI, VT)
   - Monitor completion before scaling to larger states (CA, TX)

4. **Review `OPERATIONS.md`**
   - Familiarize with troubleshooting runbooks
   - Review KPI thresholds and tuning ladder

5. **Schedule regular jobs** (optional)
   - Edit `deployment.yaml` to add cron schedule
   - Reapply deployment: `poetry run prefect deployment apply deployment.yaml`

---

## Support

**Documentation**:
- Prefect: https://docs.prefect.io/
- BigQuery: https://cloud.google.com/bigquery/docs
- Serper.dev: https://serper.dev/api-reference

**Internal**:
- `OPERATIONS.md`: Daily operations and troubleshooting
- `sql/views/dashboards.sql`: Production monitoring queries
- GitHub Issues: [Your repo URL]/issues

---

**Last Updated**: 2025-01-04
**Version**: 1.0.0
