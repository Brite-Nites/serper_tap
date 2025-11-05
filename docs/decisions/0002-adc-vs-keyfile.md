# ADR-0002: Application Default Credentials vs Service Account Keyfiles

**Status:** Accepted
**Date:** November 2024
**Deciders:** Brite Nites Data Platform Team
**Context:** Production deployment to GCE

## Context and Problem Statement

When deploying Prefect workers on Google Compute Engine (GCE) VMs, we need to authenticate with BigQuery. Two approaches are available:

1. **Service Account Keyfiles**: Download JSON keyfile, store on VM, set `GOOGLE_APPLICATION_CREDENTIALS`
2. **Application Default Credentials (ADC)**: Attach service account to VM, use implicit credentials

**Key question**: Which authentication method should we use for production deployments?

## Decision

We will use **Application Default Credentials (ADC)** on GCE VMs and **avoid keyfiles in production**.

**Implementation**:
```python
# src/utils/bigquery_client.py
if credentials_path is None or "application_default_credentials.json" in credentials_path:
    # Use ADC (GCE VM with attached service account)
    credentials, project = google.auth.default(
        scopes=["https://www.googleapis.com/auth/bigquery"]
    )
    client = bigquery.Client(credentials=credentials, project=project)
else:
    # Fallback: Explicit keyfile (local dev only)
    credentials = service_account.Credentials.from_service_account_file(
        credentials_path, scopes=[...]
    )
    client = bigquery.Client(credentials=credentials, project=project)
```

**Production configuration** (`.env.production`):
```bash
# GOOGLE_APPLICATION_CREDENTIALS is NOT set (uses ADC)
BIGQUERY_PROJECT_ID=brite-nites-data-platform
BIGQUERY_DATASET=raw_data
PREFECT_API_URL=https://api.prefect.cloud/...
```

**Local development** (`.env`):
```bash
# Option 1: Use gcloud ADC
# $ gcloud auth application-default login
# GOOGLE_APPLICATION_CREDENTIALS not set

# Option 2: Explicit keyfile
GOOGLE_APPLICATION_CREDENTIALS=/path/to/dev-keyfile.json
BIGQUERY_PROJECT_ID=brite-nites-dev
BIGQUERY_DATASET=raw_data
```

## Consequences

### Positive

✅ **Security**: No keyfiles on disk, no secrets in environment variables
✅ **Automatic rotation**: Google manages credential lifecycle
✅ **Audit trail**: IAM logs show which service account performed actions
✅ **Simpler deployment**: No keyfile distribution or management
✅ **Best practice**: Google's recommended approach for GCE workloads

### Negative

❌ **Local dev requires gcloud**: Developers must run `gcloud auth application-default login`
❌ **Different behavior**: Local vs production authentication differs (acceptable trade-off)

### Neutral

⚖️ **VM metadata dependency**: Relies on GCE metadata server (highly available)

## Implementation Details

### Production Deployment

**GCE VM creation** (`deploy/01_create_vm.sh`):
```bash
gcloud compute instances create serper-tap-worker \
  --zone=us-central1-a \
  --machine-type=e2-standard-2 \
  --service-account=serper-tap-worker@PROJECT.iam.gserviceaccount.com \
  --scopes=https://www.googleapis.com/auth/bigquery
```

**Service account permissions**:
- `roles/bigquery.dataEditor` (read/write tables)
- `roles/bigquery.jobUser` (run queries)

**Worker startup**:
```bash
# No GOOGLE_APPLICATION_CREDENTIALS needed
cd /opt/serper_tap
poetry run serper-process-batches  # Uses ADC automatically
```

### Local Development

**Option 1: gcloud ADC (recommended)**:
```bash
# One-time setup
gcloud auth application-default login

# Use dev project
gcloud config set project brite-nites-dev

# Run locally (uses ADC)
poetry run serper-create-job --keyword "bars" --state "AZ"
```

**Option 2: Explicit keyfile**:
```bash
# Download keyfile from GCP Console
# Add to .env:
GOOGLE_APPLICATION_CREDENTIALS=/path/to/dev-keyfile.json

# Run locally
poetry run serper-create-job --keyword "bars" --state "AZ"
```

### Testing ADC

**Verify ADC is working**:
```bash
# On GCE VM
python -c "import google.auth; creds, project = google.auth.default(); print(f'Project: {project}')"
# Output: Project: brite-nites-data-platform

# Check service account
gcloud auth list
# Output: serper-tap-worker@brite-nites-data-platform.iam.gserviceaccount.com
```

## Alternative Approaches Considered

### Alternative 1: Service Account Keyfiles (rejected)

**Approach**: Store JSON keyfile on VM at `/etc/gcp/sa.json`, set `GOOGLE_APPLICATION_CREDENTIALS`

**Pros**:
- Familiar to developers
- Same auth method for local and production

**Cons**:
- ❌ **Security risk**: Keyfile can be exfiltrated from VM
- ❌ **Rotation complexity**: Must update keyfile periodically
- ❌ **Secret management**: Need secure way to distribute keyfiles
- ❌ **Audit trail**: Harder to trace actions to specific VMs

**Why rejected**: Security best practice is to avoid keyfiles on VMs when ADC is available.

### Alternative 2: Workload Identity (considered, deferred)

**Approach**: Use Workload Identity to federate GKE pods with GCP service accounts

**Pros**:
- Most secure option
- Fine-grained per-pod permissions

**Cons**:
- ⚠️ **Requires GKE**: We're using GCE VMs, not Kubernetes
- ⚠️ **Complexity**: Adds K8s layer for simple worker deployment

**Why deferred**: ADC on GCE is sufficient for current needs. Revisit if we migrate to GKE.

## Troubleshooting

### "Could not automatically determine credentials"

**Cause**: Running locally without ADC or keyfile

**Solution**:
```bash
# Use gcloud ADC
gcloud auth application-default login

# Or set keyfile
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/keyfile.json
```

### "Permission denied" on GCE VM

**Cause**: Service account lacks BigQuery permissions

**Solution**:
```bash
# Grant roles to service account
gcloud projects add-iam-policy-binding PROJECT_ID \
  --member="serviceAccount:serper-tap-worker@PROJECT.iam.gserviceaccount.com" \
  --role="roles/bigquery.dataEditor"

gcloud projects add-iam-policy-binding PROJECT_ID \
  --member="serviceAccount:serper-tap-worker@PROJECT.iam.gserviceaccount.com" \
  --role="roles/bigquery.jobUser"

# Restart worker to pick up new permissions
sudo systemctl restart prefect-worker
```

### Worker crashes with "Service account does not exist"

**Cause**: Service account propagation delay (new service account takes ~60s to propagate)

**Solution**:
```bash
# Wait 60-90 seconds after creating service account
sleep 90

# Then retry granting roles
```

## Related

- [ARCHITECTURE.md - Orchestration & Deployment](../../ARCHITECTURE.md#orchestration--deployment)
- [deploy/README.md - Deployment Guide](../../deploy/README.md)
- [Google Cloud: Application Default Credentials](https://cloud.google.com/docs/authentication/application-default-credentials)
- [Google Cloud: Best practices for using service accounts](https://cloud.google.com/iam/docs/best-practices-service-accounts)

## Revision History

- **2024-11**: Initial decision, implemented in production deployment
- **2024-11**: Updated `src/utils/bigquery_client.py` to prioritize ADC
- **2024-11**: Removed `GOOGLE_APPLICATION_CREDENTIALS` from `.env.production`
