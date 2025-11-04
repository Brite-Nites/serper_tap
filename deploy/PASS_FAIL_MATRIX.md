# Serper Tap Production Deployment - PASS/FAIL Matrix

## Deployment Validation Results

**Date**: _____________________
**Operator**: _____________________
**VM Name**: _____________________
**Zone**: _____________________

---

## Checklist

| Checkpoint               | Value | Threshold           | Pass? | Notes |
|--------------------------|-------|---------------------|-------|-------|
| **Python Version**       |       | 3.11.x              | ⬜    |       |
| **Poetry Installed**     |       | ✓ present           | ⬜    |       |
| **BigQuery CLI**         |       | ✓ present           | ⬜    |       |
| **Prefect CLI**          |       | ✓ present           | ⬜    |       |
| **Migration 001 Applied**|       | batch_size exists   | ⬜    |       |
| **Migration 002 Applied**|       | payload_raw exists  | ⬜    |       |
| **Prefect Deployment**   |       | must be "yes"       | ⬜    |       |
| **Worker Status**        |       | must be "ONLINE"    | ⬜    |       |
| **QPM (10-min)**         |       | ≥ 300 (warn ≥250)   | ⬜    |       |
| **JSON Success (24h)**   |       | ≥ 99.5%             | ⬜    |       |
| **Cost Today ($)**       |       | ≤ DAILY_BUDGET      | ⬜    |       |
| **Idempotency**          |       | must be "true"      | ⬜    |       |

---

## Canary Job Details

- **Job ID**: ___________________________________
- **Keyword**: bars
- **State**: AZ
- **Pages**: 3
- **Queries Total**: _________
- **Queries Completed**: _________
- **Places Found**: _________
- **Credits Used**: _________

---

## Performance Metrics

### QPM (Queries Per Minute)
- **10-min window**: _________
- **Target**: ≥ 300 qpm
- **Warn threshold**: ≥ 250 qpm
- **Status**: ⬜ PASS  ⬜ WARN  ⬜ FAIL

### JSON Parse Health
- **Parsed successfully**: _________
- **Failed to parse**: _________
- **Success rate**: __________%
- **Target**: ≥ 99.5%
- **Status**: ⬜ PASS  ⬜ WARN  ⬜ FAIL

### Cost Management
- **Cost today**: $_________
- **Daily budget**: $_________
- **Percentage used**: __________%
- **Status**: ⬜ PASS  ⬜ FAIL

### Idempotency Verification
- **Places before re-run**: _________
- **Places after re-run**: _________
- **Match**: ⬜ YES  ⬜ NO
- **Status**: ⬜ PASS  ⬜ FAIL

---

## System Status

### Service Health
- **Prefect worker**: ⬜ ONLINE  ⬜ OFFLINE
- **BigQuery access**: ⬜ ✓  ⬜ ✗
- **Serper API**: ⬜ ✓  ⬜ ✗

### Resource Usage
- **VM CPU**: _________% (check with `top`)
- **VM Memory**: _________% used
- **VM Disk**: _________% used (check with `df -h`)

---

## Final Decision

⬜ **GO FOR PRODUCTION** - All checks passed, system ready for production load

⬜ **NO-GO** - Critical failures detected, address issues before production

---

## Proof Block (Paste from first_run.sh output)

```json
{
  "vm_name": "",
  "zone": "",
  "python": "",
  "poetry": "",
  "prefect_deployment": "",
  "worker_status": "",
  "canary_job_id": "",
  "qpm_10m": 0,
  "json_success_pct_24h": 0,
  "cost_usd_today": 0,
  "idempotent": false,
  "timestamp_utc": ""
}
```

---

## Sign-off

**Technical Lead**: _____________________  Date: _________

**Operations Manager**: _____________________  Date: _________

---

## Notes / Issues Found

_Use this space to document any warnings, issues, or follow-up items:_

