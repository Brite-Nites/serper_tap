# Serper Tap - Production Operations Runbook

## Daily Operations

### Starting/Stopping Services

**Start Prefect Worker**
```bash
# Via systemd (production)
sudo systemctl start prefect-worker
sudo systemctl status prefect-worker

# Manual (development)
cd /opt/serper_tap
source .venv/bin/activate
prefect worker start --pool default-pool
```

**Stop Prefect Worker**
```bash
sudo systemctl stop prefect-worker
```

### Creating Jobs

**Standard Job Creation**
```bash
source .venv/bin/activate
poetry run serper-create-job \
  --keyword bars \
  --state AZ \
  --pages 3 \
  --batch-size 150 \
  --concurrency 150
```

**Dry Run (validation only)**
```bash
poetry run serper-create-job \
  --keyword test \
  --state RI \
  --pages 1 \
  --dry-run
```

### Monitoring Commands

**Check System Health**
```bash
poetry run serper-health-check
poetry run serper-health-check --json  # JSON output
```

**Monitor Job Progress**
```bash
poetry run serper-monitor-job <JOB_ID>
poetry run serper-monitor-job <JOB_ID> --interval 5
```

**Check Worker Status**
```bash
prefect worker ls
journalctl -u prefect-worker -f  # Follow logs
```

**Quick Performance Check**
```bash
bq query --use_legacy_sql=false --format=csv '
SELECT
  COUNT(*) as queries,
  ROUND(60.0*COUNT(*)/NULLIF(TIMESTAMP_DIFF(MAX(ran_at),MIN(ran_at),SECOND),0),1) as qpm
FROM `brite-nites-data-platform.raw_data.serper_queries`
WHERE ran_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 10 MINUTE)
  AND status="success"'
```

## KPI Thresholds & Alerts

### Performance Targets
- **QPM (Queries Per Minute)**: ≥ 300 target
  - ⚠️  Alert if < 250 sustained for 10 minutes
  - 🔴 Critical if < 150 for 10 minutes

### Error Rates
- **API 429 (Rate Limit)**: < 5% of requests
  - ⚠️  Alert if > 5%
  - 🔴 Critical if > 10%

- **API 5xx (Server Errors)**: < 1% of requests
  - ⚠️  Alert if > 1%
  - 🔴 Critical if > 3%

### Latency
- **BigQuery MERGE p50**: < 6 seconds
  - ⚠️  Alert if > 10s
  - 🔴 Critical if > 20s

### Cost Management
- **Daily Budget**: $100 default
  - Soft threshold (80%): $80 - warning logged
  - Hard threshold (100%): $100 - jobs blocked
  - ⚠️  Alert at 80%
  - 🔴 Alert at 90%

### Data Quality
- **JSON Parse Success Rate**: > 99.5%
  - ⚠️  Alert if < 99%
  - Check `payload_raw` column for failures

## Tripwires & Tuning Ladder

### Symptom: Low QPM (< 250)

1. **Check queue depth**
   ```sql
   SELECT COUNT(*) FROM `PROJECT.DATASET.serper_queries` WHERE status='queued';
   ```

2. **Check worker status**
   ```bash
   prefect worker ls
   journalctl -u prefect-worker -n 50
   ```

3. **Tuning actions (in order)**
   - Increase `PROCESSOR_MAX_WORKERS` from 100 → 150
   - Increase `DEFAULT_BATCH_SIZE` from 150 → 200
   - Check API rate limits (429 errors)
   - Scale workers horizontally (multiple pools)

### Symptom: High 429 Rate (> 5%)

1. **Reduce concurrency**
   ```bash
   # Temporarily set lower values
   export DEFAULT_CONCURRENCY=100
   export DEFAULT_BATCH_SIZE=100
   ```

2. **Implement exponential backoff**
   - Check `SERPER_RETRY_DELAY_SECONDS` setting
   - Increase from 5 → 10 seconds

3. **Contact Serper.dev for rate limit increase**

### Symptom: High 5xx Rate (> 1%)

1. **Check Serper.dev status page**

2. **Implement circuit breaker**
   ```bash
   # Pause processing temporarily
   sudo systemctl stop prefect-worker
   ```

3. **Monitor and resume**
   ```bash
   # After 15-30 minutes
   sudo systemctl start prefect-worker
   ```

### Symptom: Slow MERGE Operations (> 10s)

1. **Check BigQuery slots**
   ```bash
   bq show --project_id=brite-nites-data-platform
   ```

2. **Verify chunking is working**
   ```bash
   grep "MERGE store 500 places" <recent_logs>
   ```

3. **Reduce batch size if needed**
   ```bash
   export DEFAULT_BATCH_SIZE=100
   ```

### Symptom: Cost Runaway

1. **Check today's spend**
   ```sql
   SELECT SUM(CAST(totals.credits AS INT64)) * 0.01 AS usd
   FROM `PROJECT.DATASET.serper_jobs`
   WHERE DATE(created_at) = CURRENT_DATE();
   ```

2. **Immediate actions**
   ```bash
   # Stop worker
   sudo systemctl stop prefect-worker

   # Lower budget
   export DAILY_BUDGET_USD=50

   # Review jobs
   bq query --use_legacy_sql=false '
   SELECT job_id, keyword, state, totals.credits
   FROM `PROJECT.DATASET.serper_jobs`
   WHERE DATE(created_at) = CURRENT_DATE()
   ORDER BY totals.credits DESC;'
   ```

## Troubleshooting Runbooks

### "Job Stuck" (queries queued but not processing)

**Symptoms**: Queries remain in 'queued' status for > 10 minutes

**Diagnosis**
```sql
-- Check stuck queries
SELECT job_id, COUNT(*) as queued_count, MIN(created_at) as oldest
FROM `PROJECT.DATASET.serper_queries`
WHERE status='queued'
GROUP BY job_id;

-- Check worker status
prefect worker ls
```

**Resolution**
```bash
# 1. Check worker is running
sudo systemctl status prefect-worker

# 2. Restart worker if not responding
sudo systemctl restart prefect-worker

# 3. Check for deployment issues
prefect deployment ls | grep process-job-batches

# 4. Manually trigger if needed
poetry run serper-process-batches

# 5. Reset stuck queries if truly orphaned (>1 hour in processing)
bq query --use_legacy_sql=false "
UPDATE \`PROJECT.DATASET.serper_queries\`
SET status='queued', claim_id=NULL, claimed_at=NULL
WHERE status='processing'
  AND claimed_at < TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR);"
```

### "High 429 Errors" (rate limiting)

**Symptoms**: Multiple 429 errors in logs, QPM drops

**Diagnosis**
```sql
SELECT
  COUNT(*) as total,
  COUNTIF(api_status=429) as rate_limited,
  ROUND(100.0*COUNTIF(api_status=429)/COUNT(*),2) as pct
FROM `PROJECT.DATASET.serper_queries`
WHERE ran_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 10 MINUTE);
```

**Resolution**
```bash
# 1. Reduce concurrency immediately
cat > /tmp/emergency_settings.env << EOF
DEFAULT_CONCURRENCY=50
DEFAULT_BATCH_SIZE=50
PROCESSOR_MAX_WORKERS=50
SERPER_RETRY_DELAY_SECONDS=10
EOF

# 2. Restart with new settings
sudo systemctl stop prefect-worker
export $(cat /tmp/emergency_settings.env | xargs)
sudo systemctl start prefect-worker

# 3. Monitor for improvement
watch -n 10 'bq query --use_legacy_sql=false --format=csv "
SELECT COUNTIF(api_status=429) as rate_limited_last_5min
FROM \`PROJECT.DATASET.serper_queries\`
WHERE ran_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 5 MINUTE);" | tail -1'

# 4. Gradually increase after stabilization (10+ minutes)
```

### "MERGE Slow" (BigQuery write latency)

**Symptoms**: "MERGE store" operations taking > 10 seconds

**Diagnosis**
```bash
# Check recent MERGE timings from logs
journalctl -u prefect-worker -n 1000 | grep "MERGE store" | tail -20

# Check BigQuery job history
bq ls -j --max_results=50 --format=prettyjson | \
  jq -r '.[] | select(.configuration.query.query | contains("MERGE")) |
  "\(.statistics.endTime) \(.statistics.totalBytesProcessed)"'
```

**Resolution**
```bash
# 1. Verify chunking is enabled (should see "500 places" not larger)
grep "MERGE store [0-9]* places" <logs> | sort -k3 -n | tail -10

# 2. If chunks too large, verify MERGE_CHUNK_SIZE in code
grep "MERGE_CHUNK_SIZE" src/operations/bigquery_ops.py

# 3. Check BigQuery quota/slots
bq show --project_id=brite-nites-data-platform

# 4. Temporarily reduce batch size
export DEFAULT_BATCH_SIZE=75

# 5. Contact GCP support if persistent (> 30 min)
```

### "Cost Runaway" (budget exceeded)

**Symptoms**: Budget guard blocking, unexpected high spend

**Diagnosis**
```sql
-- Today's spend by job
SELECT
  job_id,
  keyword,
  state,
  pages,
  totals.queries,
  totals.credits,
  ROUND(totals.credits * 0.01, 2) as cost_usd,
  created_at
FROM `PROJECT.DATASET.serper_jobs`
WHERE DATE(created_at) = CURRENT_DATE()
ORDER BY totals.credits DESC;

-- Verify credit accounting
SELECT
  SUM(credits) as total_from_queries,
  (SELECT SUM(CAST(totals.credits AS INT64)) FROM `PROJECT.DATASET.serper_jobs`
   WHERE DATE(created_at)=CURRENT_DATE()) as total_from_jobs
FROM `PROJECT.DATASET.serper_queries`
WHERE DATE(ran_at) = CURRENT_DATE();
```

**Resolution**
```bash
# 1. IMMEDIATE: Stop all processing
sudo systemctl stop prefect-worker

# 2. Set emergency budget cap
export DAILY_BUDGET_USD=0.01  # Effectively blocks new jobs

# 3. Investigate anomalous jobs
bq query --use_legacy_sql=false '
SELECT job_id, keyword, state, pages, totals
FROM `PROJECT.DATASET.serper_jobs`
WHERE DATE(created_at) = CURRENT_DATE()
  AND CAST(totals.credits AS INT64) > 300;'  -- Adjust threshold

# 4. Cancel/mark failed if needed
bq query --use_legacy_sql=false "
UPDATE \`PROJECT.DATASET.serper_jobs\`
SET status='failed', finished_at=CURRENT_TIMESTAMP()
WHERE job_id IN ('<suspicious_job_id>');"

# 5. Review and adjust budget for tomorrow
# 6. Restart with appropriate safeguards
```

## Rollback Procedures

### Emergency Rollback (< 5 minutes)

**When to rollback**: Critical production issue, data corruption, severe performance degradation

**Steps**:
```bash
# 1. Stop worker immediately
sudo systemctl stop prefect-worker

# 2. Identify previous stable version
git log --oneline -10
git tag -l | tail -5

# 3. Checkout previous version
git checkout <previous-stable-tag>  # e.g., v1.2.3

# 4. Reinstall dependencies
poetry install

# 5. Revert migrations if needed (CAREFUL!)
# Check which migrations were applied
bq query --use_legacy_sql=false '
SELECT column_name FROM `PROJECT.DATASET.INFORMATION_SCHEMA.COLUMNS`
WHERE table_name="serper_places" AND column_name="payload_raw";'

# If migration needs reverting (only if it caused issues):
# bq query --use_legacy_sql=false "
# ALTER TABLE \`PROJECT.DATASET.serper_places\` DROP COLUMN payload_raw;"

# 6. Re-apply previous deployment
prefect deployment apply deployment.yaml

# 7. Restart worker
sudo systemctl start prefect-worker

# 8. Monitor for stability
journalctl -u prefect-worker -f
```

### Planned Rollback (testing/validation)

**Steps**:
```bash
# 1. Create rollback tag before deployment
git tag -a rollback-$(date +%Y%m%d) -m "Pre-deployment checkpoint"
git push origin rollback-$(date +%Y%m%d)

# 2. When needed, rollback gracefully
sudo systemctl stop prefect-worker

# 3. Finish in-flight jobs
poetry run serper-process-batches  # Let it complete

# 4. Checkout rollback point
git checkout rollback-<date>

# 5. Standard deployment process
poetry install
prefect deployment apply deployment.yaml
sudo systemctl start prefect-worker
```

## Escalation Contacts

- **Prefect Issues**: Prefect Cloud support, docs.prefect.io
- **BigQuery Issues**: GCP Support Console
- **Serper.dev API**: support@serper.dev
- **On-Call Engineer**: [Your team's on-call rotation]

## Health Check Automation

**Cron Job for Monitoring** (add to crontab):
```cron
# Check system health every 5 minutes
*/5 * * * * cd /opt/serper_tap && .venv/bin/python -m src.cli health_check --json >> /var/log/serper_tap/health.log 2>&1

# Daily cost report
0 9 * * * cd /opt/serper_tap && /usr/bin/bq query --use_legacy_sql=false --format=csv 'SELECT SUM(CAST(totals.credits AS INT64))*0.01 AS cost_usd FROM `brite-nites-data-platform.raw_data.serper_jobs` WHERE DATE(created_at)=CURRENT_DATE()' | mail -s "Serper Daily Cost" ops@company.com
```

## Performance Tuning Reference

| Metric | Current | Target | Tuning Knob |
|--------|---------|--------|-------------|
| QPM | Variable | ≥ 300 | `PROCESSOR_MAX_WORKERS`, `DEFAULT_BATCH_SIZE` |
| API 429 Rate | < 1% | < 5% | `DEFAULT_CONCURRENCY`, `SERPER_RETRY_DELAY_SECONDS` |
| MERGE Latency | ~3-6s | < 10s | `MERGE_CHUNK_SIZE` (code), BigQuery slots |
| Places/Hour | ~18,000 | Variable | All of above |
| Cost/Day | $10-$20 | < $100 | `DAILY_BUDGET_USD`, job frequency |

## Log Locations

- **Prefect Worker**: `journalctl -u prefect-worker`
- **Application Logs**: Prefect Cloud UI
- **BigQuery Audit**: GCP Console → BigQuery → Query History
- **Health Checks**: `/var/log/serper_tap/health.log`
