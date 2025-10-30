# Quick Start: First Production Run

Everything you need to run your first production scraping job (bars × AZ).

**Estimated cost:** ~$12.48 (or $6-9 with early exit optimization)
**Estimated time:** 20-30 minutes to complete

---

## 1. Switch to Real API

Edit your `.env` file to use the real Serper API:

```bash
USE_MOCK_API=false
SERPER_API_KEY=7d8f04d6f5bc56bfcf85220dbfc25e19596b3559
```

Your `.env` should already have these values. Just verify `USE_MOCK_API=false`.

---

## 2. Create the Job

Run this command from the project root:

```bash
python -m src.flows.create_job --keyword bars --state AZ --pages 3
```

**Expected output:**
```
============================================================
SUCCESS
============================================================
Job ID: 1a2b3c4d-5e6f-7g8h-9i0j-1k2l3m4n5o6p
Queries created: 1248
Processor: background-pid-12345

Batch processor started automatically in background.
Queries will be processed shortly.
```

**Copy the Job ID** - you'll need it for monitoring.

---

## 3. Monitor Progress

Run this query in BigQuery Console to see real-time progress:

```sql
SELECT
  status,
  COUNT(*) as count,
  ROUND(100.0 * COUNT(*) / 1248, 1) as pct
FROM `brite-nites-data-platform.raw_data.serper_queries`
WHERE job_id = '<paste-your-job-id-here>'
GROUP BY status
ORDER BY status;
```

**What you'll see:**
- `queued`: Queries waiting to be processed
- `processing`: Queries currently being fetched (transient)
- `success`: Queries completed successfully
- `skipped`: Queries skipped due to early exit optimization

Refresh this query every minute to watch progress. When all queries are either `success` or `skipped`, the job is complete.

---

## 4. Verify Completion

Run this query to see final results:

```sql
SELECT
  totals.queries as total_queries,
  totals.successes as successful,
  totals.places as places_found,
  totals.credits as credits_used,
  ROUND(totals.credits * 0.01, 2) as cost_usd
FROM `brite-nites-data-platform.raw_data.serper_jobs`
WHERE job_id = '<paste-your-job-id-here>';
```

**Expected results:**
- `total_queries`: 1248
- `successful`: ~1248 (or less if early exit triggered)
- `places_found`: ~5000-8000 (varies by density)
- `credits_used`: ~800-1248 (depends on early exit optimization)
- `cost_usd`: ~$6.00-$12.48

---

## Troubleshooting

**Batch processor not running?**

Check if the background processor started:
```bash
ps aux | grep process_batches
```

If not running, start it manually:
```bash
python -m src.flows.process_batches
```

**Job stuck?**

Check for errors in the processor log:
```bash
tail -f /tmp/batch_processor_output.log
```

Or check query errors in BigQuery:
```sql
SELECT zip, page, error
FROM `brite-nites-data-platform.raw_data.serper_queries`
WHERE job_id = '<your-job-id>' AND status = 'failed'
LIMIT 10;
```

---

## Next Steps

Once this first job completes successfully:
1. Verify the data quality in `serper_places` table
2. Check that places have valid `place_uid` and `payload` fields
3. Try a smaller state (RI, DE) or different keyword to test variations
4. Scale up to multiple keywords × states for production workloads

**Cost planning:** At $0.01 per credit with ~30-50% early exit savings:
- Small state (RI: 81 zips): ~$1.50
- Medium state (AZ: 416 zips): ~$7.50
- Large state (CA: 2600+ zips): ~$50
- All 50 states (~40,000 zips): ~$800
