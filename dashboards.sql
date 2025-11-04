-- ============================================================================
-- Serper Tap Production Dashboards
-- ============================================================================
-- Usage: Replace {{PROJECT}} and {{DATASET}} with your values
-- Example: sed 's/{{PROJECT}}/brite-nites-data-platform/g; s/{{DATASET}}/raw_data/g' dashboards.sql | bq query --use_legacy_sql=false
-- ============================================================================

-- ============================================================================
-- Dashboard 1: Throughput (10-minute rolling window)
-- ============================================================================
-- Purpose: Monitor real-time query processing rate
-- Target: ≥ 300 QPM (queries per minute)
-- Alert: < 250 QPM sustained for 10+ minutes

SELECT
  COUNT(*) AS queries_completed,
  ROUND(60.0 * COUNT(*) / NULLIF(TIMESTAMP_DIFF(MAX(ran_at), MIN(ran_at), SECOND), 0), 1) AS qpm,
  MIN(ran_at) AS window_start,
  MAX(ran_at) AS window_end,
  TIMESTAMP_DIFF(MAX(ran_at), MIN(ran_at), SECOND) AS window_seconds
FROM `{{PROJECT}}.{{DATASET}}.serper_queries`
WHERE ran_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 10 MINUTE)
  AND status = 'success';

-- ============================================================================
-- Dashboard 2: Cost Today (daily budget tracking)
-- ============================================================================
-- Purpose: Track API credit consumption and costs
-- Target: < $100/day default
-- Alert: ≥ 80% of daily budget

SELECT
  COUNT(DISTINCT job_id) AS jobs_today,
  SUM(CAST(totals.credits AS INT64)) AS total_credits,
  ROUND(SUM(CAST(totals.credits AS INT64)) * {{COST_PER_CREDIT}}, 2) AS cost_usd,
  ROUND(SUM(CAST(totals.credits AS INT64)) * {{COST_PER_CREDIT}} / 100.0 * 100, 1) AS pct_of_100_budget
FROM `{{PROJECT}}.{{DATASET}}.serper_jobs`
WHERE DATE(created_at) = CURRENT_DATE();

-- ============================================================================
-- Dashboard 3: JSON Health (parse success rate)
-- ============================================================================
-- Purpose: Monitor SAFE.PARSE_JSON effectiveness and payload_raw fallback
-- Target: > 99.5% parse success rate
-- Alert: < 99% success or > 100 unparsable in last 24h

SELECT
  COUNTIF(payload IS NULL AND payload_raw IS NOT NULL) AS unparsable_raw,
  COUNTIF(payload IS NOT NULL) AS parsed_ok,
  COUNT(*) AS total_places,
  ROUND(100.0 * COUNTIF(payload IS NOT NULL) / COUNT(*), 2) AS parse_success_pct
FROM `{{PROJECT}}.{{DATASET}}.serper_places`
WHERE ingest_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR);

-- ============================================================================
-- Dashboard 4: Job Status Summary
-- ============================================================================
-- Purpose: Overview of all jobs and their completion status
-- Useful for: Identifying stuck or failed jobs

SELECT
  job_id,
  keyword,
  state,
  pages,
  status,
  totals.queries AS total_queries,
  totals.successes AS completed,
  totals.failures AS failed,
  totals.places AS places_found,
  totals.credits AS credits_used,
  ROUND(totals.credits * {{COST_PER_CREDIT}}, 2) AS cost_usd,
  TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), created_at, MINUTE) AS age_minutes,
  created_at,
  finished_at
FROM `{{PROJECT}}.{{DATASET}}.serper_jobs`
WHERE DATE(created_at) >= CURRENT_DATE() - 1
ORDER BY created_at DESC
LIMIT 50;

-- ============================================================================
-- Dashboard 5: Error Rates (API health)
-- ============================================================================
-- Purpose: Monitor Serper API error rates and types
-- Target: 429 rate < 5%, 5xx rate < 1%
-- Alert: High error rates indicate API or rate limiting issues

WITH recent_queries AS (
  SELECT
    api_status,
    COUNT(*) AS count
  FROM `{{PROJECT}}.{{DATASET}}.serper_queries`
  WHERE ran_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
    AND status IN ('success', 'failed')
  GROUP BY api_status
)
SELECT
  COALESCE(api_status, 0) AS status_code,
  count,
  ROUND(100.0 * count / SUM(count) OVER (), 2) AS percentage,
  CASE
    WHEN api_status = 200 THEN '✅ Success'
    WHEN api_status = 429 THEN '⚠️  Rate Limited'
    WHEN api_status BETWEEN 400 AND 499 THEN '❌ Client Error'
    WHEN api_status BETWEEN 500 AND 599 THEN '🔴 Server Error'
    ELSE '❓ Unknown'
  END AS status_category
FROM recent_queries
ORDER BY count DESC;

-- ============================================================================
-- Dashboard 6: Performance Metrics (latency breakdown)
-- ============================================================================
-- Purpose: Identify performance bottlenecks
-- Target: MERGE operations < 6s p50, < 10s p95
-- Note: This requires application logs; use Prefect Cloud UI for detailed timing

SELECT
  job_id,
  keyword,
  state,
  totals.queries,
  totals.places,
  TIMESTAMP_DIFF(finished_at, started_at, SECOND) AS total_runtime_seconds,
  ROUND(totals.queries / NULLIF(TIMESTAMP_DIFF(finished_at, started_at, SECOND), 0) * 60, 1) AS avg_qpm,
  ROUND(CAST(totals.places AS FLOAT64) / NULLIF(CAST(totals.queries AS FLOAT64), 0), 2) AS places_per_query
FROM `{{PROJECT}}.{{DATASET}}.serper_jobs`
WHERE status = 'done'
  AND finished_at IS NOT NULL
  AND DATE(created_at) >= CURRENT_DATE() - 7
ORDER BY finished_at DESC
LIMIT 20;

-- ============================================================================
-- Dashboard 7: Queue Depth (capacity planning)
-- ============================================================================
-- Purpose: Monitor backlog and identify processing bottlenecks
-- Alert: > 10,000 queued queries indicates capacity issues

SELECT
  status,
  COUNT(*) AS query_count,
  ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS percentage
FROM `{{PROJECT}}.{{DATASET}}.serper_queries`
WHERE created_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
GROUP BY status
ORDER BY query_count DESC;

-- ============================================================================
-- Dashboard 8: Idempotency Check (data quality)
-- ============================================================================
-- Purpose: Verify MERGE operations are preventing duplicates
-- Expected: Zero duplicates on (job_id, place_uid)

SELECT
  job_id,
  place_uid,
  COUNT(*) AS duplicate_count
FROM `{{PROJECT}}.{{DATASET}}.serper_places`
WHERE DATE(ingest_ts) >= CURRENT_DATE() - 1
GROUP BY job_id, place_uid
HAVING COUNT(*) > 1
ORDER BY duplicate_count DESC
LIMIT 100;

-- ============================================================================
-- Dashboard 9: Credit Efficiency (ROI analysis)
-- ============================================================================
-- Purpose: Analyze places found per credit spent
-- Useful for: Identifying valuable vs sparse regions

SELECT
  state,
  keyword,
  COUNT(DISTINCT job_id) AS jobs,
  SUM(CAST(totals.queries AS INT64)) AS total_queries,
  SUM(CAST(totals.places AS INT64)) AS total_places,
  SUM(CAST(totals.credits AS INT64)) AS total_credits,
  ROUND(SUM(CAST(totals.places AS INT64)) / NULLIF(SUM(CAST(totals.credits AS INT64)), 0), 2) AS places_per_credit
FROM `{{PROJECT}}.{{DATASET}}.serper_jobs`
WHERE DATE(created_at) >= CURRENT_DATE() - 30
  AND status = 'done'
GROUP BY state, keyword
ORDER BY places_per_credit DESC
LIMIT 50;

-- ============================================================================
-- Dashboard 10: Early Exit Effectiveness (optimization analysis)
-- ============================================================================
-- Purpose: Monitor early exit optimization impact (pages 2-3 skipped when page 1 < 10 results)
-- Savings: Credits saved by not calling unnecessary pages

SELECT
  COUNT(*) AS early_exit_count,
  SUM(CASE WHEN page IN (2, 3) THEN 1 ELSE 0 END) AS pages_skipped,
  ROUND(SUM(CASE WHEN page IN (2, 3) THEN 1 ELSE 0 END) * {{COST_PER_CREDIT}}, 2) AS credits_saved_usd
FROM `{{PROJECT}}.{{DATASET}}.serper_queries`
WHERE status = 'skipped'
  AND error = 'early_exit_page1_lt10'
  AND DATE(ran_at) >= CURRENT_DATE() - 7;
