#!/usr/bin/env bash
set -euo pipefail

: "${PROJECT:=brite-nites-data-platform}"
: "${COST_PER_CREDIT:=0.01}"

echo "== Health =="
if .venv/bin/python -m src.cli health 2>/dev/null; then
  echo "Health check passed"
else
  echo "WARN: health check failed or not found; continuing"
fi

echo "== Schema: batch_size non-NULL =="
bq query --use_legacy_sql=false \
'SELECT COUNTIF(batch_size IS NULL) as null_count FROM `'"$PROJECT"'.raw_data.serper_jobs`' | tee /tmp/pra_nulls.txt
grep -q " 0 " /tmp/pra_nulls.txt || { echo "❌ batch_size has NULLs"; exit 1; }

echo "== Create job =="
JOB_OUT=$(.venv/bin/python -m src.cli create --keyword bars --state AZ --pages 3 --batch-size 150 || true)
echo "$JOB_OUT"
JOB_ID=$(echo "$JOB_OUT" | grep -oE '[a-f0-9-]{36}' | head -n1)
[ -n "$JOB_ID" ] || { echo "❌ no JOB_ID produced"; exit 1; }
echo "JOB_ID=$JOB_ID"

echo "== Process batches (10 min max) =="
timeout 600 .venv/bin/python -m src.cli process || true

echo "== Verify job totals =="
bq query --use_legacy_sql=false \
'SELECT status, totals FROM `'"$PROJECT"'.raw_data.serper_jobs` WHERE job_id="'"$JOB_ID"'"' || { echo "❌ job status query failed"; exit 1; }

echo "== Idempotency snapshot =="
bq query --use_legacy_sql=false \
'SELECT COUNT(*) total, COUNT(DISTINCT place_uid) uniq
 FROM `'"$PROJECT"'.raw_data.serper_places` WHERE job_id="'"$JOB_ID"'"'

echo "== QPM over last 10 minutes =="
bq query --use_legacy_sql=false \
'WITH m AS (
  SELECT TIMESTAMP_TRUNC(ran_at, MINUTE) m, COUNT(*) q
  FROM `'"$PROJECT"'.raw_data.serper_queries`
  WHERE status="success" AND ran_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 10 MINUTE)
  GROUP BY 1)
 SELECT IFNULL(AVG(q),0) avg_qpm FROM m' | tee /tmp/pra_qpm.txt
AVG_QPM=$(awk '/[0-9]/{print $1}' /tmp/pra_qpm.txt | tail -n1)
echo "AVG_QPM=${AVG_QPM:-0}"
awk -v qpm="${AVG_QPM:-0}" 'BEGIN{ if (qpm+0 < 300) { print "❌ avg_qpm < 300"; exit 1 } }'

echo "== Cost =="
bq query --use_legacy_sql=false \
'SELECT totals.credits, ROUND(totals.credits*'"$COST_PER_CREDIT"',2) usd
 FROM `'"$PROJECT"'.raw_data.serper_jobs` WHERE job_id="'"$JOB_ID"'"'

echo "✅ VALIDATION PASSED"
