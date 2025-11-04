-- Migration 001: Add batch_size column to serper_jobs
--
-- Issue: Schema only has 'concurrency' column, but code expects separate 'batch_size'
-- This fixes the aliasing confusion between batch_size (queries per batch)
-- and concurrency (parallel workers)
--
-- Run with:
--   bq query --use_legacy_sql=false < sql/migrations/001_add_batch_size_column.sql

-- Step 1: Add batch_size column
ALTER TABLE `brite-nites-internal.serper_tap.serper_jobs`
ADD COLUMN IF NOT EXISTS batch_size INT64;

-- Step 2: Backfill existing jobs (use concurrency as initial value)
-- This ensures old jobs continue working
UPDATE `brite-nites-internal.serper_tap.serper_jobs`
SET batch_size = concurrency
WHERE batch_size IS NULL;

-- Step 3: Set default for future inserts
ALTER TABLE `brite-nites-internal.serper_tap.serper_jobs`
ALTER COLUMN batch_size SET DEFAULT 100;

-- Verification: Check that all jobs now have batch_size
SELECT
    COUNT(*) as total_jobs,
    COUNT(batch_size) as jobs_with_batch_size,
    COUNT(*) - COUNT(batch_size) as jobs_missing_batch_size
FROM `brite-nites-internal.serper_tap.serper_jobs`;

-- Expected result: jobs_missing_batch_size should be 0
