-- Serper Scraping Pipeline - BigQuery Schema
-- This file contains DDL for all tables used in the pipeline
-- Run these statements in your BigQuery console to create the schema

-- ============================================================================
-- Table: raw_data.serper_jobs
-- Purpose: Job-level metadata and rollup statistics
-- ============================================================================

CREATE TABLE IF NOT EXISTS `raw_data.serper_jobs` (
  job_id STRING NOT NULL,
  keyword STRING NOT NULL,
  state STRING NOT NULL,
  pages INT64 NOT NULL,
  dry_run BOOL NOT NULL DEFAULT FALSE,
  concurrency INT64 NOT NULL DEFAULT 20,
  status STRING NOT NULL,  -- 'running' | 'done' | 'failed'
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP(),
  started_at TIMESTAMP,
  finished_at TIMESTAMP,
  totals STRUCT<
    zips INT64,
    queries INT64,
    successes INT64,
    failures INT64,
    places INT64,
    credits INT64
  >
)
OPTIONS(
  description = "Job-level metadata and rollup statistics for Serper scraping jobs"
);


-- ============================================================================
-- Table: raw_data.serper_queries
-- Purpose: Per-query audit trail and queue management
-- Primary Key: (job_id, zip, page) ensures idempotent query creation
-- ============================================================================

CREATE TABLE IF NOT EXISTS `raw_data.serper_queries` (
  job_id STRING NOT NULL,
  zip STRING NOT NULL,
  page INT64 NOT NULL,
  q STRING NOT NULL,  -- Query text sent to Serper (e.g., "85001 bars")
  status STRING NOT NULL,  -- 'queued' | 'processing' | 'success' | 'failed' | 'skipped'
  claim_id STRING,  -- For atomic batch claiming pattern
  claimed_at TIMESTAMP,
  api_status INT64,
  results_count INT64,
  credits INT64,
  error STRING,
  ran_at TIMESTAMP
)
PARTITION BY DATE(ran_at)
CLUSTER BY job_id, status
OPTIONS(
  description = "Per-query audit trail and queue management. Status tracks query lifecycle."
);

-- Note: BigQuery doesn't enforce PRIMARY KEY constraints, but we'll handle
-- uniqueness via MERGE operations in application code on (job_id, zip, page)


-- ============================================================================
-- Table: raw_data.serper_places
-- Purpose: Raw place data from Serper API
-- Unique Key: (job_id, place_uid) prevents duplicate places per job
-- ============================================================================

CREATE TABLE IF NOT EXISTS `raw_data.serper_places` (
  ingest_id STRING NOT NULL,
  job_id STRING NOT NULL,
  source STRING NOT NULL DEFAULT 'serper_places',
  source_version STRING NOT NULL DEFAULT 'v1',
  ingest_ts TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP(),
  keyword STRING NOT NULL,
  state STRING NOT NULL,
  zip STRING NOT NULL,
  page INT64 NOT NULL,
  place_uid STRING NOT NULL,  -- placeId or cid from Serper
  payload JSON NOT NULL,  -- Full Serper response for this place
  api_status INT64,
  api_ms INT64,
  results_count INT64,
  credits INT64,
  error STRING
)
PARTITION BY DATE(ingest_ts)
CLUSTER BY job_id, state, keyword
OPTIONS(
  description = "Raw place data from Serper API. Each row represents one place from search results."
);

-- Note: Application code will use MERGE on (job_id, place_uid) to prevent duplicates


-- ============================================================================
-- Reference Table: reference.geo_zip_all
-- Purpose: Assumed to exist - contains zip code reference data
-- ============================================================================

-- This table is assumed to already exist in your BigQuery instance
-- Expected schema:
--   zip STRING
--   state STRING
--   ... other geo fields

-- If you need to create this table, you can populate it from a public dataset
-- or your own zip code reference data.
