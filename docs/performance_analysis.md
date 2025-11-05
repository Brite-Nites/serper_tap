# Production Performance Analysis

## Executive Summary

**Date**: October 30, 2025
**Test**: First production-scale run with real Serper API
**Result**: ❌ **FAILED to meet spec targets**

- **Actual throughput**: 48.9 queries/minute
- **Spec target**: 333-500 queries/minute
- **Gap**: **85% below minimum target** (only 15% of target)

## Test Configuration

### Job Parameters
- **Job ID**: `f8f265bb-59ad-4b33-8445-f00fb4e0c2d5`
- **Keyword**: "bars"
- **State**: Arizona (AZ)
- **Total queries**: 1248 (416 zips × 3 pages)
- **Batch configuration**: concurrency=20 (used as batch_size due to aliasing)
- **Processor**: Prefect with ConcurrentTaskRunner(max_workers=100)

### Environment
- **API Mode**: Real Serper API (`USE_MOCK_API=false`)
- **Started**: 2025-10-30 22:01:12 UTC
- **Finished**: 2025-10-30 22:26:25 UTC
- **Runtime**: 1513 seconds (25.2 minutes)

## Results

### Success Metrics ✅
- **Completion**: Job completed successfully
- **Reliability**: 0 failures out of 1232 queries (100% success rate)
- **Early exit**: 16 queries skipped (1.3% savings from optimization)
- **Places found**: 4,175 unique places
- **Cost**: $12.32 (1232 credits × $0.01)

### Performance Metrics ❌
| Metric | Value | Target | Status |
|--------|-------|--------|--------|
| Queries processed | 1,232 | 1,248 | ✅ 98.7% |
| Runtime | 25.2 minutes | ~2-4 minutes | ❌ 6-12x slower |
| Throughput | 48.9 queries/min | 333-500 | ❌ 85% below target |
| Avg time per query | 1.23 seconds | ~0.2 seconds | ❌ 6x slower |
| Avg time per batch | 24.4 seconds | ~2-3 seconds | ❌ 8-12x slower |

## Root Cause Analysis

### Issue #1: Configuration Bug
**Location**: `src/operations/bigquery_ops.py:607`

```python
# Current (INCORRECT):
SELECT concurrency AS batch_size
FROM serper_jobs
```

**Impact**:
- Jobs use `concurrency=20` as `batch_size`
- Should use `batch_size=100` (the intended batch size)
- Processing 20 queries/batch instead of 100

### Issue #2: Ineffective Parallelism
**Evidence**:
- 20 queries/batch with `.map()` should take ~2-3s per batch
- Actual: 24.4s per batch (8x slower)
- Real API latency: ~1s per query
- Expected parallel execution: all 20 queries run simultaneously

**Hypothesis**:
One or more of:
1. Prefect `.map()` not executing queries in parallel
2. BigQuery operations blocking parallelism
3. Resource contention (CPU, network, or database connections)
4. Serper API rate limiting kicking in

### Issue #3: Real API Latency Underestimated
**Discovery**: Real API takes ~5x longer than mock

| Test Type | Per Query | Per 3-Query Batch | Notes |
|-----------|-----------|-------------------|-------|
| Mock API | 0.2s | 0.6s | Simulated with `time.sleep(0.1-0.3)` |
| Real API (small test) | 0.7-1.0s | 7.5s | Includes network + BigQuery overhead |
| Real API (production) | 1.23s | 24.4s (20 queries) | Much worse than expected |

## Performance Breakdown

### Batch Processing Timeline
```
Total batches: 62 (1232 queries ÷ 20/batch)
Total runtime: 1512 seconds
Sleep overhead: 62 seconds (1s between batches)
Net processing: 1450 seconds
Time per batch: 23.4 seconds
```

### Expected vs Actual (per batch of 20 queries)
| Operation | Expected | Actual | Ratio |
|-----------|----------|--------|-------|
| API calls (parallel) | 1-2s | ? | ? |
| BigQuery batch update | 2-3s | ? | ? |
| BigQuery skip pages | 2-3s | ? | ? |
| BigQuery store places | 2-3s | ? | ? |
| BigQuery update stats | 2-3s | ? | ? |
| **Total** | **~10s** | **23.4s** | **2.3x slower** |

## Cost Analysis

### Production Run
- **Queries**: 1,232 successful
- **Credits**: 1,232 × $0.01 = **$12.32**
- **Early exit savings**: 16 queries skipped = $0.16 saved

### Projected Full State (Without Fixes)
- **AZ full (416 zips × 3 pages)**: ~$12.50
- **Runtime**: ~25 minutes per state
- **All 50 states**: ~$625, ~21 hours

### If Performance Target Was Met (333 queries/min)
- **AZ full**: ~$12.50 (same cost)
- **Runtime**: ~3.7 minutes per state
- **All 50 states**: ~$625, ~3 hours

## Validation Status

| Phase | Status | Result |
|-------|--------|--------|
| 1.1: Reference data | ✅ PASS | geo_zip_all table validated |
| 1.2: Real API (3 queries) | ✅ PASS | API works, discovered 5x slowdown vs mock |
| 1.3: Production scale (1248 queries) | ⚠️ PARTIAL | Completes successfully but 85% below target |
| 2: Error recovery | ⏭️  SKIPPED | Blocked on performance fix |
| 3: Operational readiness | ⏭️  SKIPPED | Blocked on performance fix |

## Recommendations

### Immediate Actions (P0)
1. **Fix config aliasing bug**: Add proper `batch_size` column to `serper_jobs` table
2. **Investigate parallelism**: Profile why `.map()` isn't achieving expected parallelism
3. **Benchmark components**: Measure actual time for each operation (API, BigQuery ops)

### Short-term (P1)
4. **Increase batch size**: Test with batch_size=100 (intended value)
5. **Optimize BigQuery operations**: Review if batched operations are truly batched
6. **Test concurrency settings**: Try ConcurrentTaskRunner with different max_workers

### Long-term (P2)
7. **Add performance monitoring**: Log timing for each batch component
8. **Add alerting**: Detect when throughput drops below threshold
9. **Cost optimization**: Tune early exit threshold based on actual data

## Spec Impact

### Claims That Need Revision

**SPECIFICATION.md Line 163-164**:
> Target throughput: 333-500 queries/minute (60-90 seconds for 500 queries)

**Actual**:
- 48.9 queries/minute
- 500 queries would take **~10 minutes** (not 60-90 seconds)

**Recommendation**: Update spec to reflect actual measured performance or mark as "blocked pending performance investigation"

## Next Steps

1. Document these findings ✅ (this file)
2. Update SPECIFICATION.md with realistic expectations
3. Create GitHub issue for performance investigation
4. Decide: Fix performance OR adjust expectations?

## Appendix: Raw Data

### Job Query
```sql
SELECT
  job_id,
  keyword,
  state,
  status,
  created_at,
  finished_at,
  TIMESTAMP_DIFF(finished_at, created_at, SECOND) as runtime_seconds
FROM `brite-nites-internal.serper_tap.serper_jobs`
WHERE job_id = 'f8f265bb-59ad-4b33-8445-f00fb4e0c2d5'
```

### Query Breakdown
```sql
SELECT status, COUNT(*) as count
FROM `brite-nites-internal.serper_tap.serper_queries`
WHERE job_id = 'f8f265bb-59ad-4b33-8445-f00fb4e0c2d5'
GROUP BY status
```

**Results**:
- `success`: 1232
- `skipped`: 16
- Total: 1248
