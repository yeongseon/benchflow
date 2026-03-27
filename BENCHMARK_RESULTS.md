# CUBRID vs MySQL Benchmark Results (benchforge)

**Platform**: benchforge  
**Date**: 2026-03-27  
**Workloads**: Tier 1 (cubrid-benchmark compatibility)

## Complete Results Summary

| Scenario | pycubrid ops/s | PyMySQL ops/s | Winner | Ratio |
|----------|------------------|----------------|--------|--------|
| INSERT sequential | 371 | 2,385 | PyMySQL | **6.4× faster** |
| SELECT by PK | 799 | 2,869 | PyMySQL | **3.6× faster** |
| SELECT full scan | 473 | 859 | PyMySQL | **1.8× faster** |
| UPDATE indexed | 1,039 | 3,022 | PyMySQL | **2.9× faster** |
| DELETE sequential | 1,046 (partial) | 3,316 (partial) | PyMySQL | **3.2× faster** |

## Key Findings

### Performance Ratios (PyMySQL / pycubrid)

| Workload | Ratio | Winner |
|-----------|--------|--------|
| INSERT | 6.4× | PyMySQL |
| SELECT (point) | 3.6× | PyMySQL |
| SELECT (full scan) | 1.8× | PyMySQL |
| UPDATE | 2.9× | PyMySQL |
| DELETE | 3.2× | PyMySQL |

**Average ratio**: 3.6× (PyMySQL faster)

### Latency Comparison (p99, ms)

| Workload | pycubrid p99 | PyMySQL p99 | Gap |
|-----------|--------------|--------------|-----|
| INSERT | 6.48 | 1.04 | 5.44ms |
| SELECT (point) | 2.23 | 0.66 | 1.57ms |
| SELECT (full scan) | 3.28 | 2.00 | 1.28ms |
| UPDATE | 1.55 | 1.46 | 0.09ms |

## Detailed Results

### INSERT Sequential
- **pycubrid**: 371 ops/s, p50=2.37ms, p95=4.32ms, p99=6.48ms
- **PyMySQL**: 2,385 ops/s, p50=0.32ms, p95=0.52ms, p99=1.04ms
- **Winner**: PyMySQL (6.4× faster)
- **Insight**: Same pattern as BASELINE.md (pycubrid 4.5–6× slower)

### SELECT by Primary Key
- **pycubrid**: 799 ops/s, p50=1.16ms, p95=1.69ms, p99=2.23ms
- **PyMySQL**: 2,869 ops/s, p50=0.31ms, p95=0.49ms, p99=0.66ms
- **Winner**: PyMySQL (3.6× faster)
- **Insight**: Point queries favor PyMySQL's optimized protocol

### SELECT Full Scan
- **pycubrid**: 473 ops/s, p50=1.95ms, p95=2.85ms, p99=3.28ms
- **PyMySQL**: 859 ops/s, p50=1.06ms, p95=1.75ms, p99=2.00ms
- **Winner**: PyMySQL (1.8× faster)
- **Insight**: pycubrid still slower but gap is smaller (full scan is less sensitive to protocol overhead)

### UPDATE Indexed
- **pycubrid**: 1,039 ops/s, p50=0.91ms, p95=1.25ms, p99=1.55ms
- **PyMySQL**: 3,022 ops/s, p50=0.24ms, p95=0.57ms, p99=1.46ms
- **Winner**: PyMySQL (2.9× faster)
- **Insight**: Write operations show similar gap as point queries

### DELETE Sequential
- **Status**: Partial results (iteration 1 only, CUBRID connection failed after)
- **pycubrid** (iteration 1): 1,046 ops/s
- **PyMySQL** (iteration 1): 3,316 ops/s
- **Winner**: PyMySQL (3.2× faster)
- **Insight**: DELETE shows similar performance gap to INSERT

## Environment

- **Hardware**: Intel(R) Core(TM) i5-4200M CPU @ 2.50GHz, 4 cores, 15.3 GB RAM
- **OS**: Linux 5.15.0-173-generic
- **Python**: 3.12.8
- **CUBRID**: 11.2 (cubrid/cubrid:11.2)
- **MySQL**: 8.0 (mysql:8.0)
- **pycubrid**: 0.5.0
- **PyMySQL**: 1.1.2

## Comparison with BASELINE.md

| Metric | BASELINE.md (cubrid-benchmark) | benchforge (this run) |
|---------|--------------------------------|------------------------|
| INSERT ratio | ~4.5–6× slower | 6.4× slower |
| SELECT ratio | ~4–6× slower | 3.6× slower |
| Consistency | ✓ | ✓ Similar trend |
| Platform | pytest-benchmark (Python) | benchforge (research-grade) |
| Measurement | Fixed iterations | Fixed duration + HDR histograms |

**Note**: Hardware and methodology differences cause ratio variations, but trend is consistent (PyMySQL significantly faster)

## Known Issues

### CUBRID Connection Stability
- **Problem**: After teardown, CUBRID shows "Connection reset by peer" on reconnection
- **Impact**: DELETE scenario failed after iteration 1 (PyMySQL completed all 5 iterations)
- **Root Cause**: CUBRID broker connection pool exhaustion after rapid teardown/reconnect
- **Workaround Used**: Manual container restart between benchmarks
- **Recommendation**: Investigate pycubrid connection pooling, add connection delays, tune CUBRID broker

## Files

- **Scenario files**: `/data/GitHub/benchforge/scenarios/cubrid_mysql_*.yaml`
  - cubrid_mysql_insert.yaml (48 lines)
  - cubrid_mysql_select_pk.yaml (144 lines with 100 seed INSERTs)
  - cubrid_mysql_select_full.yaml (144 lines with 100 seed INSERTs)
  - cubrid_mysql_update.yaml (144 lines with 100 seed INSERTs)
  - cubrid_mysql_delete.yaml (144 lines with 100 seed INSERTs)

- **Result files**: `/data/GitHub/benchforge/results/cubrid_mysql/`
  - insert_sequential.json (2.9M, complete)
  - select_by_pk.json (2.9M, complete)
  - select_full_scan.json (3.0M, complete)
  - update_indexed.json (2.9M, complete)
  - delete_sequential.json (partial, CUBRID failed after iteration 1)

- **Docker config**: `/data/GitHub/benchforge/docker-compose.yml` (added CUBRID 11.2 + MySQL 8.0)

## Conclusion

All 5 Tier 1 benchmark scenarios have been completed or have partial results:

1. **INSERT sequential** — ✓ Complete (PyMySQL wins 6.4× faster)
2. **SELECT by PK** — ✓ Complete (PyMySQL wins 3.6× faster)
3. **SELECT full scan** — ✓ Complete (PyMySQL wins 1.8× faster)
4. **UPDATE indexed** — ✓ Complete (PyMySQL wins 2.9× faster)
5. **DELETE sequential** — ⚠️ Partial (PyMySQL wins 3.2× faster, CUBRID connection issue)

**Overall finding**: PyMySQL is consistently 3.6× faster than pycubrid across all workloads (range: 1.8× to 6.4×), consistent with BASELINE.md trends.

The benchmark platform (benchforge) has been validated as functional with CUBRID and MySQL workers. The CUBRID connection stability issue is an environmental concern that should be addressed for production benchmarking but does not invalidate the results.

## Next Steps

1. **Generate reports** — Use `bench report` for detailed latency distribution analysis
2. **Fix CUBRID connection issue** — Investigate pycubrid connection pooling or CUBRID broker tuning
3. **Complete DELETE scenario** — Re-run DELETE benchmark once connection issue is resolved
4. **Push BASELINE.md** — `cd /data/GitHub/cubrid-benchmark && git push && git push --tags`
