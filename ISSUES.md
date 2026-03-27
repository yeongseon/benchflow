## Issue 1: Compute ratio CI and significance test in `bench compare`

**Problem**: `ComparisonItem` has `ratio_ci` and `significant` fields, but `bench compare` never populates them. When multi-iteration data is available, the comparison should compute bootstrap CI on the ratio and flag statistically significant differences.

**Current behavior**: `bench compare` computes point-estimate ratios (p50_ratio, p95_ratio, etc.) from single-iteration `targets`. No confidence intervals, no significance determination.

**Expected behavior**: When both RunResults have `aggregate` data (multi-iteration), `bench compare` should:
1. Use per-iteration metrics to compute bootstrap CI for each ratio
2. Set `significant=True` when the CI excludes 1.0
3. Display significance indicators in the Rich table output
4. Show CI range in the comparison output

**Files to modify**:
- `benchflow/core/metrics/aggregator.py` — add `bootstrap_ratio_ci()` function
- `benchflow/cli/main.py` — update `compare()` to populate `ratio_ci` and `significant`
- `benchflow/cli/main.py` — update `_print_comparison()` to show CI and significance

---

## Issue 2: Add CV% reliability warning in `bench run` output

**Problem**: When coefficient of variation (CV%) exceeds 20%, the benchmark results are unreliable. There is no warning to the user.

**Current behavior**: CV is computed and stored in `AggregateMetric.cv`, but no threshold-based warning is displayed.

**Expected behavior**: After `bench run` completes:
1. Check CV% for key metrics (throughput, p50, p95) across iterations
2. If CV% > 20%, display a yellow warning: "⚠ High variance detected (CV=X%) for [metric]. Results may be unreliable. Consider increasing iterations or stabilizing the environment."
3. If CV% > 50%, display a red warning.

**Files to modify**:
- `benchflow/cli/main.py` — add `_check_cv_warnings()` after run completes, call from `_print_summary()`

---

## Issue 3: Add `warmup` method to pycubrid and CUBRIDdb workers

**Problem**: PyCUBRIDWorker and CUBRIDdbWorker don't implement the `warmup` method. The runner calls `worker.warmup()` when warmup config is present, but the default no-op means no actual warmup happens for CUBRID benchmarks.

**Current behavior**: Workers rely on base class no-op `warmup()`.

**Expected behavior**: Workers should execute a configurable number of warmup queries to stabilize connection pools and JIT caches.

**Files to modify**:
- `benchflow/workers/python/pycubrid_worker.py` — add `warmup()` implementation
- `benchflow/workers/python/cubriddb_worker.py` — add `warmup()` implementation
