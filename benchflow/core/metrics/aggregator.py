"""Metrics aggregation — histogram-based latency computation and cross-iteration statistics."""

from __future__ import annotations

import random

import numpy as np

from benchflow.core.metrics.histogram import HdrHistogram
from benchflow.core.result import (
    AggregateMetric,
    AggregateStepResult,
    AggregateTargetResult,
    ConfidenceInterval,
    LatencySummary,
    StepResult,
    TimeWindow,
)

RESERVOIR_MAX = 10_000


def compute_latency_summary(latencies_ns: list[int]) -> LatencySummary:
    """Compute latency summary from raw latency values using numpy."""
    arr = np.array(latencies_ns, dtype=np.int64)
    return LatencySummary(
        min_ns=int(arr.min()),
        max_ns=int(arr.max()),
        mean_ns=float(arr.mean()),
        stdev_ns=float(arr.std()),
        p50_ns=float(np.percentile(arr, 50)),
        p95_ns=float(np.percentile(arr, 95)),
        p99_ns=float(np.percentile(arr, 99)),
        p999_ns=float(np.percentile(arr, 99.9)),
        p9999_ns=float(np.percentile(arr, 99.99)) if len(arr) >= 10000 else 0.0,
    )


def compute_latency_summary_from_histogram(histogram: HdrHistogram) -> LatencySummary:
    """Compute latency summary from an HdrHistogram (zero-copy, O(buckets))."""
    return LatencySummary(
        min_ns=histogram.min_value,
        max_ns=histogram.max_value,
        mean_ns=histogram.mean(),
        stdev_ns=histogram.stdev(),
        p50_ns=histogram.percentile(50),
        p95_ns=histogram.percentile(95),
        p99_ns=histogram.percentile(99),
        p999_ns=histogram.percentile(99.9),
        p9999_ns=histogram.percentile(99.99) if histogram.total_count >= 10000 else 0.0,
    )


def reservoir_sample(latencies_ns: list[int], max_size: int = RESERVOIR_MAX) -> list[int]:
    if len(latencies_ns) <= max_size:
        return list(latencies_ns)
    return random.sample(latencies_ns, max_size)


def build_step_result(
    name: str,
    latencies_ns: list[int],
    errors: int,
    duration_s: float,
    time_series: list[TimeWindow] | None = None,
) -> StepResult:
    ops = len(latencies_ns)
    throughput = ops / duration_s if duration_s > 0 else 0.0

    return StepResult(
        name=name,
        ops=ops,
        errors=errors,
        latency_summary=compute_latency_summary(latencies_ns),
        throughput_ops_s=round(throughput, 2),
        samples_ns=reservoir_sample(latencies_ns),
        time_series=time_series or [],
    )


def build_step_result_from_histogram(
    name: str,
    histogram: HdrHistogram,
    errors: int,
    duration_s: float,
    samples_ns: list[int] | None = None,
    time_series: list[TimeWindow] | None = None,
) -> StepResult:
    """Build a StepResult from an HdrHistogram instead of raw latencies."""
    ops = histogram.total_count
    throughput = ops / duration_s if duration_s > 0 else 0.0

    return StepResult(
        name=name,
        ops=ops,
        errors=errors,
        latency_summary=compute_latency_summary_from_histogram(histogram),
        throughput_ops_s=round(throughput, 2),
        samples_ns=samples_ns or [],
        time_series=time_series or [],
    )


# ---------------------------------------------------------------------------
# Cross-iteration statistics (bootstrap CI)
# ---------------------------------------------------------------------------


def bootstrap_ci(
    values: list[float],
    confidence: float = 0.95,
    n_resamples: int = 10_000,
    rng: random.Random | None = None,
) -> ConfidenceInterval:
    """Compute bootstrap confidence interval for the mean of values.

    Args:
        values: Observed values (one per iteration).
        confidence: Confidence level (default 0.95).
        n_resamples: Number of bootstrap resamples (default 10,000).
        rng: Optional seeded Random for reproducibility.

    Returns:
        ConfidenceInterval with low, high, confidence.
    """
    if len(values) < 2:
        v = values[0] if values else 0.0
        return ConfidenceInterval(low=v, high=v, confidence=confidence)

    _rng = rng or random.Random()
    n = len(values)
    means: list[float] = []

    for _ in range(n_resamples):
        sample = [values[_rng.randint(0, n - 1)] for _ in range(n)]
        means.append(sum(sample) / n)

    means.sort()
    alpha = 1 - confidence
    low_idx = int(alpha / 2 * n_resamples)
    high_idx = int((1 - alpha / 2) * n_resamples) - 1

    return ConfidenceInterval(
        low=means[low_idx],
        high=means[high_idx],
        confidence=confidence,
    )


def bootstrap_ratio_ci(
    baseline_values: list[float],
    contender_values: list[float],
    confidence: float = 0.95,
    n_resamples: int = 10_000,
    rng: random.Random | None = None,
) -> tuple[ConfidenceInterval, bool]:
    """Compute bootstrap CI for the ratio of means (contender / baseline).

    Returns (ci, significant) where significant is True when the CI excludes 1.0.
    """
    n_b = len(baseline_values)
    n_c = len(contender_values)
    if n_b < 2 or n_c < 2:
        mean_b = sum(baseline_values) / n_b if n_b else 1.0
        mean_c = sum(contender_values) / n_c if n_c else 1.0
        ratio = mean_c / mean_b if mean_b != 0 else 0.0
        ci = ConfidenceInterval(low=ratio, high=ratio, confidence=confidence)
        return ci, False

    _rng = rng or random.Random()
    ratios: list[float] = []

    for _ in range(n_resamples):
        b_sample = [baseline_values[_rng.randint(0, n_b - 1)] for _ in range(n_b)]
        c_sample = [contender_values[_rng.randint(0, n_c - 1)] for _ in range(n_c)]
        mean_b = sum(b_sample) / n_b
        mean_c = sum(c_sample) / n_c
        if mean_b > 0:
            ratios.append(mean_c / mean_b)

    if not ratios:
        ci = ConfidenceInterval(low=0.0, high=0.0, confidence=confidence)
        return ci, False

    ratios.sort()
    alpha = 1 - confidence
    low_idx = int(alpha / 2 * len(ratios))
    high_idx = int((1 - alpha / 2) * len(ratios)) - 1

    ci = ConfidenceInterval(
        low=ratios[low_idx],
        high=ratios[high_idx],
        confidence=confidence,
    )
    significant = ci.low > 1.0 or ci.high < 1.0
    return ci, significant


def compute_aggregate_metric(
    values: list[float],
    confidence: float = 0.95,
    rng: random.Random | None = None,
) -> AggregateMetric:
    """Compute mean, stdev, CV, and bootstrap CI from per-iteration values."""
    if not values:
        return AggregateMetric(
            mean=0.0,
            stdev=0.0,
            cv=0.0,
            ci=ConfidenceInterval(low=0.0, high=0.0, confidence=confidence),
        )

    arr = np.array(values, dtype=np.float64)
    mean_val = float(arr.mean())
    stdev_val = float(arr.std(ddof=1)) if len(arr) > 1 else 0.0
    cv_val = stdev_val / mean_val if mean_val != 0 else 0.0

    ci = bootstrap_ci(values, confidence=confidence, rng=rng)

    return AggregateMetric(mean=mean_val, stdev=stdev_val, cv=cv_val, ci=ci)


def compute_cross_iteration_aggregate(
    iteration_step_results: list[list[StepResult]],
    stack_id: str,
    rng: random.Random | None = None,
) -> AggregateTargetResult:
    """Compute cross-iteration statistics for a target stack.

    Args:
        iteration_step_results: List of step result lists, one per iteration.
            Each inner list contains StepResult objects for that iteration.
        stack_id: The target stack identifier.
        rng: Optional seeded Random for reproducible bootstrap.

    Returns:
        AggregateTargetResult with per-step cross-iteration metrics.
    """
    n_iterations = len(iteration_step_results)
    if n_iterations == 0:
        return AggregateTargetResult(stack_id=stack_id, iterations_completed=0)

    # Collect per-step values across iterations
    step_names: list[str] = []
    step_values: dict[str, dict[str, list[float]]] = {}

    for step_results in iteration_step_results:
        for sr in step_results:
            if sr.name not in step_values:
                step_names.append(sr.name)
                step_values[sr.name] = {
                    "ops": [],
                    "throughput_ops_s": [],
                    "p50_ns": [],
                    "p95_ns": [],
                    "p99_ns": [],
                    "p999_ns": [],
                }
            step_values[sr.name]["ops"].append(float(sr.ops))
            step_values[sr.name]["throughput_ops_s"].append(sr.throughput_ops_s)
            step_values[sr.name]["p50_ns"].append(sr.latency_summary.p50_ns)
            step_values[sr.name]["p95_ns"].append(sr.latency_summary.p95_ns)
            step_values[sr.name]["p99_ns"].append(sr.latency_summary.p99_ns)
            step_values[sr.name]["p999_ns"].append(sr.latency_summary.p999_ns)

    aggregate_steps: list[AggregateStepResult] = []
    for step_name in step_names:
        sv = step_values[step_name]
        aggregate_steps.append(
            AggregateStepResult(
                step_name=step_name,
                ops=compute_aggregate_metric(sv["ops"], rng=rng),
                throughput_ops_s=compute_aggregate_metric(sv["throughput_ops_s"], rng=rng),
                p50_ns=compute_aggregate_metric(sv["p50_ns"], rng=rng),
                p95_ns=compute_aggregate_metric(sv["p95_ns"], rng=rng),
                p99_ns=compute_aggregate_metric(sv["p99_ns"], rng=rng),
                p999_ns=compute_aggregate_metric(sv["p999_ns"], rng=rng),
            )
        )

    return AggregateTargetResult(
        stack_id=stack_id,
        iterations_completed=n_iterations,
        steps=aggregate_steps,
    )
