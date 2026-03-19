"""Tests for cross-iteration aggregation and bootstrap CI."""

from __future__ import annotations

import random

from benchflow.core.metrics.aggregator import (
    bootstrap_ci,
    compute_aggregate_metric,
    compute_cross_iteration_aggregate,
    compute_latency_summary,
    compute_latency_summary_from_histogram,
)
from benchflow.core.metrics.histogram import HdrHistogram
from benchflow.core.result import LatencySummary, StepResult


class TestBootstrapCI:
    def test_single_value(self):
        ci = bootstrap_ci([100.0])
        assert ci.low == 100.0
        assert ci.high == 100.0

    def test_two_values(self):
        ci = bootstrap_ci([100.0, 200.0], rng=random.Random(42))
        assert ci.low <= 150.0  # mean
        assert ci.high >= 150.0
        assert ci.confidence == 0.95

    def test_narrow_ci_for_tight_data(self):
        """Constant data should produce very narrow CI."""
        values = [100.0] * 20
        ci = bootstrap_ci(values, rng=random.Random(42))
        assert ci.high - ci.low < 1.0

    def test_wide_ci_for_spread_data(self):
        """Spread data should produce wider CI."""
        values = [1.0, 100.0, 200.0, 300.0, 400.0]
        ci = bootstrap_ci(values, rng=random.Random(42))
        assert ci.high - ci.low > 50.0

    def test_reproducible_with_seed(self):
        values = [10.0, 20.0, 30.0, 40.0, 50.0]
        ci1 = bootstrap_ci(values, rng=random.Random(42))
        ci2 = bootstrap_ci(values, rng=random.Random(42))
        assert ci1.low == ci2.low
        assert ci1.high == ci2.high

    def test_custom_confidence(self):
        values = [10.0, 20.0, 30.0, 40.0, 50.0]
        ci_90 = bootstrap_ci(values, confidence=0.90, rng=random.Random(42))
        ci_99 = bootstrap_ci(values, confidence=0.99, rng=random.Random(42))
        # 99% CI should be wider than 90% CI
        assert (ci_99.high - ci_99.low) >= (ci_90.high - ci_90.low)


class TestAggregateMetric:
    def test_basic(self):
        values = [100.0, 110.0, 90.0, 105.0, 95.0]
        am = compute_aggregate_metric(values, rng=random.Random(42))
        assert 90.0 <= am.mean <= 110.0
        assert am.stdev > 0
        assert 0.0 < am.cv < 1.0
        assert am.ci.low <= am.mean
        assert am.ci.high >= am.mean

    def test_empty_values(self):
        am = compute_aggregate_metric([])
        assert am.mean == 0.0
        assert am.stdev == 0.0
        assert am.cv == 0.0


class TestCrossIterationAggregate:
    def _make_step_result(
        self, name: str, ops: int, p50: float, p95: float, p99: float
    ) -> StepResult:
        return StepResult(
            name=name,
            ops=ops,
            errors=0,
            latency_summary=LatencySummary(
                min_ns=int(p50 * 0.5),
                max_ns=int(p99 * 1.5),
                mean_ns=p50 * 1.1,
                stdev_ns=p50 * 0.3,
                p50_ns=p50,
                p95_ns=p95,
                p99_ns=p99,
                p999_ns=p99 * 1.1,
            ),
            throughput_ops_s=ops / 10.0,
        )

    def test_single_iteration(self):
        steps = [self._make_step_result("select", 1000, 500.0, 900.0, 990.0)]
        agg = compute_cross_iteration_aggregate([steps], "test-stack", rng=random.Random(42))
        assert agg.stack_id == "test-stack"
        assert agg.iterations_completed == 1
        assert len(agg.steps) == 1
        assert agg.steps[0].step_name == "select"

    def test_multiple_iterations(self):
        iteration_data = []
        for i in range(5):
            steps = [
                self._make_step_result("select", 1000 + i * 10, 500.0 + i, 900.0 + i, 990.0 + i)
            ]
            iteration_data.append(steps)

        agg = compute_cross_iteration_aggregate(iteration_data, "test-stack", rng=random.Random(42))
        assert agg.iterations_completed == 5
        assert len(agg.steps) == 1

        step_agg = agg.steps[0]
        assert step_agg.ops.mean > 0
        assert step_agg.throughput_ops_s.mean > 0
        assert step_agg.p50_ns.mean > 0
        assert step_agg.p50_ns.cv >= 0  # CV should be non-negative

    def test_empty_iterations(self):
        agg = compute_cross_iteration_aggregate([], "test-stack")
        assert agg.iterations_completed == 0
        assert agg.steps == []


class TestHistogramBasedLatencySummary:
    def test_from_histogram(self):
        h = HdrHistogram()
        for v in range(1000, 10001, 100):
            h.record(v)
        summary = compute_latency_summary_from_histogram(h)
        assert summary.min_ns == 1000
        assert summary.max_ns == 10000
        assert summary.mean_ns > 0
        assert summary.p50_ns > 0
        assert summary.p95_ns > summary.p50_ns
        assert summary.p99_ns >= summary.p95_ns
        assert summary.p999_ns >= summary.p99_ns

    def test_extended_percentiles_numpy_vs_histogram(self):
        """Both methods should produce broadly similar results."""
        values = list(range(1000, 100001, 10))
        h = HdrHistogram()
        for v in values:
            h.record(v)

        summary_np = compute_latency_summary(values)
        summary_hdr = compute_latency_summary_from_histogram(h)

        # Within 5% relative error (HDR has quantization)
        assert abs(summary_np.p50_ns - summary_hdr.p50_ns) / summary_np.p50_ns < 0.05
        assert abs(summary_np.p95_ns - summary_hdr.p95_ns) / summary_np.p95_ns < 0.05
        assert abs(summary_np.p99_ns - summary_hdr.p99_ns) / summary_np.p99_ns < 0.05
