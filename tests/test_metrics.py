from benchflow.core.metrics.aggregator import (
    build_step_result,
    compute_latency_summary,
    reservoir_sample,
)


def test_compute_latency_summary():
    latencies = list(range(1000, 11000, 100))
    summary = compute_latency_summary(latencies)
    assert summary.min_ns == 1000
    assert summary.max_ns == 10900
    assert summary.p50_ns > 0
    assert summary.p95_ns > summary.p50_ns
    assert summary.p99_ns >= summary.p95_ns


def test_reservoir_sample_small():
    data = [1, 2, 3, 4, 5]
    sampled = reservoir_sample(data, max_size=10)
    assert sampled == [1, 2, 3, 4, 5]


def test_reservoir_sample_large():
    data = list(range(50000))
    sampled = reservoir_sample(data, max_size=10000)
    assert len(sampled) == 10000


def test_build_step_result():
    latencies = [1000000, 2000000, 3000000, 4000000, 5000000]
    result = build_step_result("test-step", latencies, errors=1, duration_s=1.0)
    assert result.name == "test-step"
    assert result.ops == 5
    assert result.errors == 1
    assert result.throughput_ops_s == 5.0
    assert result.latency_summary.min_ns == 1000000
    assert result.latency_summary.max_ns == 5000000
