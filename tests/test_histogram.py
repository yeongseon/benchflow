"""Tests for HdrHistogram — log-bucket histogram with mergeable support."""

from __future__ import annotations

import pytest

from benchflow.core.metrics.histogram import HdrHistogram


class TestHdrHistogramBasic:
    def test_empty_histogram(self):
        h = HdrHistogram()
        assert h.total_count == 0
        assert h.min_value == 0
        assert h.max_value == 0
        assert h.mean() == 0.0
        assert h.stdev() == 0.0
        assert h.percentile(50) == 0.0

    def test_single_value(self):
        h = HdrHistogram()
        h.record(1000)
        assert h.total_count == 1
        assert h.min_value == 1000
        assert h.max_value == 1000
        assert h.percentile(50) == pytest.approx(1000, rel=0.01)

    def test_multiple_values(self):
        h = HdrHistogram()
        for v in range(1, 1001):
            h.record(v)
        assert h.total_count == 1000
        assert h.min_value == 1
        assert h.max_value == 1000
        # p50 should be around 500
        assert 450 <= h.percentile(50) <= 550

    def test_percentiles_ordering(self):
        h = HdrHistogram()
        for v in range(1, 10001):
            h.record(v * 1000)
        p50 = h.percentile(50)
        p95 = h.percentile(95)
        p99 = h.percentile(99)
        p999 = h.percentile(99.9)
        assert p50 < p95 < p99 < p999

    def test_extended_percentiles(self):
        """Test p99.9 and p99.99 with large dataset."""
        h = HdrHistogram()
        for v in range(1, 100001):
            h.record(v)
        p999 = h.percentile(99.9)
        p9999 = h.percentile(99.99)
        assert p999 > h.percentile(99)
        assert p9999 >= p999

    def test_record_n(self):
        h = HdrHistogram()
        h.record_n(5000, 100)
        assert h.total_count == 100
        assert h.min_value == 5000
        assert h.max_value == 5000

    def test_negative_value_raises(self):
        h = HdrHistogram()
        with pytest.raises(ValueError, match="negative"):
            h.record(-1)


class TestHdrHistogramMerge:
    def test_merge_basic(self):
        h1 = HdrHistogram()
        h2 = HdrHistogram()
        for v in range(1, 501):
            h1.record(v)
        for v in range(501, 1001):
            h2.record(v)
        h1.merge(h2)
        assert h1.total_count == 1000
        assert h1.min_value == 1
        assert h1.max_value == 1000

    def test_merge_empty_into_populated(self):
        h1 = HdrHistogram()
        h2 = HdrHistogram()
        h1.record(100)
        h1.merge(h2)
        assert h1.total_count == 1
        assert h1.min_value == 100

    def test_merge_populated_into_empty(self):
        h1 = HdrHistogram()
        h2 = HdrHistogram()
        h2.record(200)
        h1.merge(h2)
        assert h1.total_count == 1
        assert h1.min_value == 200

    def test_merge_preserves_min_max(self):
        h1 = HdrHistogram()
        h2 = HdrHistogram()
        h1.record(100)
        h1.record(200)
        h2.record(50)
        h2.record(300)
        h1.merge(h2)
        assert h1.min_value == 50
        assert h1.max_value == 300

    def test_merge_different_geometry_raises(self):
        h1 = HdrHistogram(significant_digits=3)
        h2 = HdrHistogram(significant_digits=2)
        h2.record(100)
        with pytest.raises(ValueError, match="geometry"):
            h1.merge(h2)


class TestHdrHistogramECDF:
    def test_ecdf_empty(self):
        h = HdrHistogram()
        values, percentiles = h.to_ecdf()
        assert values == []
        assert percentiles == []

    def test_ecdf_returns_sorted_data(self):
        h = HdrHistogram()
        for v in [100, 200, 300, 400, 500]:
            h.record(v)
        values, percentiles = h.to_ecdf()
        assert len(values) > 0
        assert len(values) == len(percentiles)
        # Percentiles should be monotonically increasing
        for i in range(1, len(percentiles)):
            assert percentiles[i] >= percentiles[i - 1]
        # Last percentile should be 1.0
        assert percentiles[-1] == pytest.approx(1.0)

    def test_ecdf_values_ascending(self):
        h = HdrHistogram()
        for v in range(1, 101):
            h.record(v * 1000)
        values, _ = h.to_ecdf()
        for i in range(1, len(values)):
            assert values[i] >= values[i - 1]


class TestHdrHistogramCopy:
    def test_copy_independence(self):
        h1 = HdrHistogram()
        h1.record(100)
        h1.record(200)
        h2 = h1.copy()
        h2.record(300)
        assert h1.total_count == 2
        assert h2.total_count == 3

    def test_reset(self):
        h = HdrHistogram()
        for v in range(1, 101):
            h.record(v)
        h.reset()
        assert h.total_count == 0
        assert h.min_value == 0
        assert h.max_value == 0


class TestHdrHistogramStatistics:
    def test_mean_uniform(self):
        h = HdrHistogram()
        for v in range(1000, 2001):
            h.record(v)
        # Mean should be approximately 1500
        assert 1400 <= h.mean() <= 1600

    def test_stdev_constant(self):
        h = HdrHistogram()
        for _ in range(100):
            h.record(1000)
        # All same value → stdev should be near 0
        assert h.stdev() < 10  # Allow some quantization error

    def test_stdev_single_value(self):
        h = HdrHistogram()
        h.record(1000)
        assert h.stdev() == 0.0
