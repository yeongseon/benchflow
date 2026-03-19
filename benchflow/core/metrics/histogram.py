"""HDR-style log-bucket histogram for mergeable latency recording.

Provides O(1) record, O(1) merge, and accurate percentile computation
with configurable relative error. No external dependencies beyond math.

The histogram uses logarithmic sub-buckets: values are grouped into
exponentially-spaced major buckets (powers of 2), each subdivided into
`2^sub_bucket_bits` linear sub-buckets. This gives a worst-case relative
error of `1 / (2^sub_bucket_bits)` — e.g., 3 sub-bucket bits → 12.5%
relative error, which is tighter than most benchmark noise.

Design decisions (per Oracle recommendation):
- In-house implementation, zero new dependencies
- Mergeable across threads and iterations
- ECDF extraction for publication-quality plots
- Extended percentiles: p99.9, p99.99
"""

from __future__ import annotations

import math
from typing import Iterator


class HdrHistogram:
    """Log-linear histogram with configurable precision.

    Args:
        lowest_value: Minimum trackable value (inclusive). Must be >= 1.
        highest_value: Maximum trackable value (inclusive).
        significant_digits: Number of significant decimal digits of precision (1-5).
            Controls sub-bucket count: 2^(ceil(log2(10^digits))) sub-buckets per
            major bucket. Higher = more memory, lower relative error.
    """

    __slots__ = (
        "_lowest_value",
        "_highest_value",
        "_significant_digits",
        "_sub_bucket_count",
        "_sub_bucket_half_count",
        "_sub_bucket_bits",
        "_sub_bucket_half_count_magnitude",
        "_sub_bucket_mask",
        "_unit_magnitude",
        "_bucket_count",
        "_counts_len",
        "_counts",
        "_total_count",
        "_min_value",
        "_max_value",
    )

    def __init__(
        self,
        lowest_value: int = 1,
        highest_value: int = 3_600_000_000_000,  # 1 hour in ns
        significant_digits: int = 3,
    ) -> None:
        if lowest_value < 1:
            raise ValueError("lowest_value must be >= 1")
        if significant_digits < 1 or significant_digits > 5:
            raise ValueError("significant_digits must be 1-5")
        if highest_value < 2 * lowest_value:
            raise ValueError("highest_value must be >= 2 * lowest_value")

        self._lowest_value = lowest_value
        self._highest_value = highest_value
        self._significant_digits = significant_digits

        # Sub-bucket geometry
        largest_value_with_single_unit_resolution = 2 * (10 ** significant_digits)
        self._sub_bucket_bits = int(
            math.ceil(math.log2(largest_value_with_single_unit_resolution))
        )
        self._sub_bucket_count = 1 << self._sub_bucket_bits
        self._sub_bucket_half_count = self._sub_bucket_count >> 1
        self._sub_bucket_half_count_magnitude = self._sub_bucket_bits - 1
        self._sub_bucket_mask = (self._sub_bucket_count - 1) << self._unit_magnitude_calc(
            lowest_value
        )

        self._unit_magnitude = self._unit_magnitude_calc(lowest_value)

        # Bucket count
        smallest_untrackable = self._sub_bucket_count << self._unit_magnitude
        buckets_needed = 1
        while smallest_untrackable <= highest_value:
            smallest_untrackable <<= 1
            buckets_needed += 1
        self._bucket_count = buckets_needed

        # Flat array of counts
        self._counts_len = (self._bucket_count + 1) * self._sub_bucket_half_count
        self._counts: list[int] = [0] * self._counts_len
        self._total_count = 0
        self._min_value = 0
        self._max_value = 0

    @staticmethod
    def _unit_magnitude_calc(lowest_value: int) -> int:
        return max(0, int(math.floor(math.log2(lowest_value))))

    def _counts_index(self, bucket: int, sub_bucket: int) -> int:
        return (bucket + 1) * self._sub_bucket_half_count + sub_bucket - self._sub_bucket_half_count

    def _get_bucket_index(self, value: int) -> int:
        pow2ceiling = 64 - self._leading_zeros_64(value | self._sub_bucket_mask)
        return max(0, pow2ceiling - self._unit_magnitude - self._sub_bucket_bits)

    def _get_sub_bucket_index(self, value: int, bucket: int) -> int:
        return value >> (bucket + self._unit_magnitude)

    @staticmethod
    def _leading_zeros_64(value: int) -> int:
        if value <= 0:
            return 64 if value == 0 else 0
        n = 63
        if value >= 1 << 32:
            n -= 32
            value >>= 32
        if value >= 1 << 16:
            n -= 16
            value >>= 16
        if value >= 1 << 8:
            n -= 8
            value >>= 8
        if value >= 1 << 4:
            n -= 4
            value >>= 4
        if value >= 1 << 2:
            n -= 2
            value >>= 2
        if value >= 1 << 1:
            n -= 1
        return n

    def _value_from_index(self, bucket: int, sub_bucket: int) -> int:
        return sub_bucket << (bucket + self._unit_magnitude)

    def _value_from_linear_index(self, index: int) -> int:
        """Convert a flat counts-array index back to the value it represents.

        This reverses the _counts_index(bucket, sub_bucket) mapping.
        In the canonical HDR layout:
          - Indices 0..sub_bucket_half_count-1 are the lower half of bucket 0
            (sub_bucket = index, bucket = 0).
          - Remaining indices map to (bucket >= 0, sub_bucket >= sub_bucket_half_count).
        """
        bucket_index = (index >> self._sub_bucket_half_count_magnitude) - 1
        sub_bucket_index = (index & (self._sub_bucket_half_count - 1)) + self._sub_bucket_half_count
        if bucket_index < 0:
            sub_bucket_index -= self._sub_bucket_half_count
            bucket_index = 0
        return sub_bucket_index << (bucket_index + self._unit_magnitude)

    def _highest_equivalent(self, value: int) -> int:
        bucket = self._get_bucket_index(value)
        sub_bucket = self._get_sub_bucket_index(value, bucket)
        # Next value at current resolution minus 1
        step = 1 << (bucket + self._unit_magnitude)
        return self._value_from_index(bucket, sub_bucket) + step - 1

    # ----- Public API -----

    @property
    def total_count(self) -> int:
        return self._total_count

    @property
    def min_value(self) -> int:
        return self._min_value if self._total_count > 0 else 0

    @property
    def max_value(self) -> int:
        return self._max_value

    def record(self, value: int) -> None:
        """Record a single value into the histogram."""
        if value < 0:
            raise ValueError(f"Cannot record negative value: {value}")

        bucket = self._get_bucket_index(value)
        sub_bucket = self._get_sub_bucket_index(value, bucket)
        idx = self._counts_index(bucket, sub_bucket)

        if idx < 0 or idx >= self._counts_len:
            raise ValueError(
                f"Value {value} out of range for histogram "
                f"[{self._lowest_value}, {self._highest_value}]"
            )

        self._counts[idx] += 1
        self._total_count += 1

        if self._total_count == 1:
            self._min_value = value
            self._max_value = value
        else:
            if value < self._min_value:
                self._min_value = value
            if value > self._max_value:
                self._max_value = value

    def record_n(self, value: int, count: int) -> None:
        """Record a value with a count (for batch recording)."""
        if count <= 0:
            return
        bucket = self._get_bucket_index(value)
        sub_bucket = self._get_sub_bucket_index(value, bucket)
        idx = self._counts_index(bucket, sub_bucket)

        if idx < 0 or idx >= self._counts_len:
            raise ValueError(
                f"Value {value} out of range for histogram "
                f"[{self._lowest_value}, {self._highest_value}]"
            )

        self._counts[idx] += count
        self._total_count += count

        if self._total_count == count:
            self._min_value = value
            self._max_value = value
        else:
            if value < self._min_value:
                self._min_value = value
            if value > self._max_value:
                self._max_value = value

    def merge(self, other: HdrHistogram) -> None:
        """Merge another histogram into this one.

        Both histograms must have the same geometry (lowest, highest,
        significant_digits). This is the primary mechanism for combining
        per-thread and per-iteration data.
        """
        if other._total_count == 0:
            return

        if (
            self._sub_bucket_count != other._sub_bucket_count
            or self._unit_magnitude != other._unit_magnitude
            or self._counts_len != other._counts_len
        ):
            raise ValueError(
                "Cannot merge histograms with different geometry. "
                "Ensure both use the same lowest_value, highest_value, "
                "and significant_digits."
            )

        for i in range(other._counts_len):
            if other._counts[i] > 0:
                self._counts[i] += other._counts[i]

        self._total_count += other._total_count

        if self._total_count == other._total_count:
            # We were empty before
            self._min_value = other._min_value
            self._max_value = other._max_value
        else:
            if other._min_value < self._min_value:
                self._min_value = other._min_value
            if other._max_value > self._max_value:
                self._max_value = other._max_value

    def percentile(self, q: float) -> float:
        """Return the value at the given percentile (0-100).

        Uses the smallest recorded value whose cumulative count reaches
        the target fraction of total_count.
        """
        if self._total_count == 0:
            return 0.0

        target_count = max(1, int(math.ceil((q / 100.0) * self._total_count)))
        running = 0

        for idx in range(self._counts_len):
            running += self._counts[idx]
            if running >= target_count:
                return float(self._value_from_linear_index(idx))

        return float(self._max_value)

    def mean(self) -> float:
        """Compute the estimated mean value.

        Each bucket's contribution uses the midpoint of its value range
        (value to highest_equivalent) for better accuracy.
        """
        if self._total_count == 0:
            return 0.0

        total = 0.0
        for idx in range(self._counts_len):
            count = self._counts[idx]
            if count > 0:
                value = self._value_from_linear_index(idx)
                he = self._highest_equivalent(value)
                mid = (value + he) / 2.0
                total += mid * count

        return total / self._total_count

    def stdev(self) -> float:
        """Compute the estimated standard deviation."""
        if self._total_count < 2:
            return 0.0

        m = self.mean()
        geometric_dev_total = 0.0

        for idx in range(self._counts_len):
            count = self._counts[idx]
            if count > 0:
                value = self._value_from_linear_index(idx)
                he = self._highest_equivalent(value)
                mid = (value + he) / 2.0
                dev = mid - m
                geometric_dev_total += dev * dev * count

        return math.sqrt(geometric_dev_total / self._total_count)

    def _iter_recorded(self) -> Iterator[tuple[int, int]]:
        """Iterate over (value, count) pairs for all recorded buckets.

        Yields in ascending value order by traversing the flat counts array
        linearly (index 0 to counts_len - 1).
        """
        for idx in range(self._counts_len):
            count = self._counts[idx]
            if count > 0:
                value = self._value_from_linear_index(idx)
                yield value, count

    def to_ecdf(self) -> tuple[list[float], list[float]]:
        """Return (values, percentiles) for ECDF plotting.

        Returns:
            Tuple of (values, percentiles) where percentiles are in [0, 1].
        """
        if self._total_count == 0:
            return [], []

        values: list[float] = []
        percentiles: list[float] = []
        running = 0

        for value, count in self._iter_recorded():
            running += count
            values.append(float(value))
            percentiles.append(running / self._total_count)

        return values, percentiles

    def copy(self) -> HdrHistogram:
        """Create a deep copy of this histogram."""
        h = HdrHistogram(
            lowest_value=self._lowest_value,
            highest_value=self._highest_value,
            significant_digits=self._significant_digits,
        )
        h._counts = self._counts.copy()
        h._total_count = self._total_count
        h._min_value = self._min_value
        h._max_value = self._max_value
        return h

    def reset(self) -> None:
        """Reset all counts to zero."""
        for i in range(self._counts_len):
            self._counts[i] = 0
        self._total_count = 0
        self._min_value = 0
        self._max_value = 0
