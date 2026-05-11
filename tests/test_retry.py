"""Tests for backoff strategies and retry policy helpers."""

from __future__ import annotations

import pytest

from baqueue.retry import BackoffStrategy, compute_delay, should_retry


class TestBackoffStrategy:
    def test_enum_values(self):
        assert BackoffStrategy.FIXED == "fixed"
        assert BackoffStrategy.LINEAR == "linear"
        assert BackoffStrategy.EXPONENTIAL == "exponential"

    def test_enum_membership(self):
        assert "fixed" in {s.value for s in BackoffStrategy}


class TestComputeDelayFixed:
    def test_returns_base_for_every_attempt(self):
        for attempt in range(1, 6):
            assert compute_delay("fixed", attempt, base_delay=7.0) == 7.0

    def test_uppercase_name(self):
        assert compute_delay("FIXED", 1, base_delay=5.0) == 5.0


class TestComputeDelayLinear:
    def test_grows_linearly_with_attempt(self):
        assert compute_delay("linear", 1, base_delay=2.0) == 2.0
        assert compute_delay("linear", 2, base_delay=2.0) == 4.0
        assert compute_delay("linear", 5, base_delay=2.0) == 10.0

    def test_clamped_to_max_delay(self):
        delay = compute_delay("linear", 100, base_delay=10.0, max_delay=50.0)
        assert delay == 50.0


class TestComputeDelayExponential:
    def test_grows_exponentially(self):
        # base * 2^(attempt-1) with up-to-10% jitter
        for attempt in range(1, 6):
            d = compute_delay("exponential", attempt, base_delay=5.0, max_delay=10_000.0)
            base = 5.0 * (2 ** (attempt - 1))
            assert base <= d <= base * 1.1 + 0.001  # jitter is 0..10%

    def test_clamped_to_max(self):
        d = compute_delay("exponential", 20, base_delay=5.0, max_delay=100.0)
        assert d == 100.0


class TestComputeDelayList:
    def test_picks_per_attempt(self):
        backoff = [10, 30, 60]
        assert compute_delay(backoff, 1) == 10.0
        assert compute_delay(backoff, 2) == 30.0
        assert compute_delay(backoff, 3) == 60.0

    def test_clamps_past_end(self):
        backoff = [10, 30, 60]
        assert compute_delay(backoff, 5) == 60.0  # uses last entry
        assert compute_delay(backoff, 100) == 60.0

    def test_returns_float(self):
        assert isinstance(compute_delay([10], 1), float)


class TestComputeDelayUnknown:
    def test_unknown_strategy_falls_back_to_base(self):
        assert compute_delay("bogus", 5, base_delay=3.0) == 3.0


class TestShouldRetry:
    @pytest.mark.parametrize("attempt,max_attempts,expected", [
        (0, 3, True),
        (1, 3, True),
        (2, 3, True),
        (3, 3, False),
        (4, 3, False),
        (1, 1, False),
        (0, 1, True),
    ])
    def test_attempt_thresholds(self, attempt, max_attempts, expected):
        assert should_retry(attempt, max_attempts) is expected
