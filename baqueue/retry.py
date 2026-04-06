"""Backoff / retry strategies for failed jobs."""

from __future__ import annotations

import random
from enum import Enum


class BackoffStrategy(str, Enum):
    FIXED = "fixed"
    LINEAR = "linear"
    EXPONENTIAL = "exponential"


def compute_delay(
    backoff: str | list[int],
    attempt: int,
    base_delay: float = 5.0,
    max_delay: float = 3600.0,
) -> float:
    """Compute the retry delay in seconds for a given attempt.

    Args:
        backoff: Strategy name ("fixed", "linear", "exponential") or
                 a list of explicit delays in seconds (e.g. [10, 60, 300]).
        attempt: Current attempt number (1-based).
        base_delay: Base delay in seconds for computed strategies.
        max_delay: Maximum delay cap.

    Returns:
        Delay in seconds before the next retry.
    """
    if isinstance(backoff, list):
        idx = min(attempt - 1, len(backoff) - 1)
        return float(backoff[idx])

    strategy = backoff.lower()

    if strategy == BackoffStrategy.FIXED:
        return base_delay

    if strategy == BackoffStrategy.LINEAR:
        return min(base_delay * attempt, max_delay)

    if strategy == BackoffStrategy.EXPONENTIAL:
        delay = base_delay * (2 ** (attempt - 1))
        jitter = random.uniform(0, delay * 0.1)
        return min(delay + jitter, max_delay)

    return base_delay


def should_retry(attempt: int, max_attempts: int) -> bool:
    """Whether the job should be retried."""
    return attempt < max_attempts
