"""Worker balancing strategies across queues."""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod

from baqueue.drivers.base import BaseDriver

logger = logging.getLogger("baqueue.balancer")


class BaseBalancer(ABC):
    """Base class for balancing strategies."""

    @abstractmethod
    async def recommend(
        self,
        driver: BaseDriver,
        queues: list[str],
        current_workers: int,
    ) -> int:
        """Return the recommended number of workers."""
        ...


class AutoBalancer(BaseBalancer):
    """Dynamically adjusts workers based on queue pressure.

    Scales up when queues have high wait times or large backlogs,
    scales down when queues are mostly idle.
    """

    def __init__(self, min_workers: int = 1, max_workers: int = 10):
        self.min_workers = min_workers
        self.max_workers = max_workers

    async def recommend(
        self,
        driver: BaseDriver,
        queues: list[str],
        current_workers: int,
    ) -> int:
        total_pending = 0
        for q in queues:
            total_pending += await driver.size(q)

        if total_pending == 0:
            return self.min_workers

        if total_pending > current_workers * 10:
            desired = min(current_workers + 2, self.max_workers)
        elif total_pending > current_workers * 5:
            desired = min(current_workers + 1, self.max_workers)
        elif total_pending < current_workers:
            desired = max(current_workers - 1, self.min_workers)
        else:
            desired = current_workers

        return desired


class SimpleBalancer(BaseBalancer):
    """Round-robin: keeps worker count stable, just ensures fairness."""

    def __init__(self, min_workers: int = 1, max_workers: int = 10):
        self.min_workers = min_workers
        self.max_workers = max_workers

    async def recommend(
        self,
        driver: BaseDriver,
        queues: list[str],
        current_workers: int,
    ) -> int:
        total_pending = 0
        for q in queues:
            total_pending += await driver.size(q)

        if total_pending > 0:
            return max(min(len(queues), self.max_workers), self.min_workers)
        return self.min_workers


class NullBalancer(BaseBalancer):
    """No balancing - keeps the current worker count unchanged."""

    async def recommend(
        self,
        driver: BaseDriver,
        queues: list[str],
        current_workers: int,
    ) -> int:
        return current_workers


def create_balancer(
    strategy: str,
    min_workers: int = 1,
    max_workers: int = 10,
) -> BaseBalancer:
    """Factory for creating a balancer by strategy name."""
    if strategy == "auto":
        return AutoBalancer(min_workers=min_workers, max_workers=max_workers)
    elif strategy == "simple":
        return SimpleBalancer(min_workers=min_workers, max_workers=max_workers)
    else:
        return NullBalancer()
