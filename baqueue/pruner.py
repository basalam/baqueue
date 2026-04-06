"""Pruner - removes old or unwanted jobs from the queue system."""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from baqueue.config import BaQueueConfig
from baqueue.drivers.base import BaseDriver
from baqueue.events import EventBus

logger = logging.getLogger("baqueue.pruner")


class Pruner:
    """Automatically prunes old jobs on a schedule.

    Can also be triggered manually via the CLI or API.
    """

    def __init__(
        self,
        driver: BaseDriver,
        config: BaQueueConfig | None = None,
        events: EventBus | None = None,
    ):
        self.driver = driver
        self.config = config or BaQueueConfig()
        self.events = events or EventBus.default()
        self._running = False

    async def prune_once(self) -> dict[str, int]:
        """Run a single prune pass based on config."""
        results: dict[str, int] = {}

        if self.config.prune_completed_hours > 0:
            count = await self.driver.prune(
                status="completed",
                older_than_seconds=self.config.prune_completed_hours * 3600,
            )
            results["completed"] = count

        if self.config.prune_failed_hours > 0:
            count = await self.driver.prune(
                status="failed",
                older_than_seconds=self.config.prune_failed_hours * 3600,
            )
            results["failed"] = count

        if self.config.prune_cancelled_hours > 0:
            count = await self.driver.prune(
                status="cancelled",
                older_than_seconds=self.config.prune_cancelled_hours * 3600,
            )
            results["cancelled"] = count

        total = sum(results.values())
        if total > 0:
            logger.info("Pruned %d jobs: %s", total, results)
            self.events.emit_nowait("queue.pruned", results=results)

        return results

    async def prune_by_tag(self, tag: str) -> int:
        count = await self.driver.prune(tag=tag)
        if count > 0:
            logger.info("Pruned %d jobs with tag '%s'", count, tag)
        return count

    async def prune_by_status(self, status: str, hours: float | None = None) -> int:
        older_than = hours * 3600 if hours else None
        count = await self.driver.prune(status=status, older_than_seconds=older_than)
        if count > 0:
            logger.info("Pruned %d jobs with status '%s'", count, status)
        return count

    async def start(self, interval_minutes: int = 60) -> None:
        """Start automatic pruning loop."""
        self._running = True
        logger.info("Pruner started (every %d minutes)", interval_minutes)

        while self._running:
            try:
                await self.prune_once()
            except Exception:
                logger.exception("Error during prune cycle")
            await asyncio.sleep(interval_minutes * 60)

    def stop(self) -> None:
        self._running = False
