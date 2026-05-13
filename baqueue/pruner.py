"""Pruner - removes old or unwanted jobs from the queue system."""

from __future__ import annotations

import asyncio
import logging

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

    # ── Threshold resolution ────────────────────────────────────
    # New *_seconds fields are primary. The legacy *_hours fields override
    # when set to a positive value, so existing JSON configs keep working.

    @property
    def completed_threshold(self) -> float:
        if self.config.prune_completed_hours > 0:
            return self.config.prune_completed_hours * 3600
        return float(self.config.prune_completed_seconds)

    @property
    def failed_threshold(self) -> float:
        if self.config.prune_failed_hours > 0:
            return self.config.prune_failed_hours * 3600
        return float(self.config.prune_other_seconds)

    @property
    def cancelled_threshold(self) -> float:
        if self.config.prune_cancelled_hours > 0:
            return self.config.prune_cancelled_hours * 3600
        return float(self.config.prune_other_seconds)

    @property
    def metrics_threshold(self) -> float:
        if self.config.prune_metrics_hours > 0:
            return self.config.prune_metrics_hours * 3600
        return float(self.config.prune_metrics_seconds)

    async def prune_once(self) -> dict[str, int]:
        """Run a single prune pass based on config."""
        results: dict[str, int] = {}

        if self.completed_threshold > 0:
            results["completed"] = await self.driver.prune(
                status="completed",
                older_than_seconds=self.completed_threshold,
            )

        if self.failed_threshold > 0:
            results["failed"] = await self.driver.prune(
                status="failed",
                older_than_seconds=self.failed_threshold,
            )

        if self.cancelled_threshold > 0:
            results["cancelled"] = await self.driver.prune(
                status="cancelled",
                older_than_seconds=self.cancelled_threshold,
            )

        if self.metrics_threshold > 0:
            results["metrics"] = await self.driver.prune_metrics(
                older_than_seconds=self.metrics_threshold,
            )

        total = sum(results.values())
        if total > 0:
            logger.info("Pruned %d entries: %s", total, results)
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

    async def start(self, interval_seconds: float | None = None) -> None:
        """Start automatic pruning loop. Uses config.prune_interval_seconds by default."""
        if interval_seconds is None:
            interval_seconds = float(self.config.prune_interval_seconds)
        interval_seconds = max(1.0, float(interval_seconds))

        self._running = True
        logger.info("Pruner started (every %.0fs)", interval_seconds)

        try:
            while self._running:
                try:
                    await self.prune_once()
                except Exception:
                    logger.exception("Error during prune cycle")
                await asyncio.sleep(interval_seconds)
        except asyncio.CancelledError:
            self._running = False
            raise

    def stop(self) -> None:
        self._running = False
