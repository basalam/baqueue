"""Scheduler - runs jobs on cron expressions or fixed intervals."""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone

from croniter import croniter

from baqueue.config import ScheduleEntry
from baqueue.drivers.base import BaseDriver
from baqueue.events import EventBus
from baqueue.serializer import JobPayload, resolve_job_class

logger = logging.getLogger("baqueue.scheduler")


class Scheduler:
    """Runs scheduled jobs based on cron expressions or intervals.

    Usage::

        scheduler = Scheduler(driver, [
            ScheduleEntry(job_class="myapp.jobs.Cleanup", cron="0 * * * *"),
            ScheduleEntry(job_class="myapp.jobs.Heartbeat", every=30),
        ])
        await scheduler.start()
    """

    def __init__(
        self,
        driver: BaseDriver,
        entries: list[ScheduleEntry] | None = None,
        events: EventBus | None = None,
    ):
        self.driver = driver
        self.entries = entries or []
        self.events = events or EventBus.default()
        self._running = False
        self._last_run: dict[str, float] = {}

    @property
    def is_running(self) -> bool:
        return self._running

    def add(self, entry: ScheduleEntry) -> None:
        self.entries.append(entry)

    async def start(self) -> None:
        self._running = True
        logger.info("Scheduler started with %d entries", len(self.entries))

        while self._running:
            now = datetime.now(timezone.utc)
            for entry in self.entries:
                key = f"{entry.job_class}:{entry.cron or entry.every}"
                if self._should_run(entry, now, key):
                    await self._dispatch(entry)
                    self._last_run[key] = now.timestamp()
            await asyncio.sleep(1)

    def stop(self) -> None:
        self._running = False
        logger.info("Scheduler stopped")

    def _should_run(self, entry: ScheduleEntry, now: datetime, key: str) -> bool:
        last = self._last_run.get(key, 0)

        if entry.cron:
            cron = croniter(entry.cron, datetime.fromtimestamp(last, tz=timezone.utc))
            next_run = cron.get_next(datetime)
            if next_run.tzinfo is None:
                next_run = next_run.replace(tzinfo=timezone.utc)
            return now >= next_run

        if entry.every > 0:
            return (now.timestamp() - last) >= entry.every

        return False

    async def _dispatch(self, entry: ScheduleEntry) -> None:
        try:
            cls = resolve_job_class(entry.job_class)
            if hasattr(cls, "build_payload"):
                if isinstance(cls, type):
                    payload = cls().build_payload(**entry.payload)
                else:
                    payload = cls.build_payload(**entry.payload)
            else:
                payload = JobPayload(
                    job_class=entry.job_class,
                    data=entry.payload,
                    queue=entry.queue,
                )
            payload.queue = entry.queue
            await self.driver.push(payload)
            logger.info("Scheduled job dispatched: %s", entry.job_class)
            self.events.emit_nowait("schedule.dispatched", entry=entry)
        except Exception:
            logger.exception("Failed to dispatch scheduled job: %s", entry.job_class)
