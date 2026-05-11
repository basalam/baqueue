"""Tests for the interval-based Scheduler."""

from __future__ import annotations

import asyncio

import pytest

from baqueue.config import BaQueueConfig, DriverConfig, ScheduleEntry
from baqueue.drivers.memory_driver import MemoryDriver
from baqueue.job import Job
from baqueue.queue import Queue
from baqueue.scheduler import Scheduler

pytestmark = pytest.mark.asyncio


class HeartbeatJob(Job):
    queue = "scheduled"
    max_attempts = 1

    async def handle(self, **kw):
        return "ok"


@pytest.fixture(autouse=True)
def _configure_queue():
    Queue.configure(BaQueueConfig(driver=DriverConfig(name="memory")))
    yield


class TestScheduler:
    async def test_starts_and_stops(self):
        driver = MemoryDriver()
        sched = Scheduler(driver=driver, entries=[])

        async def stop_soon():
            await asyncio.sleep(0.2)
            sched.stop()

        await asyncio.gather(sched.start(), stop_soon())
        assert sched.is_running is False

    async def test_interval_dispatch(self):
        await Queue.connect()
        driver = Queue.get_driver()
        sched = Scheduler(
            driver=driver,
            entries=[
                ScheduleEntry(
                    job_class="tests.test_scheduler.HeartbeatJob",
                    every=1,
                    queue="scheduled",
                ),
            ],
        )

        async def stop_after():
            # Scheduler dispatches immediately on first tick when last_run=0
            await asyncio.sleep(1.5)
            sched.stop()

        await asyncio.gather(sched.start(), stop_after())

        jobs = await driver.get_jobs(queue="scheduled", offset=0, limit=10)
        assert len(jobs) >= 1
        await Queue.disconnect()

    async def test_add_entry(self):
        sched = Scheduler(driver=MemoryDriver(), entries=[])
        assert len(sched.entries) == 0
        sched.add(ScheduleEntry(job_class="x.Y", every=10))
        assert len(sched.entries) == 1
