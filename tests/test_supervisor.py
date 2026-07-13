"""Tests for the Supervisor pool + delayed-job promotion."""

from __future__ import annotations

import asyncio

import pytest

from baqueue.config import BaQueueConfig, DriverConfig, SupervisorConfig
from baqueue.job import Job
from baqueue.queue import Queue
from baqueue.supervisor import Supervisor

pytestmark = pytest.mark.asyncio


PROCESSED: list[int] = []


class _Reset:
    def __init__(self):
        PROCESSED.clear()


@pytest.fixture(autouse=True)
def _reset_processed():
    _Reset()
    yield
    _Reset()


class TrackedJob(Job):
    queue = "sup_test"
    max_attempts = 1

    async def handle(self, n: int = 0):
        PROCESSED.append(n)
        return n


@pytest.fixture(autouse=True)
def _configure_queue():
    Queue.configure(BaQueueConfig(driver=DriverConfig(name="memory")))
    yield


async def _run_supervisor_until(supervisor: Supervisor, predicate, timeout: float = 5.0):
    """Run supervisor until predicate() returns True or timeout."""
    task = asyncio.create_task(supervisor.start())
    try:
        deadline = asyncio.get_event_loop().time() + timeout
        while not predicate():
            if asyncio.get_event_loop().time() > deadline:
                break
            await asyncio.sleep(0.05)
    finally:
        await supervisor.stop()
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass


class TestLifecycle:
    async def test_starts_with_configured_workers(self):
        await Queue.connect()
        sup = Supervisor(
            driver=Queue.get_driver(),
            config=SupervisorConfig(
                queues=["sup_test"], min_workers=2, max_workers=2, sleep=0.05,
            ),
        )

        async def stop_soon():
            await asyncio.sleep(0.3)
            await sup.stop()

        await asyncio.gather(sup.start(), stop_soon())
        # After stop, supervisor should be cleanly down
        assert sup.is_running is False
        await Queue.disconnect()

    async def test_stats_shape(self):
        await Queue.connect()
        sup = Supervisor(
            driver=Queue.get_driver(),
            config=SupervisorConfig(queues=["sup_test"], min_workers=1, max_workers=1),
        )
        stats = sup.stats
        assert stats["name"] == "default"
        assert stats["queues"] == ["sup_test"]
        assert stats["running"] is False
        await Queue.disconnect()


class TestProcessing:
    async def test_processes_pushed_jobs(self):
        await Queue.connect()
        for i in range(5):
            await Queue.push(TrackedJob, n=i)

        sup = Supervisor(
            driver=Queue.get_driver(),
            config=SupervisorConfig(
                queues=["sup_test"], min_workers=2, max_workers=2, sleep=0.05,
            ),
        )

        await _run_supervisor_until(sup, lambda: len(PROCESSED) >= 5, timeout=5.0)
        assert sorted(PROCESSED) == [0, 1, 2, 3, 4]
        await Queue.disconnect()

    async def test_requeues_stuck_processing_jobs(self):
        await Queue.connect()
        await Queue.push(TrackedJob, n=42)
        stuck = await Queue.get_driver().pop("sup_test")
        assert stuck is not None
        stuck.started_at = stuck.updated_at = stuck.started_at - 7200

        sup = Supervisor(
            driver=Queue.get_driver(),
            config=SupervisorConfig(
                queues=["sup_test"],
                min_workers=1,
                max_workers=1,
                sleep=0.05,
                stuck_processing_seconds=3600,
                stuck_check_interval_seconds=1,
            ),
        )

        await _run_supervisor_until(sup, lambda: 42 in PROCESSED, timeout=5.0)
        assert 42 in PROCESSED
        recovered = await Queue.get_job(stuck.id)
        assert recovered.status == "completed"
        await Queue.disconnect()


class TestDelayedPromotion:
    async def test_delayed_job_promoted_and_processed(self):
        await Queue.connect()
        # Tiny delay so the supervisor's poll loop picks it up within the test
        await Queue.later(TrackedJob, delay=0.5, n=99)

        sup = Supervisor(
            driver=Queue.get_driver(),
            config=SupervisorConfig(
                queues=["sup_test"], min_workers=1, max_workers=1, sleep=0.05,
            ),
        )

        await _run_supervisor_until(sup, lambda: 99 in PROCESSED, timeout=5.0)
        assert 99 in PROCESSED
        await Queue.disconnect()

    async def test_undelayed_jobs_run_first(self):
        await Queue.connect()
        await Queue.later(TrackedJob, delay=5.0, n=999)  # not within test window
        await Queue.push(TrackedJob, n=1)

        sup = Supervisor(
            driver=Queue.get_driver(),
            config=SupervisorConfig(
                queues=["sup_test"], min_workers=1, max_workers=1, sleep=0.05,
            ),
        )

        await _run_supervisor_until(sup, lambda: 1 in PROCESSED, timeout=3.0)
        # Only the immediate one should have been processed
        assert 1 in PROCESSED
        assert 999 not in PROCESSED
        await Queue.disconnect()
