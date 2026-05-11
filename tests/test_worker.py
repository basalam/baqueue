"""Tests for the Worker job execution loop."""

from __future__ import annotations

import asyncio

import pytest

from baqueue.config import BaQueueConfig, DriverConfig
from baqueue.events import EventBus
from baqueue.job import Job
from baqueue.queue import Queue
from baqueue.worker import Worker

pytestmark = pytest.mark.asyncio


# Jobs must be importable by qualified path → defined at module level

SIDE_EFFECTS: dict[str, list] = {"ok": [], "fail_count": 0}


class _ResetSideEffects:
    """Reset module-level state between tests."""

    def __init__(self):
        SIDE_EFFECTS["ok"].clear()
        SIDE_EFFECTS["fail_count"] = 0


@pytest.fixture(autouse=True)
def _reset_side_effects():
    _ResetSideEffects()
    yield
    _ResetSideEffects()


class GoodJob(Job):
    queue = "wtest"
    max_attempts = 1

    async def handle(self, n: int = 0):
        SIDE_EFFECTS["ok"].append(n)
        return n


class AlwaysFailsJob(Job):
    queue = "wtest"
    max_attempts = 1
    backoff = "fixed"

    async def handle(self, **kw):
        SIDE_EFFECTS["fail_count"] += 1
        raise RuntimeError("intentional")


class RetryableJob(Job):
    queue = "wtest"
    max_attempts = 3
    backoff = "fixed"

    async def handle(self, **kw):
        SIDE_EFFECTS["fail_count"] += 1
        raise RuntimeError("retry me")


class SlowJob(Job):
    queue = "wtest"
    max_attempts = 1
    timeout = 1  # 1 second

    async def handle(self, **kw):
        await asyncio.sleep(5)  # exceeds timeout


@pytest.fixture(autouse=True)
def _configure_queue():
    Queue.configure(BaQueueConfig(driver=DriverConfig(name="memory")))
    yield


async def _run_worker_once(worker: Worker, jobs_to_process: int = 1, max_wait: float = 3.0):
    """Run a worker until it processes `jobs_to_process` jobs or hits a deadline."""
    task = asyncio.create_task(worker.start())
    try:
        deadline = asyncio.get_event_loop().time() + max_wait
        while (worker._jobs_processed + worker._jobs_failed) < jobs_to_process:
            if asyncio.get_event_loop().time() > deadline:
                break
            await asyncio.sleep(0.05)
    finally:
        worker.stop()
        await asyncio.sleep(0.05)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass


class TestSuccessfulExecution:
    async def test_runs_and_completes_job(self):
        await Queue.connect()
        await Queue.push(GoodJob, n=42)

        worker = Worker(
            driver=Queue.get_driver(), queues=["wtest"],
            sleep_interval=0.05, timeout=5, name="w-success",
        )
        await _run_worker_once(worker, jobs_to_process=1)

        assert SIDE_EFFECTS["ok"] == [42]
        assert worker._jobs_processed == 1
        await Queue.disconnect()

    async def test_emits_completed_event(self):
        await Queue.connect()
        completed = []

        async def handler(**kw):
            completed.append(kw.get("payload"))

        EventBus.default().on("job.completed", handler)

        await Queue.push(GoodJob, n=1)
        worker = Worker(
            driver=Queue.get_driver(), queues=["wtest"],
            sleep_interval=0.05, name="w-evt",
        )
        await _run_worker_once(worker, jobs_to_process=1)
        await asyncio.sleep(0.05)
        assert len(completed) == 1
        await Queue.disconnect()


class TestFailure:
    async def test_failed_job_marked_failed_after_max_attempts(self):
        await Queue.connect()
        await Queue.push(AlwaysFailsJob)

        worker = Worker(
            driver=Queue.get_driver(), queues=["wtest"],
            sleep_interval=0.05, name="w-fail",
        )
        await _run_worker_once(worker, jobs_to_process=1)

        failed = await Queue.get_driver().get_jobs(status="failed", offset=0, limit=10)
        assert len(failed) == 1
        assert failed[0].status == "failed"
        assert worker._jobs_failed == 1
        assert worker._jobs_processed == 0
        await Queue.disconnect()

    async def test_retry_releases_job_back(self):
        await Queue.connect()
        await Queue.push(RetryableJob)

        worker = Worker(
            driver=Queue.get_driver(), queues=["wtest"],
            sleep_interval=0.05, name="w-retry",
        )

        # First attempt: should fail and release back. Stop right after.
        task = asyncio.create_task(worker.start())
        await asyncio.sleep(0.5)
        worker.stop()
        await asyncio.sleep(0.05)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

        # At least one attempt should have run
        assert SIDE_EFFECTS["fail_count"] >= 1
        # Since max_attempts=3, the job should NOT be fully failed yet on attempt 1
        all_jobs = await Queue.get_driver().get_jobs(offset=0, limit=10)
        assert len(all_jobs) == 1
        # Could be pending (released) or in delayed pool
        # depending on timing of the fixed backoff (5s)
        await Queue.disconnect()


class TestTimeout:
    async def test_slow_job_times_out_and_fails(self):
        await Queue.connect()
        await Queue.push(SlowJob)

        worker = Worker(
            driver=Queue.get_driver(), queues=["wtest"],
            sleep_interval=0.05, timeout=10, name="w-slow",
        )
        await _run_worker_once(worker, jobs_to_process=1, max_wait=3.0)

        failed = await Queue.get_driver().get_jobs(status="failed", offset=0, limit=10)
        assert len(failed) == 1
        assert "TimeoutError" in (failed[0].error or "") or "timeout" in (failed[0].error or "").lower()
        await Queue.disconnect()


class TestWorkerStats:
    async def test_stats_shape(self):
        await Queue.connect()
        w = Worker(
            driver=Queue.get_driver(), queues=["wtest"],
            sleep_interval=0.1, name="stats-test",
        )
        stats = w.stats
        assert stats["name"] == "stats-test"
        assert stats["queues"] == ["wtest"]
        assert stats["running"] is False
        assert stats["jobs_processed"] == 0
        assert stats["jobs_failed"] == 0
        assert stats["current_job"] is None
        await Queue.disconnect()
