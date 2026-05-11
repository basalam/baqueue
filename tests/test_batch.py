"""Tests for the Batch fluent builder and lifecycle callbacks."""

from __future__ import annotations

import asyncio

import pytest

from baqueue.batch import Batch, BatchResult
from baqueue.config import BaQueueConfig, DriverConfig
from baqueue.events import EventBus
from baqueue.job import Job
from baqueue.queue import Queue

pytestmark = pytest.mark.asyncio


COUNTS: dict[str, int] = {"then": 0, "catch": 0, "finally": 0}


@pytest.fixture(autouse=True)
def _reset_counts():
    COUNTS["then"] = COUNTS["catch"] = COUNTS["finally"] = 0
    yield
    COUNTS["then"] = COUNTS["catch"] = COUNTS["finally"] = 0


class WorkJob(Job):
    queue = "btest"
    max_attempts = 1

    async def handle(self, **kw):
        return None


class ThenJob(Job):
    queue = "btest"

    async def handle(self, **kw):
        COUNTS["then"] += 1


class CatchJob(Job):
    queue = "btest"

    async def handle(self, **kw):
        COUNTS["catch"] += 1


class FinallyJob(Job):
    queue = "btest"

    async def handle(self, **kw):
        COUNTS["finally"] += 1


@pytest.fixture(autouse=True)
def _configure_queue():
    Queue.configure(BaQueueConfig(driver=DriverConfig(name="memory")))
    yield


class TestBatchBuilder:
    async def test_name_and_tag_chaining(self):
        await Queue.connect()
        b = Batch(Queue.get_driver(), [(WorkJob, {})])
        # All builder methods return self
        assert b.name("xyz") is b
        assert b.tag("a", "b") is b
        assert b.allow_failures() is b
        assert b.then(ThenJob) is b
        assert b.catch(CatchJob) is b
        assert b.on_finally(FinallyJob) is b
        await Queue.disconnect()

    async def test_dispatch_returns_result(self):
        await Queue.connect()
        result = await Batch(
            Queue.get_driver(),
            [(WorkJob, {"n": i}) for i in range(3)],
        ).name("test-batch").dispatch()

        assert isinstance(result, BatchResult)
        assert result.total == 3
        assert len(result.job_ids) == 3
        assert result.name == "test-batch"
        await Queue.disconnect()

    async def test_dispatched_jobs_have_batch_metadata(self):
        await Queue.connect()
        result = await Batch(
            Queue.get_driver(),
            [(WorkJob, {}) for _ in range(2)],
        ).name("named").tag("custom").dispatch()

        for job_id in result.job_ids:
            job = await Queue.get_driver().get_job(job_id)
            assert job.batch_id == result.batch_id
            assert f"batch:{result.batch_id}" in job.tags
            assert "batch:named" in job.tags
            assert "custom" in job.tags
        await Queue.disconnect()

    async def test_batch_persisted_in_driver(self):
        await Queue.connect()
        result = await Batch(
            Queue.get_driver(),
            [(WorkJob, {})],
        ).dispatch()
        b = await Queue.get_driver().get_batch(result.batch_id)
        assert b is not None
        assert b["total"] == 1
        assert b["completed_count"] == 0
        await Queue.disconnect()


class TestBatchCallbacks:
    async def test_then_runs_when_all_complete(self):
        """Emit batch.completed manually; the callback should dispatch ThenJob."""
        await Queue.connect()
        bus = EventBus.default()

        await Batch(
            Queue.get_driver(),
            [(WorkJob, {})],
        ).then(ThenJob).dispatch()

        # Simulate batch completion event
        all_batches = await Queue.get_driver().get_batch(
            (await Queue.get_driver().get_jobs(offset=0, limit=1))[0].batch_id
        )
        batch_id = all_batches["id"]
        await bus.emit("batch.completed", batch_id=batch_id, batch={
            "id": batch_id, "failed_count": 0, "completed_count": 1, "total": 1,
        })
        # Allow scheduled tasks to run
        await asyncio.sleep(0.05)

        # Look for the dispatched ThenJob in the queue
        then_jobs = await Queue.get_driver().get_jobs(queue="btest", offset=0, limit=20)
        # Should now include the original WorkJob + the dispatched ThenJob
        names = [j.job_class.split(".")[-1] for j in then_jobs]
        assert "ThenJob" in names
        await Queue.disconnect()

    async def test_catch_runs_on_failure(self):
        await Queue.connect()
        bus = EventBus.default()

        await Batch(
            Queue.get_driver(),
            [(WorkJob, {})],
        ).catch(CatchJob).dispatch()

        batch_id = (await Queue.get_driver().get_jobs(offset=0, limit=1))[0].batch_id
        await bus.emit("batch.failed", batch_id=batch_id, batch={
            "id": batch_id, "failed_count": 1, "completed_count": 0, "total": 1,
        })
        await asyncio.sleep(0.05)

        all_jobs = await Queue.get_driver().get_jobs(queue="btest", offset=0, limit=20)
        names = [j.job_class.split(".")[-1] for j in all_jobs]
        assert "CatchJob" in names
        await Queue.disconnect()
