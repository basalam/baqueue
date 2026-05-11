"""Tests for the Queue singleton facade."""

from __future__ import annotations

import pytest

from baqueue.config import BaQueueConfig, DriverConfig
from baqueue.job import Job
from baqueue.queue import Queue
from baqueue.serializer import _now_ts


class SimpleJob(Job):
    queue = "default"
    max_attempts = 3
    tags = ["simple"]

    async def handle(self, x: int = 1) -> int:
        return x


class TaggedJob(Job):
    queue = "tagged"
    tags = ["alpha", "beta"]

    async def handle(self) -> None:
        return None


class TestConfiguration:
    async def test_configure_creates_driver(self):
        Queue.configure(BaQueueConfig(driver=DriverConfig(name="memory")))
        await Queue.connect()
        assert Queue.get_driver() is not None
        await Queue.disconnect()

    async def test_reset_clears_singletons(self):
        Queue.configure(BaQueueConfig(driver=DriverConfig(name="memory")))
        await Queue.connect()
        Queue.reset()
        assert Queue._driver is None
        assert Queue._config is None
        assert Queue._events is None


class TestPush:
    async def test_push_returns_id(self, configured_queue):
        job_id = await configured_queue.push(SimpleJob, x=42)
        assert isinstance(job_id, str) and len(job_id) == 32

    async def test_push_increases_size(self, configured_queue):
        await configured_queue.push(SimpleJob, x=1)
        await configured_queue.push(SimpleJob, x=2)
        size = await configured_queue.size("default")
        assert size == 2

    async def test_push_stores_data(self, configured_queue):
        job_id = await configured_queue.push(SimpleJob, x=99)
        job = await configured_queue.get_job(job_id)
        assert job is not None
        assert job.data == {"x": 99}
        assert job.queue == "default"
        assert job.status == "pending"

    async def test_push_emits_event(self, configured_queue):
        seen = []
        events = configured_queue.get_events()

        async def handler(**kw):
            seen.append(kw.get("payload"))

        events.on("job.pushed", handler)
        await configured_queue.push(SimpleJob, x=1)
        # emit_nowait schedules a task — give the loop a tick
        import asyncio
        await asyncio.sleep(0)
        assert len(seen) == 1


class TestLater:
    async def test_later_sets_delay_until(self, configured_queue):
        before = _now_ts()
        job_id = await configured_queue.later(SimpleJob, delay=30, x=5)
        job = await configured_queue.get_job(job_id)
        assert job.delay_until is not None
        assert job.delay_until >= before + 30 - 0.5  # tolerance

    async def test_later_does_not_show_up_in_queue_immediately(self, configured_queue):
        await configured_queue.later(SimpleJob, delay=60, x=1)
        assert await configured_queue.size("default") == 0


class TestBulk:
    async def test_bulk_returns_ids(self, configured_queue):
        ids = await configured_queue.bulk([
            (SimpleJob, {"x": 1}),
            (SimpleJob, {"x": 2}),
            (SimpleJob, {"x": 3}),
        ])
        assert len(ids) == 3
        assert all(isinstance(i, str) for i in ids)

    async def test_bulk_all_pending(self, configured_queue):
        await configured_queue.bulk([(SimpleJob, {"x": i}) for i in range(5)])
        assert await configured_queue.size("default") == 5

    async def test_bulk_empty(self, configured_queue):
        ids = await configured_queue.bulk([])
        assert ids == []


class TestQueriesAndMetrics:
    async def test_queues_returns_active_queues(self, configured_queue):
        await configured_queue.push(SimpleJob, x=1)
        await configured_queue.push(TaggedJob)
        queues = sorted(await configured_queue.queues())
        assert "default" in queues
        assert "tagged" in queues

    async def test_get_jobs_filters_by_tag(self, configured_queue):
        await configured_queue.push(SimpleJob, x=1)
        await configured_queue.push(TaggedJob)
        alpha_jobs = await configured_queue.get_jobs(tag="alpha", offset=0, limit=10)
        assert len(alpha_jobs) == 1
        assert alpha_jobs[0].queue == "tagged"

    async def test_get_jobs_filters_by_status(self, configured_queue):
        await configured_queue.push(SimpleJob, x=1)
        # Manually mark one as failed
        driver = configured_queue.get_driver()
        jobs = await driver.get_jobs(offset=0, limit=10)
        await driver.fail(jobs[0], "test failure")
        failed = await configured_queue.get_jobs(status="failed", offset=0, limit=10)
        assert len(failed) == 1

    async def test_metrics_shape(self, configured_queue):
        # MemoryDriver only reports queues that have at least one metric event
        # — push alone doesn't record one. Use stats() which counts directly.
        await configured_queue.push(SimpleJob, x=1)
        driver = configured_queue.get_driver()
        await driver.record_metric("default", "processing", 1)
        m = await configured_queue.metrics()
        assert "default" in m
        assert m["default"]["pending"] == 1


class TestPrune:
    async def test_prune_by_status(self, configured_queue):
        await configured_queue.push(SimpleJob, x=1)
        driver = configured_queue.get_driver()
        jobs = await driver.get_jobs(offset=0, limit=10)
        await driver.complete(jobs[0])
        n = await configured_queue.prune(status="completed")
        assert n == 1

    async def test_prune_by_tag(self, configured_queue):
        await configured_queue.push(TaggedJob)
        n = await configured_queue.prune(tag="alpha")
        assert n == 1

    async def test_prune_emits_event(self, configured_queue):
        seen = []
        events = configured_queue.get_events()

        async def handler(**kw):
            seen.append(kw)

        events.on("queue.pruned", handler)
        await configured_queue.push(SimpleJob, x=1)
        driver = configured_queue.get_driver()
        jobs = await driver.get_jobs(offset=0, limit=10)
        await driver.complete(jobs[0])
        await configured_queue.prune(status="completed")
        import asyncio
        await asyncio.sleep(0)
        assert len(seen) == 1


class TestRetryFailed:
    """The new bulk-retry method introduced for the dashboard / CLI."""

    async def _fail_all(self, configured_queue, jobs_to_fail: int = 5, queue: str = "default", tag: str | None = None):
        driver = configured_queue.get_driver()
        ids = []
        for i in range(jobs_to_fail):
            if tag:
                # Push with custom tag by setting class attribute path; easier to push
                # then mutate the stored payload directly.
                jid = await configured_queue.push(SimpleJob, x=i)
                stored = await driver.get_job(jid)
                stored.tags.append(tag)
                stored.queue = queue
            else:
                jid = await configured_queue.push(SimpleJob, x=i)
                stored = await driver.get_job(jid)
                stored.queue = queue
            await driver.fail(stored, f"boom {i}")
            ids.append(jid)
        return ids

    async def test_returns_zero_when_nothing_failed(self, configured_queue):
        await configured_queue.push(SimpleJob, x=1)
        n = await configured_queue.retry_failed()
        assert n == 0

    async def test_retries_all_failed(self, configured_queue):
        await self._fail_all(configured_queue, jobs_to_fail=5)
        n = await configured_queue.retry_failed()
        assert n == 5
        # All should now be pending
        driver = configured_queue.get_driver()
        failed = await driver.get_jobs(status="failed", offset=0, limit=20)
        assert len(failed) == 0
        pending = await driver.get_jobs(status="pending", offset=0, limit=20)
        assert len(pending) == 5

    async def test_queue_filter_isolates(self, configured_queue):
        await self._fail_all(configured_queue, jobs_to_fail=3, queue="emails")
        await self._fail_all(configured_queue, jobs_to_fail=2, queue="reports")

        n = await configured_queue.retry_failed(queue="emails")
        assert n == 3

        driver = configured_queue.get_driver()
        still_failed = await driver.get_jobs(status="failed", offset=0, limit=20)
        assert len(still_failed) == 2
        assert all(j.queue == "reports" for j in still_failed)

    async def test_tag_filter_isolates(self, configured_queue):
        await self._fail_all(configured_queue, jobs_to_fail=2, tag="critical")
        await self._fail_all(configured_queue, jobs_to_fail=3, tag="low")

        n = await configured_queue.retry_failed(tag="critical")
        assert n == 2

        driver = configured_queue.get_driver()
        remaining = await driver.get_jobs(status="failed", offset=0, limit=20)
        assert len(remaining) == 3
        assert all("low" in j.tags for j in remaining)

    async def test_created_from_filter(self, configured_queue):
        # Push 3 jobs, fail them, then push 2 more later and fail them.
        await self._fail_all(configured_queue, jobs_to_fail=3)
        cutoff = _now_ts()
        # ensure later jobs have created_at strictly greater than cutoff
        import asyncio
        await asyncio.sleep(0.01)
        await self._fail_all(configured_queue, jobs_to_fail=2)

        n = await configured_queue.retry_failed(created_from=cutoff)
        assert n == 2  # Only the newer 2 should retry

    async def test_large_batch_loop(self, configured_queue):
        # More than one batch_size (200) — verify the loop continues
        # We'll push 250 to keep this fast.
        await self._fail_all(configured_queue, jobs_to_fail=250)
        n = await configured_queue.retry_failed()
        assert n == 250
