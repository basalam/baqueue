"""Tests for the DashboardAPI."""

from __future__ import annotations

import pytest

from baqueue.config import BaQueueConfig, DriverConfig
from baqueue.dashboard.api import DashboardAPI
from baqueue.job import Job
from baqueue.queue import Queue

pytestmark = pytest.mark.asyncio


class SimpleJob(Job):
    queue = "dapi_test"
    max_attempts = 1
    tags = ["api"]

    async def handle(self, **kw):
        return None


@pytest.fixture(autouse=True)
def _configure_queue():
    Queue.configure(BaQueueConfig(driver=DriverConfig(name="memory")))
    yield


async def _fail(driver, payload):
    await driver.fail(payload, "intentional")


class TestOverview:
    async def test_overview_shape(self):
        await Queue.connect()
        # overview() drives totals off get_metrics(), which in MemoryDriver
        # only sees queues with a recorded metric event. Record one to make
        # the queue visible in the snapshot.
        await Queue.push(SimpleJob)
        await Queue.get_driver().record_metric("dapi_test", "processing", 1)
        api = DashboardAPI(Queue.get_driver())
        data = await api.overview()
        assert "totals" in data
        assert "queues" in data
        assert "rates" in data
        assert "supervisors" in data
        assert data["totals"]["pending"] >= 1
        await Queue.disconnect()


class TestJobsList:
    async def test_filter_by_status(self):
        await Queue.connect()
        api = DashboardAPI(Queue.get_driver())
        await Queue.push(SimpleJob)
        await Queue.push(SimpleJob)
        # Fail one
        driver = Queue.get_driver()
        jobs = await driver.get_jobs(offset=0, limit=10)
        await _fail(driver, jobs[0])

        failed = await api.jobs_list(status="failed", page=1, per_page=10)
        assert failed["total"] == 1
        assert len(failed["jobs"]) == 1

        pending = await api.jobs_list(status="pending", page=1, per_page=10)
        assert pending["total"] == 1
        await Queue.disconnect()

    async def test_pagination(self):
        await Queue.connect()
        api = DashboardAPI(Queue.get_driver())
        for _ in range(7):
            await Queue.push(SimpleJob)

        page1 = await api.jobs_list(page=1, per_page=3)
        page2 = await api.jobs_list(page=2, per_page=3)
        page3 = await api.jobs_list(page=3, per_page=3)

        assert len(page1["jobs"]) == 3
        assert len(page2["jobs"]) == 3
        assert len(page3["jobs"]) == 1

        ids = {j["id"] for j in page1["jobs"] + page2["jobs"] + page3["jobs"]}
        assert len(ids) == 7
        await Queue.disconnect()


class TestRetry:
    async def test_retry_single_job(self):
        await Queue.connect()
        api = DashboardAPI(Queue.get_driver())
        await Queue.push(SimpleJob)
        driver = Queue.get_driver()
        jobs = await driver.get_jobs(offset=0, limit=10)
        await _fail(driver, jobs[0])

        ok = await api.retry_job(jobs[0].id)
        assert ok is True
        # Job should now be pending
        after = await driver.get_job(jobs[0].id)
        assert after.status == "pending"
        await Queue.disconnect()

    async def test_retry_nonexistent_returns_false(self):
        await Queue.connect()
        api = DashboardAPI(Queue.get_driver())
        ok = await api.retry_job("nonexistent")
        assert ok is False
        await Queue.disconnect()

    async def test_retry_non_failed_returns_false(self):
        await Queue.connect()
        api = DashboardAPI(Queue.get_driver())
        job_id = await Queue.push(SimpleJob)
        # Still pending, not failed
        ok = await api.retry_job(job_id)
        assert ok is False
        await Queue.disconnect()


class TestBulkRetryFailed:
    """The new endpoint that powers the dashboard 'Retry All' button."""

    async def test_retries_all_failed(self):
        await Queue.connect()
        api = DashboardAPI(Queue.get_driver())
        # Push 4 jobs, fail 3 of them
        ids = [await Queue.push(SimpleJob) for _ in range(4)]
        driver = Queue.get_driver()
        for jid in ids[:3]:
            j = await driver.get_job(jid)
            await _fail(driver, j)

        n = await api.retry_failed_jobs()
        assert n == 3
        # All previously-failed jobs should now be pending
        for jid in ids[:3]:
            j = await driver.get_job(jid)
            assert j.status == "pending"
        await Queue.disconnect()

    async def test_no_failed_returns_zero(self):
        await Queue.connect()
        api = DashboardAPI(Queue.get_driver())
        await Queue.push(SimpleJob)
        n = await api.retry_failed_jobs()
        assert n == 0
        await Queue.disconnect()

    async def test_queue_filter(self):
        await Queue.connect()
        api = DashboardAPI(Queue.get_driver())

        # Push jobs to two queues, fail them all
        driver = Queue.get_driver()

        async def push_to(queue_name, count):
            for _ in range(count):
                jid = await Queue.push(SimpleJob)
                stored = await driver.get_job(jid)
                stored.queue = queue_name
                await _fail(driver, stored)

        await push_to("q1", 2)
        await push_to("q2", 3)

        n = await api.retry_failed_jobs(queue="q1")
        assert n == 2

        remaining = await driver.get_jobs(status="failed", offset=0, limit=20)
        assert len(remaining) == 3
        assert all(j.queue == "q2" for j in remaining)
        await Queue.disconnect()


class TestPrune:
    async def test_prune_via_api(self):
        await Queue.connect()
        api = DashboardAPI(Queue.get_driver())
        await Queue.push(SimpleJob)
        driver = Queue.get_driver()
        jobs = await driver.get_jobs(offset=0, limit=10)
        await driver.complete(jobs[0])

        n = await api.prune_jobs(status="completed")
        assert n == 1
        await Queue.disconnect()


class TestStats:
    async def test_stats_shape(self):
        await Queue.connect()
        api = DashboardAPI(Queue.get_driver())
        await Queue.push(SimpleJob)
        data = await api.stats()
        assert "queues" in data
        assert "statuses" in data
        assert "total" in data
        assert data["statuses"]["pending"] >= 1
        await Queue.disconnect()
