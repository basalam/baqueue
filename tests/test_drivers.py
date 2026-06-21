"""Cross-driver contract tests.

These run against the memory + sqlite drivers via the `driver` fixture in
conftest.py, so anything we promise via BaseDriver gets exercised on both.
"""

from __future__ import annotations

import asyncio

import pytest

from baqueue.serializer import JobPayload, _now_ts


pytestmark = pytest.mark.asyncio


def _make_payload(**kw) -> JobPayload:
    return JobPayload(
        job_class="tests.fake.Job",
        data={"x": 1},
        **kw,
    )


class TestPushPop:
    async def test_push_then_pop(self, driver):
        payload = _make_payload(queue="default")
        await driver.push(payload)
        popped = await driver.pop("default")
        assert popped is not None
        assert popped.id == payload.id
        assert popped.status == "processing"
        assert popped.attempts == 1

    async def test_pop_empty_queue_returns_none(self, driver):
        assert await driver.pop("nonexistent") is None

    async def test_push_many(self, driver):
        payloads = [_make_payload(queue="bulk") for _ in range(5)]
        ids = await driver.push_many(payloads)
        assert len(ids) == 5
        size = await driver.size("bulk")
        assert size == 5

    async def test_pop_delayed_only_promotes_expired(self, driver):
        # MemoryDriver eagerly promotes past-delays at push time; SQLiteDriver
        # lazily promotes via pop_delayed. To exercise both consistently we use
        # a tiny future delay + sleep, plus a long delay that stays put.
        short_payload = _make_payload(queue="delayed_q", delay_until=_now_ts() + 0.1)
        long_payload = _make_payload(queue="delayed_q", delay_until=_now_ts() + 60)
        await driver.push(short_payload)
        await driver.push(long_payload)
        await asyncio.sleep(0.25)

        moved = await driver.pop_delayed()
        assert len(moved) == 1
        assert moved[0].id == short_payload.id


class TestJobLifecycle:
    async def test_complete(self, driver):
        payload = _make_payload(queue="default")
        await driver.push(payload)
        popped = await driver.pop("default")
        await driver.complete(popped)
        after = await driver.get_job(popped.id)
        assert after.status == "completed"
        assert after.completed_at is not None

    async def test_fail(self, driver):
        payload = _make_payload(queue="default")
        await driver.push(payload)
        popped = await driver.pop("default")
        await driver.fail(popped, "intentional")
        after = await driver.get_job(popped.id)
        assert after.status == "failed"
        assert after.failed_at is not None
        assert after.error == "intentional"

    async def test_release_no_delay_returns_to_queue(self, driver):
        payload = _make_payload(queue="default")
        await driver.push(payload)
        popped = await driver.pop("default")
        await driver.release(popped, delay=0)
        again = await driver.pop("default")
        assert again is not None
        assert again.id == popped.id

    async def test_release_with_delay_goes_to_delayed_pool(self, driver):
        payload = _make_payload(queue="default")
        await driver.push(payload)
        popped = await driver.pop("default")
        await driver.release(popped, delay=120)
        # Should not be immediately available
        assert await driver.pop("default") is None

    async def test_delete_removes_job(self, driver):
        payload = _make_payload(queue="default")
        await driver.push(payload)
        await driver.delete(payload.id)
        assert await driver.get_job(payload.id) is None


class TestQueries:
    async def test_get_jobs_filter_by_status(self, driver):
        a = _make_payload(queue="default")
        b = _make_payload(queue="default")
        await driver.push(a)
        await driver.push(b)
        await driver.fail(a, "boom")
        failed = await driver.get_jobs(status="failed", offset=0, limit=10)
        assert len(failed) == 1
        assert failed[0].id == a.id

    async def test_get_jobs_filter_by_queue(self, driver):
        a = _make_payload(queue="emails")
        b = _make_payload(queue="reports")
        await driver.push(a)
        await driver.push(b)
        emails = await driver.get_jobs(queue="emails", offset=0, limit=10)
        assert len(emails) == 1

    async def test_get_jobs_filter_by_tag(self, driver):
        a = _make_payload(queue="default", tags=["critical", "urgent"])
        b = _make_payload(queue="default", tags=["low"])
        await driver.push(a)
        await driver.push(b)
        critical = await driver.get_jobs(tag="critical", offset=0, limit=10)
        assert len(critical) == 1
        assert critical[0].id == a.id

    async def test_get_jobs_pagination(self, driver):
        for _ in range(5):
            await driver.push(_make_payload(queue="default"))
        page1 = await driver.get_jobs(queue="default", offset=0, limit=2)
        page2 = await driver.get_jobs(queue="default", offset=2, limit=2)
        assert len(page1) == 2
        assert len(page2) == 2
        ids1 = {j.id for j in page1}
        ids2 = {j.id for j in page2}
        assert ids1.isdisjoint(ids2)

    async def test_count_jobs(self, driver):
        for _ in range(7):
            await driver.push(_make_payload(queue="default"))
        assert await driver.count_jobs(queue="default") == 7
        assert await driver.count_jobs(status="pending") == 7
        assert await driver.count_jobs(status="failed") == 0

    async def test_count_with_created_from(self, driver):
        await driver.push(_make_payload(queue="default"))
        cutoff = _now_ts()
        await asyncio.sleep(0.01)
        await driver.push(_make_payload(queue="default"))
        await driver.push(_make_payload(queue="default"))
        count_after = await driver.count_jobs(created_from=cutoff)
        # At least the 2 we pushed after cutoff
        assert count_after >= 2

    async def test_queues_lists_all_seen(self, driver):
        await driver.push(_make_payload(queue="q1"))
        await driver.push(_make_payload(queue="q2"))
        await driver.push(_make_payload(queue="q3"))
        names = await driver.queues()
        assert {"q1", "q2", "q3"}.issubset(set(names))


class TestPruning:
    async def test_prune_by_status(self, driver):
        a = _make_payload(queue="default")
        b = _make_payload(queue="default")
        await driver.push(a)
        await driver.push(b)
        await driver.complete(a)
        n = await driver.prune(status="completed")
        assert n == 1
        assert await driver.get_job(a.id) is None
        assert await driver.get_job(b.id) is not None

    async def test_prune_by_tag(self, driver):
        a = _make_payload(queue="default", tags=["x"])
        b = _make_payload(queue="default", tags=["y"])
        await driver.push(a)
        await driver.push(b)
        n = await driver.prune(tag="x")
        assert n == 1

    async def test_prune_by_queue(self, driver):
        await driver.push(_make_payload(queue="q1"))
        await driver.push(_make_payload(queue="q1"))
        await driver.push(_make_payload(queue="q2"))
        n = await driver.prune(queue="q1")
        assert n == 2

    async def test_bulk_delete_jobs(self, driver):
        a = _make_payload(queue="default")
        b = _make_payload(queue="default")
        c = _make_payload(queue="default")
        for p in (a, b, c):
            await driver.push(p)
        removed = await driver.bulk_delete_jobs([a.id, b.id])
        assert removed == 2
        assert await driver.get_job(a.id) is None
        assert await driver.get_job(b.id) is None
        assert await driver.get_job(c.id) is not None

    async def test_bulk_delete_jobs_limit(self, driver):
        ids = []
        for _ in range(3):
            p = _make_payload(queue="default")
            await driver.push(p)
            ids.append(p.id)
        removed = await driver.bulk_delete_jobs(ids, limit=1)
        assert removed == 1

    async def test_prune_terminal_jobs(self, driver):
        a = _make_payload(queue="default")
        b = _make_payload(queue="default")
        await driver.push(a)
        await driver.push(b)
        await driver.complete(a)
        removed = await driver.prune_terminal_jobs(status="completed")
        assert removed == 1
        assert await driver.get_job(a.id) is None
        assert await driver.get_job(b.id) is not None

    async def test_reconcile_indexes_noop(self, driver):
        # Drivers without secondary indexes report nothing to reconcile.
        await driver.push(_make_payload(queue="default"))
        assert await driver.reconcile_indexes() == 0

    async def test_flush_specific_queue(self, driver):
        await driver.push(_make_payload(queue="ephemeral"))
        await driver.push(_make_payload(queue="keep"))
        await driver.flush("ephemeral")
        assert await driver.size("ephemeral") == 0
        assert await driver.size("keep") == 1

    async def test_flush_all(self, driver):
        await driver.push(_make_payload(queue="a"))
        await driver.push(_make_payload(queue="b"))
        await driver.flush()
        assert await driver.count_jobs() == 0


class TestMetrics:
    async def test_record_and_recent_throughput(self, driver):
        await driver.record_metric("default", "completed", 1)
        await driver.record_metric("default", "completed", 1)
        await driver.record_metric("default", "failed", 1)
        tp = await driver.recent_throughput(seconds=60)
        assert tp["completed"] >= 2
        assert tp["failed"] >= 1

    async def test_get_metrics_shape(self, driver):
        await driver.push(_make_payload(queue="x"))
        m = await driver.get_metrics()
        assert isinstance(m, dict)
        assert "x" in m

    async def test_get_metrics_reports_live_job_status_counts(self, driver):
        pending = _make_payload(queue="x")
        completed = _make_payload(queue="x")
        failed = _make_payload(queue="x")
        other_queue = _make_payload(queue="y")

        await driver.push(pending)
        await driver.push(completed)
        await driver.push(failed)
        await driver.push(other_queue)
        await driver.complete(completed)
        await driver.fail(failed, "boom")

        # Extra metric events must not inflate live Overview counters.
        await driver.record_metric("x", "completed", 1)
        await driver.record_metric("x", "completed", 1)
        await driver.record_metric("x", "failed", 1)

        m = await driver.get_metrics()
        assert isinstance(m, dict)
        assert m["x"]["pending"] == 1
        assert m["x"]["completed"] == 1
        assert m["x"]["failed"] == 1
        assert m["y"]["pending"] == 1


class TestSupervisorReporting:
    async def test_report_and_get(self, driver):
        await driver.report_supervisor({
            "name": "sup-test",
            "running": True,
            "workers": 3,
            "queues": ["default"],
            "worker_stats": [],
        })
        stats = await driver.get_supervisor_stats(stale_after=30.0)
        assert any(s.get("name") == "sup-test" for s in stats)

    async def test_stopped_supervisors_filtered_out(self, driver):
        await driver.report_supervisor({
            "name": "sup-stopped",
            "running": False,
            "workers": 0,
            "queues": [],
            "worker_stats": [],
        })
        stats = await driver.get_supervisor_stats(stale_after=30.0)
        assert not any(s.get("name") == "sup-stopped" for s in stats)


class TestBatchHelpers:
    async def test_store_and_get(self, driver):
        await driver.store_batch("b1", {"id": "b1", "total": 5, "completed_count": 0, "failed_count": 0})
        b = await driver.get_batch("b1")
        assert b is not None
        assert b["total"] == 5

    async def test_increment_counter(self, driver):
        await driver.store_batch("b1", {"id": "b1", "total": 3, "completed_count": 0, "failed_count": 0})
        await driver.increment_batch_counter("b1", "completed_count", 1)
        await driver.increment_batch_counter("b1", "completed_count", 1)
        b = await driver.get_batch("b1")
        assert b["completed_count"] == 2

    async def test_increment_missing_batch_returns_none(self, driver):
        result = await driver.increment_batch_counter("nope", "completed_count", 1)
        assert result is None
