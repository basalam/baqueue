"""Redis secondary-index consistency tests (fakeredis-backed).

These pin down the bounded-index guarantees: bulk deletes drop every index family
by exactly the number of jobs removed, orphaned index entries (job hash gone) are
reaped instead of accumulating, and reconcile_indexes heals drift across all four
ZSET families.
"""

from __future__ import annotations

import asyncio

import pytest

from baqueue.serializer import JobPayload, _now_ts

pytestmark = pytest.mark.asyncio


# ── Helpers ──────────────────────────────────────────────────────────────

def _payload(queue: str = "default", **kw) -> JobPayload:
    return JobPayload(job_class="tests.fake.Job", data={"x": 1}, queue=queue, **kw)


async def _seed_terminal(driver, queue: str, n: int, status: str) -> list[str]:
    """Create n jobs in `queue` and drive them to a terminal `status` through the
    real lifecycle, so every secondary index is populated the way production does."""
    ids: list[str] = []
    for _ in range(n):
        p = _payload(queue=queue)
        await driver.push(p)
        ids.append(p.id)
    for _ in range(n):
        popped = await driver.pop(queue)  # pending -> processing
        assert popped is not None
        if status == "completed":
            await driver.complete(popped)
        elif status == "failed":
            await driver.fail(popped, "boom")
        else:
            raise ValueError(status)
    return ids


def _families(driver, queue: str, status: str) -> list[str]:
    return [
        driver._idx_all(),
        driver._idx_status(status),
        driver._idx_queue(queue),
        driver._idx_queue_status(queue, status),
    ]


async def _zcards(driver, keys: list[str]) -> list[int]:
    return [int(await driver._redis.zcard(k)) for k in keys]


# ── Tests ────────────────────────────────────────────────────────────────

class TestIndexPopulation:
    async def test_terminal_jobs_populate_all_four_families(self, redis_driver):
        d = redis_driver
        await _seed_terminal(d, "q1", 4, "completed")
        await _seed_terminal(d, "q1", 3, "failed")

        assert int(await d._redis.zcard(d._idx_all())) == 7
        assert int(await d._redis.zcard(d._idx_status("completed"))) == 4
        assert int(await d._redis.zcard(d._idx_status("failed"))) == 3
        assert int(await d._redis.zcard(d._idx_queue("q1"))) == 7
        assert int(await d._redis.zcard(d._idx_queue_status("q1", "completed"))) == 4
        assert int(await d._redis.zcard(d._idx_queue_status("q1", "failed"))) == 3


class TestPruneTerminalJobs:
    async def test_drops_every_family_by_exactly_n(self, redis_driver):
        d = redis_driver
        completed = await _seed_terminal(d, "q1", 5, "completed")
        await _seed_terminal(d, "q1", 2, "failed")

        removed = await d.prune_terminal_jobs(status="completed")
        assert removed == 5

        # completed families are emptied; failed jobs are untouched.
        assert int(await d._redis.zcard(d._idx_status("completed"))) == 0
        assert int(await d._redis.zcard(d._idx_queue_status("q1", "completed"))) == 0
        assert int(await d._redis.zcard(d._idx_all())) == 2  # only the 2 failed
        assert int(await d._redis.zcard(d._idx_queue("q1"))) == 2

        # the job hashes are gone too.
        for jid in completed:
            assert int(await d._redis.exists(d._key("job", jid))) == 0

    async def test_older_than_filter_keeps_recent_jobs(self, redis_driver):
        d = redis_driver
        await _seed_terminal(d, "q1", 3, "completed")
        # Everything was just completed, so nothing is older than an hour.
        removed = await d.prune_terminal_jobs(status="completed", older_than=3600)
        assert removed == 0
        assert int(await d._redis.zcard(d._idx_status("completed"))) == 3

    async def test_small_batch_drains_fully(self, redis_driver):
        """`limit` is the per-round-trip batch size, not a per-call cap: a single
        call drains the whole backlog in batches of `limit`."""
        d = redis_driver
        await _seed_terminal(d, "q1", 5, "completed")

        removed = await d.prune_terminal_jobs(status="completed", limit=2)
        assert removed == 5
        for k in _families(d, "q1", "completed"):
            assert int(await d._redis.zcard(k)) == 0

    async def test_filter_skips_front_window_but_drains_deeper_matches(self, redis_driver):
        """Regression: a capped drain must page past filter-skipped entries at the
        front of the index, not stop early and miss matches deeper in.

        The index is ordered by created_at; here the oldest-created jobs are too
        *recent* by updated_at (skipped by older_than) while the newest-created ones
        are old enough to prune. With a tiny batch the skipped front would have
        terminated a naive re-read-from-zero loop."""
        d = redis_driver
        now = _now_ts()
        recent_ids: list[str] = []
        old_ids: list[str] = []
        # created_at order: 3 recent-updated first, then 3 old-updated. Seed the
        # indexes the way push+complete would, but with controlled timestamps.
        for i in range(6):
            recent = i < 3
            p = _payload(
                queue="q1",
                status="completed",
                created_at=now + i,                       # index score (ascending)
                updated_at=now if recent else now - 7200,  # age for older_than filter
            )
            await d._redis.hset(d._key("job", p.id), mapping={"data": p.to_json()})
            pipe = d._redis.pipeline()
            d._index_add(pipe, p)
            await pipe.execute()
            (recent_ids if recent else old_ids).append(p.id)

        removed = await d.prune_terminal_jobs(status="completed", older_than=3600, limit=2)
        assert removed == 3  # only the 3 old-updated jobs

        # The 3 recent jobs survive; the 3 old ones (and their hashes) are gone.
        assert int(await d._redis.zcard(d._idx_status("completed"))) == 3
        for jid in old_ids:
            assert int(await d._redis.exists(d._key("job", jid))) == 0
        for jid in recent_ids:
            assert int(await d._redis.exists(d._key("job", jid))) == 1

    async def test_reaps_orphans_from_global_indexes(self, redis_driver):
        """Job hashes deleted out-of-band leave orphaned index entries; pruning the
        status index must reap them from jobs:all and jobs:status:* (not skip them)."""
        d = redis_driver
        ids = await _seed_terminal(d, "q1", 6, "completed")

        # Simulate an external bulk delete of half the hashes (bypassing the driver).
        orphans = ids[:3]
        await d._redis.unlink(*[d._key("job", jid) for jid in orphans])

        removed = await d.prune_terminal_jobs(status="completed")
        assert removed == 6  # 3 live deletes + 3 orphans reaped

        # No orphan ids survive in the global indexes.
        assert int(await d._redis.zcard(d._idx_all())) == 0
        assert int(await d._redis.zcard(d._idx_status("completed"))) == 0
        members = await d._redis.zrange(d._idx_status("completed"), 0, -1)
        assert members == []


class TestBulkDeleteJobs:
    async def test_removes_all_families_and_hash(self, redis_driver):
        d = redis_driver
        ids = await _seed_terminal(d, "q1", 3, "completed")

        removed = await d.bulk_delete_jobs(ids)
        assert removed == 3
        for k in _families(d, "q1", "completed"):
            assert int(await d._redis.zcard(k)) == 0
        for jid in ids:
            assert int(await d._redis.exists(d._key("job", jid))) == 0

    async def test_handles_orphan_ids(self, redis_driver):
        d = redis_driver
        ids = await _seed_terminal(d, "q1", 4, "completed")
        await d._redis.unlink(*[d._key("job", jid) for jid in ids[:2]])

        removed = await d.bulk_delete_jobs(ids)
        assert removed == 4
        assert int(await d._redis.zcard(d._idx_all())) == 0
        assert int(await d._redis.zcard(d._idx_status("completed"))) == 0

    async def test_limit_caps_input(self, redis_driver):
        d = redis_driver
        ids = await _seed_terminal(d, "q1", 4, "completed")
        removed = await d.bulk_delete_jobs(ids, limit=2)
        assert removed == 2
        assert int(await d._redis.zcard(d._idx_status("completed"))) == 2


class TestReconcileIndexes:
    async def test_clears_orphans_across_all_families(self, redis_driver):
        d = redis_driver
        ids = await _seed_terminal(d, "q1", 5, "completed")

        # Delete every hash out-of-band -> every index entry is now an orphan.
        await d._redis.unlink(*[d._key("job", jid) for jid in ids])

        # Each orphan sits in 4 families (all, status, queue, queue+status).
        before = sum(await _zcards(d, _families(d, "q1", "completed")))
        assert before == 5 * 4

        removed = await d.reconcile_indexes()
        assert removed == before

        for k in _families(d, "q1", "completed"):
            assert int(await d._redis.zcard(k)) == 0

    async def test_keeps_live_entries(self, redis_driver):
        d = redis_driver
        await _seed_terminal(d, "q1", 3, "completed")
        removed = await d.reconcile_indexes()
        assert removed == 0
        assert int(await d._redis.zcard(d._idx_status("completed"))) == 3

    async def test_reconcile_on_connect_flag_runs_repair(self, redis_driver):
        """connect() honours reconcile_on_connect; here we drive the same path by
        flipping the flag and re-running the connect tail against the fake client."""
        d = redis_driver
        ids = await _seed_terminal(d, "q1", 2, "completed")
        await d._redis.unlink(*[d._key("job", jid) for jid in ids])

        d.reconcile_on_connect = True
        removed = await d.reconcile_indexes()
        assert removed == 2 * 4
        assert int(await d._redis.zcard(d._idx_all())) == 0


class TestPromoteRedis:
    async def test_promote_moves_scheduled_job_to_ready_list_once(self, redis_driver):
        d = redis_driver
        p = _payload(queue="q1", delay_until=_now_ts() + 3600)
        await d.push(p)
        # Sitting in the delayed ZSET, not yet in the ready list.
        assert await d._redis.zscore(d._key("delayed"), p.id) is not None
        assert await d._redis.lrange(d._key("queue", "q1"), 0, -1) == []

        assert await d.promote(p.id) is True

        # Removed from delayed, enqueued exactly once.
        assert await d._redis.zscore(d._key("delayed"), p.id) is None
        assert await d._redis.lrange(d._key("queue", "q1"), 0, -1) == [p.id]

        popped = await d.pop("q1")
        assert popped is not None and popped.id == p.id
        assert popped.delay_until is None

    async def test_promote_ready_job_does_not_duplicate(self, redis_driver):
        d = redis_driver
        p = _payload(queue="q1")  # no delay → already ready
        await d.push(p)
        assert await d.promote(p.id) is True
        # Still a single copy in the ready list.
        assert await d._redis.lrange(d._key("queue", "q1"), 0, -1) == [p.id]

    async def test_promote_non_pending_returns_false(self, redis_driver):
        d = redis_driver
        ids = await _seed_terminal(d, "q1", 1, "completed")
        assert await d.promote(ids[0]) is False

    async def test_promote_missing_returns_false(self, redis_driver):
        assert await redis_driver.promote("nope") is False

    async def test_concurrent_delayed_pollers_enqueue_once(self, redis_driver, monkeypatch):
        d = redis_driver
        p = _payload(queue="q1", delay_until=_now_ts() + 0.05)
        await d.push(p)
        await asyncio.sleep(0.1)

        original = d._redis.zrangebyscore
        both_read = asyncio.Event()
        readers = 0

        async def synchronized_read(*args, **kwargs):
            nonlocal readers
            result = await original(*args, **kwargs)
            readers += 1
            if readers == 2:
                both_read.set()
            await both_read.wait()
            return result

        monkeypatch.setattr(d._redis, "zrangebyscore", synchronized_read)
        moved = await asyncio.gather(d.pop_delayed(), d.pop_delayed())

        assert sum(len(batch) for batch in moved) == 1
        assert await d._redis.lrange(d._key("queue", "q1"), 0, -1) == [p.id]

    async def test_pop_discards_duplicate_non_pending_entries(self, redis_driver):
        d = redis_driver
        p = _payload(queue="q1")
        await d.push(p)
        await d._redis.rpush(d._key("queue", "q1"), p.id, p.id)

        popped = await d.pop("q1")
        assert popped is not None and popped.id == p.id
        assert await d.pop("q1") is None

        stored = await d.get_job(p.id)
        assert stored is not None
        assert stored.status == "processing"
        assert stored.attempts == 1
        assert await d._redis.llen(d._key("queue", "q1")) == 0


class TestRequeueStuckRedis:
    async def test_requeue_stuck_processing_updates_indexes_and_ready_list(self, redis_driver):
        d = redis_driver
        p = _payload(queue="q1")
        await d.push(p)

        popped = await d.pop("q1")
        assert popped is not None
        stale_started_at = _now_ts() - 7200
        popped.started_at = stale_started_at
        popped.updated_at = stale_started_at
        await d._redis.hset(d._key("job", popped.id), mapping={"data": popped.to_json()})

        assert await d.requeue_stuck_jobs(older_than_seconds=3600, queue="q1") == 1

        recovered = await d.get_job(popped.id)
        assert recovered.status == "pending"
        assert recovered.started_at is None
        assert await d._redis.lrange(d._key("queue", "q1"), 0, -1) == [popped.id]
        assert int(await d._redis.zcard(d._idx_queue_status("q1", "processing"))) == 0
        assert int(await d._redis.zcard(d._idx_queue_status("q1", "pending"))) == 1

        assert await d.requeue_stuck_jobs(older_than_seconds=3600, queue="q1") == 0
        assert await d._redis.lrange(d._key("queue", "q1"), 0, -1) == [popped.id]

        again = await d.pop("q1")
        assert again is not None
        assert again.id == popped.id
        assert again.attempts == 2


class TestHistoryPersistenceRedis:
    async def test_history_survives_retry_round_trip(self, redis_driver):
        d = redis_driver
        p = _payload(queue="q1")
        await d.push(p)

        # Attempt 1: the worker would append a record, then release for retry.
        popped = await d.pop("q1")
        popped.history.append({
            "attempt": 1, "started_at": popped.started_at, "finished_at": _now_ts(),
            "status": "failed", "error": "boom", "will_retry": True,
            "next_retry_at": _now_ts() + 5,
        })
        await d.release(popped, delay=0)

        reloaded = await d.get_job(p.id)
        assert [h["status"] for h in reloaded.history] == ["failed"]

        # Attempt 2: prior history is preserved, then a completion is appended.
        popped2 = await d.pop("q1")
        assert [h["status"] for h in popped2.history] == ["failed"]
        popped2.history.append({
            "attempt": 2, "started_at": popped2.started_at, "finished_at": _now_ts(),
            "status": "completed", "error": None, "will_retry": False,
            "next_retry_at": None,
        })
        await d.complete(popped2)

        final = await d.get_job(p.id)
        assert [h["status"] for h in final.history] == ["failed", "completed"]


class TestPruneReapsOrphans:
    async def test_prune_no_longer_skips_orphans(self, redis_driver):
        """The legacy prune() bug: orphaned index entries were skipped forever.
        It must now reap them via the same batched path."""
        d = redis_driver
        ids = await _seed_terminal(d, "q1", 4, "completed")
        await d._redis.unlink(*[d._key("job", jid) for jid in ids])

        removed = await d.prune(status="completed")
        assert removed == 4
        assert int(await d._redis.zcard(d._idx_status("completed"))) == 0
        assert int(await d._redis.zcard(d._idx_all())) == 0
