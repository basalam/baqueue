"""Redis driver for BaQueue - high-throughput queue backend."""

from __future__ import annotations

import json
import logging
from typing import Any

from baqueue.drivers.base import BaseDriver
from baqueue.serializer import JobPayload, _now_ts

logger = logging.getLogger("baqueue.redis")


class RedisDriver(BaseDriver):
    """Redis-backed driver using sorted sets for indexed pagination.

    Key layout (prefix = "baqueue"):
        baqueue:queue:{name}                          LIST  pending job ids (FIFO)
        baqueue:delayed                               ZSET  job_id scored by delay_until
        baqueue:job:{id}                              HASH  full job payload
        baqueue:queues                                SET   known queue names
        baqueue:metrics:{queue}                       LIST  metric entries (capped at 10k)
        baqueue:batch:{id}                            HASH  batch metadata

        # Secondary indexes scored by created_at — used by get_jobs / count_jobs
        baqueue:jobs:all                              ZSET  every job
        baqueue:jobs:queue:{queue}                    ZSET  jobs in a queue
        baqueue:jobs:status:{status}                  ZSET  jobs in a status
        baqueue:jobs:queue:{queue}:status:{status}    ZSET  jobs in (queue, status)

    Filtering by tag or batch_id falls back to a per-result scan of the
    most-specific applicable index — fast for small filtered sets, slow if
    the input set is huge. Filter by queue/status when possible.
    """

    def __init__(self, url: str = "redis://localhost:6379/0", prefix: str = "baqueue", **kwargs: Any):
        self._url = url
        self._prefix = prefix
        self._kwargs = kwargs
        self._redis: Any = None

    def _key(self, *parts: str) -> str:
        return ":".join([self._prefix, *parts])

    def _idx_all(self) -> str:
        return self._key("jobs", "all")

    def _idx_queue(self, queue: str) -> str:
        return self._key("jobs", "queue", queue)

    def _idx_status(self, status: str) -> str:
        return self._key("jobs", "status", status)

    def _idx_queue_status(self, queue: str, status: str) -> str:
        return self._key("jobs", "queue", queue, "status", status)

    def _index_key(self, queue: str | None, status: str | None) -> str:
        if queue and status:
            return self._idx_queue_status(queue, status)
        if queue:
            return self._idx_queue(queue)
        if status:
            return self._idx_status(status)
        return self._idx_all()

    def _supervisor_key(self, name: str) -> str:
        return self._key("supervisor", name)

    async def connect(self) -> None:
        try:
            import redis.asyncio as aioredis
        except ImportError:
            raise ImportError(
                "Redis driver requires the 'redis' package.\n"
                "Install it with: pip install baqueue[redis]"
            ) from None
        self._redis = aioredis.from_url(self._url, decode_responses=True, **self._kwargs)
        await self._redis.ping()
        await self._backfill_indexes_if_needed()

    async def disconnect(self) -> None:
        if self._redis:
            await self._redis.aclose()
            self._redis = None

    async def _backfill_indexes_if_needed(self) -> None:
        """One-time rebuild of secondary ZSETs for upgrades from a version
        that didn't maintain them. Safe to call on every connect — exits fast
        when the global index is non-empty."""
        if await self._redis.exists(self._idx_all()):
            return
        cursor: Any = "0"
        pattern = self._key("job", "*")
        backfilled = 0
        while True:
            cursor, keys = await self._redis.scan(cursor=cursor, match=pattern, count=500)
            if keys:
                pipe = self._redis.pipeline()
                for key in keys:
                    pipe.hget(key, "data")
                raws = await pipe.execute()
                pipe = self._redis.pipeline()
                for raw in raws:
                    if not raw:
                        continue
                    job = JobPayload.from_json(raw)
                    self._index_add(pipe, job)
                    backfilled += 1
                await pipe.execute()
            if cursor == "0" or cursor == 0:
                break
        if backfilled:
            logger.info("Backfilled %d job(s) into Redis secondary indexes", backfilled)

    # ── Index maintenance ──────────────────────────────────────

    def _index_add(self, pipe: Any, job: JobPayload) -> None:
        score = job.created_at
        pipe.zadd(self._idx_all(), {job.id: score})
        pipe.zadd(self._idx_queue(job.queue), {job.id: score})
        pipe.zadd(self._idx_status(job.status), {job.id: score})
        pipe.zadd(self._idx_queue_status(job.queue, job.status), {job.id: score})

    def _index_remove(self, pipe: Any, job_id: str, queue: str, status: str) -> None:
        pipe.zrem(self._idx_all(), job_id)
        pipe.zrem(self._idx_queue(queue), job_id)
        pipe.zrem(self._idx_status(status), job_id)
        pipe.zrem(self._idx_queue_status(queue, status), job_id)

    def _index_status_change(
        self, pipe: Any, job_id: str, queue: str, old_status: str, new_status: str, score: float,
    ) -> None:
        if old_status == new_status:
            return
        pipe.zrem(self._idx_status(old_status), job_id)
        pipe.zrem(self._idx_queue_status(queue, old_status), job_id)
        pipe.zadd(self._idx_status(new_status), {job_id: score})
        pipe.zadd(self._idx_queue_status(queue, new_status), {job_id: score})

    # ── Push / Pop ──────────────────────────────────────────────

    async def push(self, payload: JobPayload) -> str:
        payload.status = "pending"
        payload.updated_at = _now_ts()
        pipe = self._redis.pipeline()
        pipe.hset(self._key("job", payload.id), mapping={"data": payload.to_json()})
        pipe.sadd(self._key("queues"), payload.queue)

        if payload.delay_until and payload.delay_until > _now_ts():
            pipe.zadd(self._key("delayed"), {payload.id: payload.delay_until})
        else:
            pipe.rpush(self._key("queue", payload.queue), payload.id)

        self._index_add(pipe, payload)
        await pipe.execute()
        return payload.id

    async def push_many(self, payloads: list[JobPayload]) -> list[str]:
        pipe = self._redis.pipeline()
        ids = []
        now = _now_ts()
        for p in payloads:
            p.status = "pending"
            p.updated_at = now
            pipe.hset(self._key("job", p.id), mapping={"data": p.to_json()})
            pipe.sadd(self._key("queues"), p.queue)
            if p.delay_until and p.delay_until > now:
                pipe.zadd(self._key("delayed"), {p.id: p.delay_until})
            else:
                pipe.rpush(self._key("queue", p.queue), p.id)
            self._index_add(pipe, p)
            ids.append(p.id)
        await pipe.execute()
        return ids

    async def pop(self, queue: str) -> JobPayload | None:
        job_id = await self._redis.lpop(self._key("queue", queue))
        if not job_id:
            return None
        raw = await self._redis.hget(self._key("job", job_id), "data")
        if not raw:
            return None
        payload = JobPayload.from_json(raw)
        old_status = payload.status
        payload.status = "processing"
        now = _now_ts()
        payload.started_at = now
        payload.updated_at = now
        payload.attempts += 1

        pipe = self._redis.pipeline()
        pipe.hset(self._key("job", payload.id), mapping={"data": payload.to_json()})
        self._index_status_change(pipe, payload.id, payload.queue, old_status, payload.status, payload.created_at)
        await pipe.execute()
        return payload

    async def pop_delayed(self) -> list[JobPayload]:
        now = _now_ts()
        job_ids = await self._redis.zrangebyscore(self._key("delayed"), "-inf", now)
        if not job_ids:
            return []

        pipe = self._redis.pipeline()
        for job_id in job_ids:
            pipe.zrem(self._key("delayed"), job_id)
        await pipe.execute()

        moved: list[JobPayload] = []
        for job_id in job_ids:
            raw = await self._redis.hget(self._key("job", job_id), "data")
            if raw:
                payload = JobPayload.from_json(raw)
                payload.delay_until = None
                payload.updated_at = _now_ts()
                pipe = self._redis.pipeline()
                pipe.hset(self._key("job", payload.id), mapping={"data": payload.to_json()})
                pipe.rpush(self._key("queue", payload.queue), payload.id)
                await pipe.execute()
                moved.append(payload)
        return moved

    # ── Job lifecycle ───────────────────────────────────────────

    async def _transition(self, payload: JobPayload, new_status: str) -> None:
        old_status = payload.status
        payload.status = new_status
        payload.updated_at = _now_ts()
        pipe = self._redis.pipeline()
        pipe.hset(self._key("job", payload.id), mapping={"data": payload.to_json()})
        self._index_status_change(pipe, payload.id, payload.queue, old_status, new_status, payload.created_at)
        await pipe.execute()

    async def complete(self, payload: JobPayload) -> None:
        payload.completed_at = _now_ts()
        await self._transition(payload, "completed")

    async def fail(self, payload: JobPayload, error: str) -> None:
        payload.failed_at = _now_ts()
        payload.error = error
        await self._transition(payload, "failed")

    async def release(self, payload: JobPayload, delay: float = 0) -> None:
        old_status = payload.status
        payload.status = "pending"
        payload.updated_at = _now_ts()

        pipe = self._redis.pipeline()
        if delay > 0:
            payload.delay_until = _now_ts() + delay
            pipe.hset(self._key("job", payload.id), mapping={"data": payload.to_json()})
            pipe.zadd(self._key("delayed"), {payload.id: payload.delay_until})
        else:
            pipe.hset(self._key("job", payload.id), mapping={"data": payload.to_json()})
            pipe.rpush(self._key("queue", payload.queue), payload.id)
        self._index_status_change(pipe, payload.id, payload.queue, old_status, "pending", payload.created_at)
        await pipe.execute()

    async def delete(self, job_id: str) -> None:
        raw = await self._redis.hget(self._key("job", job_id), "data")
        pipe = self._redis.pipeline()
        if raw:
            payload = JobPayload.from_json(raw)
            pipe.lrem(self._key("queue", payload.queue), 0, job_id)
            self._index_remove(pipe, job_id, payload.queue, payload.status)
        pipe.zrem(self._key("delayed"), job_id)
        pipe.delete(self._key("job", job_id))
        await pipe.execute()

    # ── Query ───────────────────────────────────────────────────

    async def get_job(self, job_id: str) -> JobPayload | None:
        raw = await self._redis.hget(self._key("job", job_id), "data")
        return JobPayload.from_json(raw) if raw else None

    async def _ids_from_index(
        self,
        index: str,
        offset: int,
        limit: int,
        created_from: float | None,
        created_to: float | None,
    ) -> list[str]:
        if created_from is None and created_to is None:
            return await self._redis.zrevrange(index, offset, offset + limit - 1)
        max_score: float | str = "+inf" if created_to is None else created_to
        min_score: float | str = "-inf" if created_from is None else created_from
        return await self._redis.zrevrangebyscore(
            index, max_score, min_score, start=offset, num=limit,
        )

    async def get_jobs(
        self,
        queue: str | None = None,
        status: str | None = None,
        tag: str | None = None,
        batch_id: str | None = None,
        offset: int = 0,
        limit: int = 50,
        created_from: float | None = None,
        created_to: float | None = None,
    ) -> list[JobPayload]:
        index = self._index_key(queue, status)

        # When tag/batch_id filtering is needed we have to over-fetch and post-filter.
        # Cap the over-fetch to avoid pathological cases.
        needs_post_filter = bool(tag or batch_id)
        fetch_offset = 0 if needs_post_filter else offset
        fetch_limit = max(limit * 10, 200) if needs_post_filter else limit

        ids = await self._ids_from_index(
            index, fetch_offset, fetch_limit, created_from, created_to,
        )
        if not ids:
            return []

        pipe = self._redis.pipeline()
        for jid in ids:
            pipe.hget(self._key("job", jid), "data")
        raws = await pipe.execute()

        results: list[JobPayload] = []
        for raw in raws:
            if not raw:
                continue
            job = JobPayload.from_json(raw)
            if tag and tag not in job.tags:
                continue
            if batch_id and job.batch_id != batch_id:
                continue
            results.append(job)

        if needs_post_filter:
            results = results[offset : offset + limit]
        return results

    async def count_jobs(
        self,
        queue: str | None = None,
        status: str | None = None,
        created_from: float | None = None,
        created_to: float | None = None,
    ) -> int:
        index = self._index_key(queue, status)
        if created_from is None and created_to is None:
            return int(await self._redis.zcard(index))
        min_score = "-inf" if created_from is None else created_from
        max_score = "+inf" if created_to is None else created_to
        return int(await self._redis.zcount(index, min_score, max_score))

    async def size(self, queue: str) -> int:
        return await self._redis.llen(self._key("queue", queue))

    async def queues(self) -> list[str]:
        members = await self._redis.smembers(self._key("queues"))
        return sorted(members)

    # ── Metrics ─────────────────────────────────────────────────

    async def record_metric(self, queue: str, metric: str, value: float) -> None:
        entry = json.dumps({"metric": metric, "value": value, "time": _now_ts()})
        await self._redis.rpush(self._key("metrics", queue), entry)
        await self._redis.ltrim(self._key("metrics", queue), -10000, -1)

    async def get_metrics(self, queue: str | None = None) -> dict[str, Any]:
        if queue:
            queue_names = [queue]
        else:
            queue_names = await self.queues()

        result: dict[str, Any] = {}
        for q in queue_names:
            entries_raw = await self._redis.lrange(self._key("metrics", q), 0, -1)
            entries = [json.loads(e) for e in entries_raw]
            completed = sum(1 for e in entries if e["metric"] == "completed")
            failed = sum(1 for e in entries if e["metric"] == "failed")
            pending = await self._redis.zcard(self._idx_queue_status(q, "pending"))
            processing = await self._redis.zcard(self._idx_queue_status(q, "processing"))
            result[q] = {
                "pending": int(pending),
                "processing": int(processing),
                "completed": completed,
                "failed": failed,
            }
        return result

    async def report_supervisor(self, stats: dict[str, Any]) -> None:
        name = str(stats.get("name", "")).strip()
        if not name:
            return
        ts = _now_ts()
        key = self._supervisor_key(name)
        payload = json.dumps(stats)
        pipe = self._redis.pipeline()
        pipe.hset(key, mapping={"data": payload, "heartbeat_at": ts})
        pipe.sadd(self._key("supervisors"), name)
        await pipe.execute()

    async def get_supervisor_stats(self, stale_after: float = 10.0) -> list[dict[str, Any]]:
        names = await self._redis.smembers(self._key("supervisors"))
        if not names:
            return []
        cutoff = _now_ts() - stale_after

        ordered = sorted(names)
        pipe = self._redis.pipeline()
        for name in ordered:
            pipe.hmget(self._supervisor_key(name), "data", "heartbeat_at")
        rows = await pipe.execute()

        out: list[dict[str, Any]] = []
        for row in rows:
            if not row:
                continue
            raw_data, raw_heartbeat = row
            if not raw_data:
                continue
            try:
                heartbeat = float(raw_heartbeat) if raw_heartbeat is not None else 0.0
            except (TypeError, ValueError):
                heartbeat = 0.0
            if heartbeat < cutoff:
                continue
            try:
                data = json.loads(raw_data)
            except (TypeError, ValueError):
                continue
            if not isinstance(data, dict):
                continue
            if not data.get("running", False):
                continue
            out.append(data)
        return out

    # ── Batch helpers ───────────────────────────────────────────

    async def store_batch(self, batch_id: str, data: dict[str, Any]) -> None:
        await self._redis.hset(self._key("batch", batch_id), mapping={"data": json.dumps(data)})

    async def get_batch(self, batch_id: str) -> dict[str, Any] | None:
        raw = await self._redis.hget(self._key("batch", batch_id), "data")
        return json.loads(raw) if raw else None

    async def update_batch(self, batch_id: str, data: dict[str, Any]) -> None:
        await self._redis.hset(self._key("batch", batch_id), mapping={"data": json.dumps(data)})

    # Lua: atomically read the batch JSON, bump one numeric field, write back.
    # KEYS[1] = batch hash key, ARGV[1] = field, ARGV[2] = delta (int as string).
    _BATCH_INCR_LUA = """
        local raw = redis.call('HGET', KEYS[1], 'data')
        if not raw then return nil end
        local data = cjson.decode(raw)
        local delta = tonumber(ARGV[2]) or 0
        data[ARGV[1]] = (tonumber(data[ARGV[1]]) or 0) + delta
        local out = cjson.encode(data)
        redis.call('HSET', KEYS[1], 'data', out)
        return out
    """

    async def increment_batch_counter(
        self, batch_id: str, field: str, delta: int = 1,
    ) -> dict[str, Any] | None:
        raw = await self._redis.eval(
            self._BATCH_INCR_LUA, 1, self._key("batch", batch_id), field, str(delta),
        )
        if raw is None:
            return None
        return json.loads(raw)

    # ── Pruning ─────────────────────────────────────────────────

    async def prune(
        self,
        status: str | None = None,
        tag: str | None = None,
        older_than_seconds: float | None = None,
        queue: str | None = None,
    ) -> int:
        if not (status or tag or older_than_seconds or queue):
            return 0

        index = self._index_key(queue, status)
        candidate_ids: list[str] = await self._redis.zrange(index, 0, -1)
        if not candidate_ids:
            return 0

        pipe = self._redis.pipeline()
        for jid in candidate_ids:
            pipe.hget(self._key("job", jid), "data")
        raws = await pipe.execute()

        now = _now_ts()
        to_delete: list[JobPayload] = []
        for raw in raws:
            if not raw:
                continue
            job = JobPayload.from_json(raw)
            if tag and tag not in job.tags:
                continue
            if older_than_seconds and (now - job.updated_at) < older_than_seconds:
                continue
            to_delete.append(job)

        if not to_delete:
            return 0

        pipe = self._redis.pipeline()
        for job in to_delete:
            pipe.lrem(self._key("queue", job.queue), 0, job.id)
            pipe.zrem(self._key("delayed"), job.id)
            pipe.delete(self._key("job", job.id))
            self._index_remove(pipe, job.id, job.queue, job.status)
        await pipe.execute()
        return len(to_delete)

    async def prune_metrics(self, older_than_seconds: float) -> int:
        # The metrics LIST is already capped at 10k entries via LTRIM in
        # record_metric, so no pruning is needed beyond what's already happening.
        return 0

    async def recent_throughput(
        self, seconds: int = 60, queue: str | None = None,
    ) -> dict[str, int]:
        cutoff = _now_ts() - seconds
        out = {"processing": 0, "completed": 0, "failed": 0}
        queue_names = [queue] if queue else await self.queues()
        for q in queue_names:
            entries_raw = await self._redis.lrange(self._key("metrics", q), 0, -1)
            for raw in entries_raw:
                try:
                    e = json.loads(raw)
                except (ValueError, TypeError):
                    continue
                if e.get("time", 0) < cutoff:
                    continue
                m = e.get("metric")
                if m in out:
                    out[m] += 1
        return out

    async def flush(self, queue: str | None = None) -> None:
        if queue:
            await self._redis.delete(self._key("queue", queue))
            ids = await self._redis.zrange(self._idx_queue(queue), 0, -1)
            if ids:
                pipe = self._redis.pipeline()
                for jid in ids:
                    pipe.delete(self._key("job", jid))
                    pipe.zrem(self._idx_all(), jid)
                    pipe.zrem(self._key("delayed"), jid)
                await pipe.execute()
            # Drop all per-queue and per-(queue,status) indexes
            pipe = self._redis.pipeline()
            pipe.delete(self._idx_queue(queue))
            for st in ("pending", "processing", "completed", "failed"):
                pipe.delete(self._idx_queue_status(queue, st))
            await pipe.execute()
            await self._redis.srem(self._key("queues"), queue)
        else:
            cursor: Any = "0"
            while True:
                cursor, keys = await self._redis.scan(cursor=cursor, match=f"{self._prefix}:*", count=200)
                if keys:
                    await self._redis.delete(*keys)
                if cursor == "0" or cursor == 0:
                    break
