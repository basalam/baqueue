"""Redis driver for BaQueue - high-throughput queue backend."""

from __future__ import annotations

import json
from typing import Any

from baqueue.drivers.base import BaseDriver
from baqueue.serializer import JobPayload, _now_ts


class RedisDriver(BaseDriver):
    """Redis-backed driver using sorted sets for delayed jobs and lists for queues.

    Key layout (prefix = "baqueue"):
        baqueue:queue:{name}         - LIST  (pending job ids, FIFO)
        baqueue:delayed              - ZSET  (job_id scored by delay_until)
        baqueue:job:{id}             - HASH  (full job payload)
        baqueue:queues               - SET   (known queue names)
        baqueue:metrics:{queue}      - LIST  (metric entries)
        baqueue:batch:{id}           - HASH  (batch metadata)
    """

    def __init__(self, url: str = "redis://localhost:6379/0", prefix: str = "baqueue", **kwargs: Any):
        self._url = url
        self._prefix = prefix
        self._kwargs = kwargs
        self._redis: Any = None

    def _key(self, *parts: str) -> str:
        return ":".join([self._prefix, *parts])

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

    async def disconnect(self) -> None:
        if self._redis:
            await self._redis.aclose()
            self._redis = None

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
        payload.status = "processing"
        payload.started_at = _now_ts()
        payload.updated_at = _now_ts()
        payload.attempts += 1
        await self._redis.hset(self._key("job", payload.id), mapping={"data": payload.to_json()})
        return payload

    async def pop_delayed(self) -> list[JobPayload]:
        now = _now_ts()
        job_ids = await self._redis.zrangebyscore(self._key("delayed"), "-inf", now)
        if not job_ids:
            return []

        moved: list[JobPayload] = []
        pipe = self._redis.pipeline()
        for job_id in job_ids:
            pipe.zrem(self._key("delayed"), job_id)
        await pipe.execute()

        for job_id in job_ids:
            raw = await self._redis.hget(self._key("job", job_id), "data")
            if raw:
                payload = JobPayload.from_json(raw)
                payload.delay_until = None
                payload.updated_at = _now_ts()
                await self._redis.hset(self._key("job", payload.id), mapping={"data": payload.to_json()})
                await self._redis.rpush(self._key("queue", payload.queue), payload.id)
                moved.append(payload)
        return moved

    # ── Job lifecycle ───────────────────────────────────────────

    async def complete(self, payload: JobPayload) -> None:
        payload.status = "completed"
        payload.completed_at = _now_ts()
        payload.updated_at = _now_ts()
        await self._redis.hset(self._key("job", payload.id), mapping={"data": payload.to_json()})

    async def fail(self, payload: JobPayload, error: str) -> None:
        payload.status = "failed"
        payload.failed_at = _now_ts()
        payload.updated_at = _now_ts()
        payload.error = error
        await self._redis.hset(self._key("job", payload.id), mapping={"data": payload.to_json()})

    async def release(self, payload: JobPayload, delay: float = 0) -> None:
        payload.status = "pending"
        payload.updated_at = _now_ts()
        if delay > 0:
            payload.delay_until = _now_ts() + delay
            await self._redis.hset(self._key("job", payload.id), mapping={"data": payload.to_json()})
            await self._redis.zadd(self._key("delayed"), {payload.id: payload.delay_until})
        else:
            await self._redis.hset(self._key("job", payload.id), mapping={"data": payload.to_json()})
            await self._redis.rpush(self._key("queue", payload.queue), payload.id)

    async def delete(self, job_id: str) -> None:
        raw = await self._redis.hget(self._key("job", job_id), "data")
        if raw:
            payload = JobPayload.from_json(raw)
            await self._redis.lrem(self._key("queue", payload.queue), 0, job_id)
        await self._redis.zrem(self._key("delayed"), job_id)
        await self._redis.delete(self._key("job", job_id))

    # ── Query ───────────────────────────────────────────────────

    async def get_job(self, job_id: str) -> JobPayload | None:
        raw = await self._redis.hget(self._key("job", job_id), "data")
        return JobPayload.from_json(raw) if raw else None

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
        cursor = "0"
        results: list[JobPayload] = []
        pattern = self._key("job", "*")

        while True:
            cursor, keys = await self._redis.scan(cursor=cursor, match=pattern, count=200)
            for key in keys:
                raw = await self._redis.hget(key, "data")
                if not raw:
                    continue
                job = JobPayload.from_json(raw)
                if queue and job.queue != queue:
                    continue
                if status and job.status != status:
                    continue
                if tag and tag not in job.tags:
                    continue
                if batch_id and job.batch_id != batch_id:
                    continue
                if created_from is not None and job.created_at < created_from:
                    continue
                if created_to is not None and job.created_at > created_to:
                    continue
                results.append(job)
            if cursor == "0" or cursor == 0:
                break

        results.sort(key=lambda j: j.created_at, reverse=True)
        return results[offset : offset + limit]

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
            pending = await self.size(q)
            result[q] = {
                "pending": pending,
                "processing": 0,
                "completed": completed,
                "failed": failed,
            }
        return result

    # ── Batch helpers ───────────────────────────────────────────

    async def store_batch(self, batch_id: str, data: dict[str, Any]) -> None:
        await self._redis.hset(self._key("batch", batch_id), mapping={"data": json.dumps(data)})

    async def get_batch(self, batch_id: str) -> dict[str, Any] | None:
        raw = await self._redis.hget(self._key("batch", batch_id), "data")
        return json.loads(raw) if raw else None

    async def update_batch(self, batch_id: str, data: dict[str, Any]) -> None:
        await self._redis.hset(self._key("batch", batch_id), mapping={"data": json.dumps(data)})

    # ── Pruning ─────────────────────────────────────────────────

    async def prune(
        self,
        status: str | None = None,
        tag: str | None = None,
        older_than_seconds: float | None = None,
        queue: str | None = None,
    ) -> int:
        now = _now_ts()
        pruned = 0
        cursor = "0"
        pattern = self._key("job", "*")

        while True:
            cursor, keys = await self._redis.scan(cursor=cursor, match=pattern, count=200)
            for key in keys:
                raw = await self._redis.hget(key, "data")
                if not raw:
                    continue
                job = JobPayload.from_json(raw)
                if queue and job.queue != queue:
                    continue
                if status and job.status != status:
                    continue
                if tag and tag not in job.tags:
                    continue
                if older_than_seconds and (now - job.updated_at) < older_than_seconds:
                    continue
                await self.delete(job.id)
                pruned += 1
            if cursor == "0" or cursor == 0:
                break
        return pruned

    async def flush(self, queue: str | None = None) -> None:
        if queue:
            await self._redis.delete(self._key("queue", queue))
            cursor = "0"
            pattern = self._key("job", "*")
            while True:
                cursor, keys = await self._redis.scan(cursor=cursor, match=pattern, count=200)
                for key in keys:
                    raw = await self._redis.hget(key, "data")
                    if raw:
                        job = JobPayload.from_json(raw)
                        if job.queue == queue:
                            await self._redis.delete(key)
                if cursor == "0" or cursor == 0:
                    break
            await self._redis.srem(self._key("queues"), queue)
        else:
            cursor = "0"
            while True:
                cursor, keys = await self._redis.scan(cursor=cursor, match=f"{self._prefix}:*", count=200)
                if keys:
                    await self._redis.delete(*keys)
                if cursor == "0" or cursor == 0:
                    break
