"""In-memory driver for testing and development."""

from __future__ import annotations

import asyncio
from collections import defaultdict
from typing import Any

from baqueue.drivers.base import BaseDriver
from baqueue.serializer import JobPayload, _now_ts


class MemoryDriver(BaseDriver):
    """Stores everything in-memory. Ideal for tests and local dev."""

    def __init__(self) -> None:
        self._jobs: dict[str, JobPayload] = {}
        self._queues: dict[str, list[str]] = defaultdict(list)
        self._delayed: list[str] = []
        self._batches: dict[str, dict[str, Any]] = {}
        self._metrics: dict[str, list[dict[str, Any]]] = defaultdict(list)
        self._lock = asyncio.Lock()

    async def connect(self) -> None:
        pass

    async def disconnect(self) -> None:
        pass

    # ── Push / Pop ──────────────────────────────────────────────

    async def push(self, payload: JobPayload) -> str:
        async with self._lock:
            payload.status = "pending"
            payload.updated_at = _now_ts()
            self._jobs[payload.id] = payload
            if payload.delay_until and payload.delay_until > _now_ts():
                self._delayed.append(payload.id)
            else:
                self._queues[payload.queue].append(payload.id)
        return payload.id

    async def push_many(self, payloads: list[JobPayload]) -> list[str]:
        ids = []
        for p in payloads:
            ids.append(await self.push(p))
        return ids

    async def pop(self, queue: str) -> JobPayload | None:
        async with self._lock:
            q = self._queues.get(queue, [])
            while q:
                job_id = q.pop(0)
                payload = self._jobs.get(job_id)
                if payload and payload.status == "pending":
                    payload.status = "processing"
                    payload.started_at = _now_ts()
                    payload.updated_at = _now_ts()
                    payload.attempts += 1
                    return payload
            return None

    async def pop_delayed(self) -> list[JobPayload]:
        now = _now_ts()
        moved: list[JobPayload] = []
        async with self._lock:
            still_delayed = []
            for job_id in self._delayed:
                payload = self._jobs.get(job_id)
                if not payload:
                    continue
                if payload.delay_until and payload.delay_until <= now:
                    payload.delay_until = None
                    self._queues[payload.queue].append(job_id)
                    moved.append(payload)
                else:
                    still_delayed.append(job_id)
            self._delayed = still_delayed
        return moved

    # ── Job lifecycle ───────────────────────────────────────────

    async def complete(self, payload: JobPayload) -> None:
        async with self._lock:
            payload.status = "completed"
            payload.completed_at = _now_ts()
            payload.updated_at = _now_ts()
            self._jobs[payload.id] = payload

    async def fail(self, payload: JobPayload, error: str) -> None:
        async with self._lock:
            payload.status = "failed"
            payload.failed_at = _now_ts()
            payload.updated_at = _now_ts()
            payload.error = error
            self._jobs[payload.id] = payload

    async def release(self, payload: JobPayload, delay: float = 0) -> None:
        async with self._lock:
            payload.status = "pending"
            payload.updated_at = _now_ts()
            if delay > 0:
                payload.delay_until = _now_ts() + delay
                self._delayed.append(payload.id)
            else:
                self._queues[payload.queue].append(payload.id)
            self._jobs[payload.id] = payload

    async def delete(self, job_id: str) -> None:
        async with self._lock:
            self._jobs.pop(job_id, None)
            for q in self._queues.values():
                if job_id in q:
                    q.remove(job_id)
            if job_id in self._delayed:
                self._delayed.remove(job_id)

    # ── Query ───────────────────────────────────────────────────

    async def get_job(self, job_id: str) -> JobPayload | None:
        return self._jobs.get(job_id)

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
        results = list(self._jobs.values())
        if queue:
            results = [j for j in results if j.queue == queue]
        if status:
            results = [j for j in results if j.status == status]
        if tag:
            results = [j for j in results if tag in j.tags]
        if batch_id:
            results = [j for j in results if j.batch_id == batch_id]
        if created_from is not None:
            results = [j for j in results if j.created_at >= created_from]
        if created_to is not None:
            results = [j for j in results if j.created_at <= created_to]
        results.sort(key=lambda j: j.created_at, reverse=True)
        return results[offset : offset + limit]

    async def size(self, queue: str) -> int:
        return len(self._queues.get(queue, []))

    async def queues(self) -> list[str]:
        all_queues = set(self._queues.keys())
        for j in self._jobs.values():
            all_queues.add(j.queue)
        return sorted(all_queues)

    # ── Metrics ─────────────────────────────────────────────────

    async def record_metric(self, queue: str, metric: str, value: float) -> None:
        self._metrics[queue].append(
            {"metric": metric, "value": value, "time": _now_ts()}
        )

    async def get_metrics(self, queue: str | None = None) -> dict[str, Any]:
        queues_list = [queue] if queue else list(self._metrics.keys())
        result: dict[str, Any] = {}
        for q in queues_list:
            entries = self._metrics.get(q, [])
            throughput = sum(1 for e in entries if e["metric"] == "completed")
            failed = sum(1 for e in entries if e["metric"] == "failed")
            pending = await self.size(q)
            all_jobs = [j for j in self._jobs.values() if j.queue == q]
            processing = sum(1 for j in all_jobs if j.status == "processing")
            result[q] = {
                "pending": pending,
                "processing": processing,
                "completed": throughput,
                "failed": failed,
                "total_jobs": len(all_jobs),
            }
        return result

    # ── Batch helpers ───────────────────────────────────────────

    async def store_batch(self, batch_id: str, data: dict[str, Any]) -> None:
        self._batches[batch_id] = data

    async def get_batch(self, batch_id: str) -> dict[str, Any] | None:
        return self._batches.get(batch_id)

    async def update_batch(self, batch_id: str, data: dict[str, Any]) -> None:
        if batch_id in self._batches:
            self._batches[batch_id].update(data)

    # ── Pruning ─────────────────────────────────────────────────

    async def prune(
        self,
        status: str | None = None,
        tag: str | None = None,
        older_than_seconds: float | None = None,
        queue: str | None = None,
    ) -> int:
        now = _now_ts()
        to_delete: list[str] = []
        for job_id, job in self._jobs.items():
            if queue and job.queue != queue:
                continue
            if status and job.status != status:
                continue
            if tag and tag not in job.tags:
                continue
            if older_than_seconds and (now - job.updated_at) < older_than_seconds:
                continue
            to_delete.append(job_id)
        for job_id in to_delete:
            await self.delete(job_id)
        return len(to_delete)

    async def flush(self, queue: str | None = None) -> None:
        if queue:
            ids = [jid for jid, j in self._jobs.items() if j.queue == queue]
            for jid in ids:
                del self._jobs[jid]
            self._queues.pop(queue, None)
        else:
            self._jobs.clear()
            self._queues.clear()
            self._delayed.clear()
            self._batches.clear()
            self._metrics.clear()
