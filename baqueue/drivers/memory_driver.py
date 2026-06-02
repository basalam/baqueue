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
        self._supervisors: dict[str, dict[str, Any]] = {}
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

    async def count_jobs(
        self,
        queue: str | None = None,
        status: str | None = None,
        created_from: float | None = None,
        created_to: float | None = None,
    ) -> int:
        n = 0
        for j in self._jobs.values():
            if queue and j.queue != queue:
                continue
            if status and j.status != status:
                continue
            if created_from is not None and j.created_at < created_from:
                continue
            if created_to is not None and j.created_at > created_to:
                continue
            n += 1
        return n

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
        """Live status counts from the jobs dict — never from the metrics event log."""
        if queue:
            queues_list = [queue]
        else:
            seen: set[str] = set(self._queues.keys())
            for j in self._jobs.values():
                seen.add(j.queue)
            queues_list = sorted(seen)

        result: dict[str, Any] = {
            q: {"pending": 0, "processing": 0, "completed": 0, "failed": 0}
            for q in queues_list
        }
        for j in self._jobs.values():
            counts = result.get(j.queue)
            if counts is None:
                continue
            if j.status in counts:
                counts[j.status] += 1
        return result

    async def report_supervisor(self, stats: dict[str, Any]) -> None:
        now = _now_ts()
        name = str(stats.get("name", "")).strip()
        if not name:
            return
        async with self._lock:
            self._supervisors[name] = {
                "data": dict(stats),
                "heartbeat_at": now,
            }

    async def get_supervisor_stats(self, stale_after: float = 10.0) -> list[dict[str, Any]]:
        cutoff = _now_ts() - stale_after
        out: list[dict[str, Any]] = []
        async with self._lock:
            for name in sorted(self._supervisors):
                item = self._supervisors[name]
                if item.get("heartbeat_at", 0.0) < cutoff:
                    continue
                data = dict(item.get("data", {}))
                if not data.get("running", False):
                    continue
                out.append(data)
        return out

    # ── Batch helpers ───────────────────────────────────────────

    async def store_batch(self, batch_id: str, data: dict[str, Any]) -> None:
        self._batches[batch_id] = data

    async def get_batch(self, batch_id: str) -> dict[str, Any] | None:
        return self._batches.get(batch_id)

    async def update_batch(self, batch_id: str, data: dict[str, Any]) -> None:
        if batch_id in self._batches:
            self._batches[batch_id].update(data)

    async def increment_batch_counter(
        self, batch_id: str, field: str, delta: int = 1,
    ) -> dict[str, Any] | None:
        async with self._lock:
            batch = self._batches.get(batch_id)
            if batch is None:
                return None
            batch[field] = batch.get(field, 0) + delta
            return dict(batch)

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

    async def prune_metrics(self, older_than_seconds: float) -> int:
        cutoff = _now_ts() - older_than_seconds
        removed = 0
        async with self._lock:
            for q, entries in list(self._metrics.items()):
                kept = [e for e in entries if e.get("time", 0) >= cutoff]
                removed += len(entries) - len(kept)
                self._metrics[q] = kept
        return removed

    async def recent_throughput(
        self, seconds: int = 60, queue: str | None = None,
    ) -> dict[str, int]:
        cutoff = _now_ts() - seconds
        result = {"processing": 0, "completed": 0, "failed": 0}
        queues = [queue] if queue else list(self._metrics.keys())
        for q in queues:
            for e in self._metrics.get(q, []):
                if e.get("time", 0) < cutoff:
                    continue
                m = e.get("metric")
                if m in result:
                    result[m] += 1
        return result

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
            self._supervisors.clear()
