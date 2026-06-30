"""Abstract base driver for BaQueue."""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from typing import Any, Awaitable, Callable

from baqueue.serializer import JobPayload

logger = logging.getLogger("baqueue.driver")

# Default per-call cap for batched bulk-delete / prune operations. Keeps a single
# call from blocking the backend on very large datasets; callers loop to drain.
DEFAULT_PRUNE_BATCH = 1000


class BaseDriver(ABC):
    """Every BaQueue driver must implement this interface."""

    # When True, write operations that fail with a storage-full error trigger
    # an emergency cleanup and one retry. Wired from BaQueueConfig in queue.py.
    auto_cleanup_on_disk_full: bool = True

    # When True, connect() runs a one-shot reconcile_indexes() pass to heal any
    # secondary-index drift accumulated while offline. Off by default so connect
    # stays fast on large datasets. Wired from BaQueueConfig in queue.py.
    reconcile_on_connect: bool = False

    # Re-entrancy guard so emergency_cleanup() doesn't recurse if its own
    # prune calls also hit disk-full.
    _in_emergency_cleanup: bool = False

    def is_storage_full_error(self, exc: BaseException) -> bool:
        """Subclasses override to detect their driver-specific storage-exhausted errors
        (SQLite "database or disk is full", Postgres disk_full, Redis OOM, etc.)."""
        return False

    async def emergency_cleanup(self) -> int:
        """Aggressively free space: purge every terminal job + old metrics.
        Returns the count of removed entries. Subclasses may override for
        a more targeted sweep."""
        total = 0
        for status in ("completed", "failed", "cancelled"):
            try:
                total += await self.prune(status=status)
            except Exception:
                logger.exception("emergency_cleanup: prune(status=%s) failed", status)
        try:
            total += await self.prune_metrics(older_than_seconds=0)
        except Exception:
            logger.exception("emergency_cleanup: prune_metrics failed")
        logger.warning("emergency_cleanup removed %d entries due to storage pressure", total)
        return total

    async def _with_disk_full_recovery(self, fn: Callable[[], Awaitable[Any]]) -> Any:
        """Run an async write; on storage-full error, run emergency cleanup once and retry.
        Drivers wrap their write paths with this helper."""
        if not self.auto_cleanup_on_disk_full or self._in_emergency_cleanup:
            return await fn()
        try:
            return await fn()
        except Exception as e:
            if not self.is_storage_full_error(e):
                raise
            logger.warning("Storage-full error caught: %s. Running emergency cleanup.", e)
            self._in_emergency_cleanup = True
            try:
                await self.emergency_cleanup()
            finally:
                self._in_emergency_cleanup = False
            return await fn()

    @abstractmethod
    async def connect(self) -> None: ...

    @abstractmethod
    async def disconnect(self) -> None: ...

    # ── Push / Pop ──────────────────────────────────────────────

    @abstractmethod
    async def push(self, payload: JobPayload) -> str:
        """Push a job onto the queue. Returns the job id."""
        ...

    @abstractmethod
    async def push_many(self, payloads: list[JobPayload]) -> list[str]:
        """Push many jobs at once (bulk insert)."""
        ...

    @abstractmethod
    async def pop(self, queue: str) -> JobPayload | None:
        """Pop the next available job from the queue."""
        ...

    @abstractmethod
    async def pop_delayed(self) -> list[JobPayload]:
        """Move delayed jobs whose delay has expired into their queues."""
        ...

    # ── Job lifecycle ───────────────────────────────────────────

    @abstractmethod
    async def complete(self, payload: JobPayload) -> None: ...

    @abstractmethod
    async def fail(self, payload: JobPayload, error: str) -> None: ...

    @abstractmethod
    async def release(self, payload: JobPayload, delay: float = 0) -> None:
        """Release a job back onto the queue (for retries)."""
        ...

    @abstractmethod
    async def delete(self, job_id: str) -> None: ...

    async def promote(self, job_id: str) -> bool:
        """Make a scheduled/pending job runnable immediately (clear its delay).

        Returns True if the job was promoted, False if it does not exist or is not
        in the ``pending`` state. Concrete (non-abstract) so existing third-party
        drivers keep working; the built-in drivers override it with a race-safe,
        index-aware version. The default relies on ``release(delay=0)`` to enqueue
        the job for immediate processing."""
        job = await self.get_job(job_id)
        if job is None or job.status != "pending":
            return False
        await self.release(job, delay=0)
        return True

    # ── Query ───────────────────────────────────────────────────

    @abstractmethod
    async def get_job(self, job_id: str) -> JobPayload | None: ...

    @abstractmethod
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
    ) -> list[JobPayload]: ...

    @abstractmethod
    async def count_jobs(
        self,
        queue: str | None = None,
        status: str | None = None,
        created_from: float | None = None,
        created_to: float | None = None,
    ) -> int:
        """Count jobs matching filters. Drivers MUST implement this efficiently —
        do not fall back to loading all rows."""
        ...

    @abstractmethod
    async def size(self, queue: str) -> int: ...

    @abstractmethod
    async def queues(self) -> list[str]: ...

    # ── Metrics ─────────────────────────────────────────────────

    @abstractmethod
    async def record_metric(self, queue: str, metric: str, value: float) -> None: ...

    @abstractmethod
    async def get_metrics(self, queue: str | None = None) -> dict[str, Any]: ...

    @abstractmethod
    async def report_supervisor(self, stats: dict[str, Any]) -> None:
        """Persist a supervisor snapshot for dashboard worker monitoring."""
        ...

    @abstractmethod
    async def get_supervisor_stats(self, stale_after: float = 10.0) -> list[dict[str, Any]]:
        """Return active supervisor snapshots (recent heartbeat only)."""
        ...

    # ── Batch helpers ───────────────────────────────────────────

    @abstractmethod
    async def store_batch(self, batch_id: str, data: dict[str, Any]) -> None: ...

    @abstractmethod
    async def get_batch(self, batch_id: str) -> dict[str, Any] | None: ...

    @abstractmethod
    async def update_batch(self, batch_id: str, data: dict[str, Any]) -> None: ...

    @abstractmethod
    async def increment_batch_counter(
        self, batch_id: str, field: str, delta: int = 1,
    ) -> dict[str, Any] | None:
        """Atomically increment data[field] on a batch. Returns the post-increment
        batch dict, or None if the batch is missing. Implementations MUST be safe
        against concurrent writers (no read-modify-write in Python)."""
        ...

    # ── Pruning ─────────────────────────────────────────────────

    @abstractmethod
    async def prune(
        self,
        status: str | None = None,
        tag: str | None = None,
        older_than_seconds: float | None = None,
        queue: str | None = None,
    ) -> int:
        """Delete matching jobs. Returns count of pruned jobs."""
        ...

    async def bulk_delete_jobs(self, job_ids: list[str], *, limit: int | None = None) -> int:
        """Delete an explicit list of jobs, keeping any secondary indexes consistent.

        Default implementation deletes one id at a time via ``delete``; drivers with
        secondary indexes (Redis) override this with an atomic, batched version that
        also reaps orphaned index entries. Returns the count of ids processed."""
        if limit is not None:
            job_ids = job_ids[:limit]
        for job_id in job_ids:
            await self.delete(job_id)
        return len(job_ids)

    async def prune_terminal_jobs(
        self,
        queue: str | None = None,
        status: str | None = None,
        *,
        older_than: float | None = None,
        limit: int = DEFAULT_PRUNE_BATCH,
    ) -> int:
        """Index-consistent bulk delete of terminal jobs, capped at ``limit`` per call.

        Default implementation delegates to ``prune``; the Redis driver overrides it to
        use its status index as the work source, reap orphaned index entries, and bound
        the per-call cost. Callers loop until a pass returns fewer than ``limit``."""
        return await self.prune(status=status, queue=queue, older_than_seconds=older_than)

    async def reconcile_indexes(self, batch: int = 500) -> int:
        """Repair secondary indexes by removing entries whose job no longer exists.

        No-op for drivers without secondary indexes (memory/sqlite/postgres). The Redis
        driver overrides this to walk its index ZSETs and ZREM orphaned ids. Returns the
        number of stale index entries removed."""
        return 0

    @abstractmethod
    async def flush(self, queue: str | None = None) -> None:
        """Remove all jobs (optionally for a specific queue)."""
        ...

    @abstractmethod
    async def prune_metrics(self, older_than_seconds: float) -> int:
        """Delete metric entries older than the cutoff. Returns count pruned."""
        ...

    @abstractmethod
    async def recent_throughput(
        self, seconds: int = 60, queue: str | None = None,
    ) -> dict[str, int]:
        """Count processing/completed/failed events recorded in the last `seconds`.
        Returns a dict like {"processing": N, "completed": N, "failed": N}."""
        ...
