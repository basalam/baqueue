"""Abstract base driver for BaQueue."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from baqueue.serializer import JobPayload


class BaseDriver(ABC):
    """Every BaQueue driver must implement this interface."""

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
        """Count completed/failed events recorded in the last `seconds`.
        Returns a dict like {"completed": N, "failed": N}."""
        ...
