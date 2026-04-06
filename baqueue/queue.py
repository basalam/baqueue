"""Queue manager - the main entry point for dispatching jobs."""

from __future__ import annotations

from typing import Any, Type

from baqueue.config import BaQueueConfig
from baqueue.drivers.base import BaseDriver
from baqueue.drivers.memory_driver import MemoryDriver
from baqueue.events import EventBus
from baqueue.job import Job, FunctionJob
from baqueue.serializer import JobPayload, get_class_path, _now_ts


class Queue:
    """Central queue manager. Maintains a singleton driver connection.

    Usage::

        Queue.configure(BaQueueConfig(driver=DriverConfig(name="redis", url="redis://localhost")))
        await Queue.push(SendEmail, to="user@example.com", subject="Hi", body="Hello")
    """

    _driver: BaseDriver | None = None
    _config: BaQueueConfig | None = None
    _events: EventBus | None = None

    @classmethod
    def configure(cls, config: BaQueueConfig | None = None, driver: BaseDriver | None = None) -> None:
        cls._config = config or BaQueueConfig()
        cls._driver = driver
        cls._events = EventBus.default()

    @classmethod
    def get_driver(cls) -> BaseDriver:
        if cls._driver is None:
            cls._driver = _create_driver(cls._config or BaQueueConfig())
        return cls._driver

    @classmethod
    def get_config(cls) -> BaQueueConfig:
        if cls._config is None:
            cls._config = BaQueueConfig()
        return cls._config

    @classmethod
    def get_events(cls) -> EventBus:
        if cls._events is None:
            cls._events = EventBus.default()
        return cls._events

    @classmethod
    async def connect(cls) -> None:
        await cls.get_driver().connect()

    @classmethod
    async def disconnect(cls) -> None:
        driver = cls.get_driver()
        await driver.disconnect()
        cls._driver = None

    # ── Dispatch ────────────────────────────────────────────────

    @classmethod
    async def push(cls, job: Type[Job] | Job | FunctionJob, **kwargs: Any) -> str:
        """Push a job onto the queue for immediate processing."""
        payload = _build_payload(job, kwargs)
        job_id = await cls.get_driver().push(payload)
        cls.get_events().emit_nowait("job.pushed", payload=payload)
        return job_id

    @classmethod
    async def later(
        cls,
        job: Type[Job] | Job | FunctionJob,
        delay: float,
        **kwargs: Any,
    ) -> str:
        """Push a job with a delay (in seconds)."""
        payload = _build_payload(job, kwargs)
        payload.delay_until = _now_ts() + delay
        job_id = await cls.get_driver().push(payload)
        cls.get_events().emit_nowait("job.pushed", payload=payload)
        return job_id

    @classmethod
    async def bulk(cls, jobs: list[tuple[Type[Job] | Job | FunctionJob, dict[str, Any]]]) -> list[str]:
        """Push multiple jobs at once."""
        payloads = [_build_payload(j, kw) for j, kw in jobs]
        ids = await cls.get_driver().push_many(payloads)
        for p in payloads:
            cls.get_events().emit_nowait("job.pushed", payload=p)
        return ids

    # ── Query ───────────────────────────────────────────────────

    @classmethod
    async def size(cls, queue: str = "default") -> int:
        return await cls.get_driver().size(queue)

    @classmethod
    async def queues(cls) -> list[str]:
        return await cls.get_driver().queues()

    @classmethod
    async def get_job(cls, job_id: str) -> JobPayload | None:
        return await cls.get_driver().get_job(job_id)

    @classmethod
    async def get_jobs(cls, **kwargs: Any) -> list[JobPayload]:
        return await cls.get_driver().get_jobs(**kwargs)

    # ── Prune ───────────────────────────────────────────────────

    @classmethod
    async def prune(
        cls,
        status: str | None = None,
        tag: str | None = None,
        hours: float | None = None,
        queue: str | None = None,
    ) -> int:
        older_than = hours * 3600 if hours else None
        count = await cls.get_driver().prune(
            status=status, tag=tag, older_than_seconds=older_than, queue=queue
        )
        cls.get_events().emit_nowait("queue.pruned", status=status, tag=tag, count=count)
        return count

    # ── Flush ───────────────────────────────────────────────────

    @classmethod
    async def flush(cls, queue: str | None = None) -> None:
        await cls.get_driver().flush(queue)

    # ── Metrics ─────────────────────────────────────────────────

    @classmethod
    async def metrics(cls, queue: str | None = None) -> dict[str, Any]:
        return await cls.get_driver().get_metrics(queue)

    # ── Reset (for testing) ─────────────────────────────────────

    @classmethod
    def reset(cls) -> None:
        cls._driver = None
        cls._config = None
        cls._events = None


def _build_payload(job: Type[Job] | Job | FunctionJob, kwargs: dict[str, Any]) -> JobPayload:
    if isinstance(job, FunctionJob):
        return job.build_payload(**kwargs)
    if isinstance(job, Job):
        return job.build_payload(**kwargs)
    if isinstance(job, type) and issubclass(job, Job):
        return job().build_payload(**kwargs)
    raise TypeError(f"Expected a Job class or instance, got {type(job)}")


def _create_driver(config: BaQueueConfig) -> BaseDriver:
    name = config.driver.name
    if name == "memory":
        return MemoryDriver()
    elif name == "sqlite":
        from baqueue.drivers.sqlite_driver import SqliteDriver
        path = config.driver.url or ".baqueue.db"
        return SqliteDriver(path=path, **config.driver.options)
    elif name == "redis":
        from baqueue.drivers.redis_driver import RedisDriver
        return RedisDriver(config.driver.url, prefix=config.prefix, **config.driver.options)
    elif name == "postgres":
        from baqueue.drivers.postgres_driver import PostgresDriver
        return PostgresDriver(config.driver.url, prefix=config.prefix, **config.driver.options)
    else:
        raise ValueError(f"Unknown driver: {name}")
