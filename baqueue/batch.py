"""Batch job orchestration with lifecycle callbacks."""

from __future__ import annotations

from typing import Any, Type
from uuid import uuid4

from baqueue.drivers.base import BaseDriver
from baqueue.events import EventBus
from baqueue.job import Job, FunctionJob
from baqueue.serializer import JobPayload, _now_ts, get_class_path


class Batch:
    """Fluent builder for dispatching a batch of jobs.

    Usage::

        batch = await Batch(driver, [
            (SendEmail, {"to": "a@b.com", "subject": "Hi", "body": "Hey"}),
            (SendEmail, {"to": "c@d.com", "subject": "Hi", "body": "Hey"}),
        ]).then(OnAllDone).catch(OnAnyFailed).name("newsletter-2024").dispatch()
    """

    def __init__(
        self,
        driver: BaseDriver,
        jobs: list[tuple[Type[Job] | Job | FunctionJob, dict[str, Any]]],
        events: EventBus | None = None,
    ):
        self._driver = driver
        self._jobs = jobs
        self._events = events or EventBus.default()
        self._batch_id = uuid4().hex
        self._name: str = ""
        self._then_job: Type[Job] | None = None
        self._catch_job: Type[Job] | None = None
        self._finally_job: Type[Job] | None = None
        self._allow_failures = False
        self._tags: list[str] = []

    def name(self, name: str) -> Batch:
        self._name = name
        return self

    def then(self, job_class: Type[Job]) -> Batch:
        """Job to run when all jobs in the batch complete successfully."""
        self._then_job = job_class
        return self

    def catch(self, job_class: Type[Job]) -> Batch:
        """Job to run when any job in the batch fails."""
        self._catch_job = job_class
        return self

    def on_finally(self, job_class: Type[Job]) -> Batch:
        """Job to run when all jobs in the batch finish (regardless of success/failure)."""
        self._finally_job = job_class
        return self

    def allow_failures(self) -> Batch:
        self._allow_failures = True
        return self

    def tag(self, *tags: str) -> Batch:
        self._tags.extend(tags)
        return self

    async def dispatch(self) -> BatchResult:
        """Push all batch jobs and store batch metadata."""
        payloads: list[JobPayload] = []
        for job, kwargs in self._jobs:
            payload = self._build_payload(job, kwargs)
            payload.batch_id = self._batch_id
            payload.tags.append(f"batch:{self._batch_id}")
            if self._name:
                payload.tags.append(f"batch:{self._name}")
            for t in self._tags:
                if t not in payload.tags:
                    payload.tags.append(t)
            payloads.append(payload)

        batch_data = {
            "id": self._batch_id,
            "name": self._name,
            "total": len(payloads),
            "completed_count": 0,
            "failed_count": 0,
            "allow_failures": self._allow_failures,
            "then_job": get_class_path(self._then_job) if self._then_job else None,
            "catch_job": get_class_path(self._catch_job) if self._catch_job else None,
            "finally_job": get_class_path(self._finally_job) if self._finally_job else None,
            "created_at": _now_ts(),
            "status": "processing",
        }

        await self._driver.store_batch(self._batch_id, batch_data)
        ids = await self._driver.push_many(payloads)

        self._events.emit_nowait("batch.dispatched", batch_id=self._batch_id, count=len(ids))

        self._register_callbacks()

        return BatchResult(batch_id=self._batch_id, name=self._name, job_ids=ids, total=len(ids))

    def _register_callbacks(self) -> None:
        batch_id = self._batch_id
        driver = self._driver
        then_job = self._then_job
        catch_job = self._catch_job
        finally_job = self._finally_job

        async def on_batch_completed(**kwargs: Any) -> None:
            if kwargs.get("batch_id") != batch_id:
                return
            batch = kwargs.get("batch", {})
            if then_job and batch.get("failed_count", 0) == 0:
                inst = then_job()
                payload = inst.build_payload(batch_id=batch_id, batch=batch)
                await driver.push(payload)
            if finally_job:
                inst = finally_job()
                payload = inst.build_payload(batch_id=batch_id, batch=batch)
                await driver.push(payload)
            await driver.update_batch(batch_id, {"status": "completed"})

        async def on_batch_failed(**kwargs: Any) -> None:
            if kwargs.get("batch_id") != batch_id:
                return
            batch = kwargs.get("batch", {})
            if catch_job:
                inst = catch_job()
                payload = inst.build_payload(batch_id=batch_id, batch=batch)
                await driver.push(payload)

        self._events.on("batch.completed", on_batch_completed)
        self._events.on("batch.failed", on_batch_failed)

    def _build_payload(self, job: Type[Job] | Job | FunctionJob, kwargs: dict[str, Any]) -> JobPayload:
        if isinstance(job, FunctionJob):
            return job.build_payload(**kwargs)
        if isinstance(job, Job):
            return job.build_payload(**kwargs)
        if isinstance(job, type) and issubclass(job, Job):
            return job().build_payload(**kwargs)
        raise TypeError(f"Expected a Job class or instance, got {type(job)}")


class BatchResult:
    """Result of a dispatched batch."""

    def __init__(self, batch_id: str, name: str, job_ids: list[str], total: int):
        self.batch_id = batch_id
        self.name = name
        self.job_ids = job_ids
        self.total = total

    def __repr__(self) -> str:
        return f"BatchResult(id={self.batch_id!r}, name={self.name!r}, total={self.total})"
