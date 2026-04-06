"""Worker - processes jobs from queues."""

from __future__ import annotations

import asyncio
import logging
import signal
import traceback
from typing import Any

from baqueue.drivers.base import BaseDriver
from baqueue.events import EventBus
from baqueue.job import Job, FunctionJob
from baqueue.retry import compute_delay, should_retry
from baqueue.serializer import JobPayload, resolve_job_class, _now_ts

logger = logging.getLogger("baqueue.worker")


class Worker:
    """Pulls and executes jobs from one or more queues."""

    def __init__(
        self,
        driver: BaseDriver,
        queues: list[str],
        events: EventBus | None = None,
        sleep_interval: float = 1.0,
        timeout: int = 60,
        name: str = "worker-0",
    ):
        self.driver = driver
        self.queues = queues
        self.events = events or EventBus.default()
        self.sleep_interval = sleep_interval
        self.timeout = timeout
        self.name = name
        self._running = False
        self._current_job: JobPayload | None = None
        self._jobs_processed = 0
        self._jobs_failed = 0

    @property
    def is_running(self) -> bool:
        return self._running

    @property
    def stats(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "queues": self.queues,
            "running": self._running,
            "current_job": self._current_job.id if self._current_job else None,
            "jobs_processed": self._jobs_processed,
            "jobs_failed": self._jobs_failed,
        }

    async def start(self) -> None:
        """Start the worker loop."""
        self._running = True
        await self.events.emit("worker.started", worker=self.name)
        logger.info("Worker %s started on queues %s", self.name, self.queues)

        try:
            while self._running:
                job = await self._fetch_next()
                if job:
                    await self._process(job)
                else:
                    await asyncio.sleep(self.sleep_interval)
        except asyncio.CancelledError:
            pass
        finally:
            self._running = False
            await self.events.emit("worker.stopped", worker=self.name)
            logger.info("Worker %s stopped", self.name)

    def stop(self) -> None:
        self._running = False

    async def _fetch_next(self) -> JobPayload | None:
        for queue in self.queues:
            job = await self.driver.pop(queue)
            if job:
                return job
        return None

    async def _process(self, payload: JobPayload) -> None:
        self._current_job = payload
        job_timeout = payload.timeout or self.timeout

        try:
            await self.events.emit("job.started", payload=payload, worker=self.name)
            logger.debug("Processing job %s (%s)", payload.id, payload.job_class)

            job_instance = self._instantiate(payload)
            result = await asyncio.wait_for(
                job_instance.handle(**payload.data),
                timeout=job_timeout,
            )

            await self.driver.complete(payload)
            await self.driver.record_metric(payload.queue, "completed", 1)
            await self.events.emit("job.completed", payload=payload, result=result, worker=self.name)
            self._jobs_processed += 1

            if hasattr(job_instance, "on_success"):
                try:
                    await job_instance.on_success(result, payload)
                except Exception:
                    logger.exception("Error in on_success for job %s", payload.id)

            await self._check_batch_completion(payload)

        except Exception as exc:
            error_msg = f"{type(exc).__name__}: {exc}\n{traceback.format_exc()}"
            logger.warning("Job %s failed (attempt %d): %s", payload.id, payload.attempts, exc)

            if should_retry(payload.attempts, payload.max_attempts):
                delay = compute_delay(payload.backoff, payload.attempts)
                await self.driver.release(payload, delay=delay)
                await self.events.emit("job.retrying", payload=payload, error=error_msg, delay=delay)
            else:
                await self.driver.fail(payload, error_msg)
                await self.driver.record_metric(payload.queue, "failed", 1)
                await self.events.emit("job.failed", payload=payload, error=error_msg, worker=self.name)
                self._jobs_failed += 1

                job_instance = self._instantiate(payload)
                try:
                    await job_instance.on_failure(exc, payload)
                except Exception:
                    logger.exception("Error in on_failure for job %s", payload.id)

                await self._check_batch_failure(payload)
        finally:
            self._current_job = None

    def _instantiate(self, payload: JobPayload) -> Job:
        cls = resolve_job_class(payload.job_class)
        if isinstance(cls, FunctionJob):
            return cls
        return cls()

    async def _check_batch_completion(self, payload: JobPayload) -> None:
        if not payload.batch_id:
            return
        batch = await self.driver.get_batch(payload.batch_id)
        if not batch:
            return
        batch["completed_count"] = batch.get("completed_count", 0) + 1
        await self.driver.update_batch(payload.batch_id, batch)
        if batch["completed_count"] + batch.get("failed_count", 0) >= batch.get("total", 0):
            await self.events.emit("batch.completed", batch_id=payload.batch_id, batch=batch)

    async def _check_batch_failure(self, payload: JobPayload) -> None:
        if not payload.batch_id:
            return
        batch = await self.driver.get_batch(payload.batch_id)
        if not batch:
            return
        batch["failed_count"] = batch.get("failed_count", 0) + 1
        await self.driver.update_batch(payload.batch_id, batch)
        if batch.get("allow_failures", False) is False and batch["failed_count"] == 1:
            await self.events.emit("batch.failed", batch_id=payload.batch_id, batch=batch)
