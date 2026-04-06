"""Base Job class and decorator for defining queue jobs."""

from __future__ import annotations

import asyncio
import functools
import inspect
from typing import Any, Callable, Coroutine

from baqueue.serializer import JobPayload, get_class_path


class Job:
    """Base class for queue jobs.

    Usage::

        class SendEmail(Job):
            queue = "emails"
            max_attempts = 3
            backoff = "exponential"
            timeout = 30
            tags = ["email"]

            async def handle(self, to: str, subject: str, body: str):
                await send_email(to, subject, body)

            async def on_failure(self, error: Exception, payload: JobPayload):
                log.error("Email send failed: %s", error)
    """

    queue: str = "default"
    max_attempts: int = 3
    backoff: str | list[int] = "exponential"
    timeout: int = 60
    tags: list[str] = []

    async def handle(self, **kwargs: Any) -> Any:
        raise NotImplementedError("Subclasses must implement handle()")

    async def on_failure(self, error: Exception, payload: JobPayload) -> None:
        """Called when the job fails all retry attempts."""
        pass

    async def on_success(self, result: Any, payload: JobPayload) -> None:
        """Called when the job completes successfully."""
        pass

    def build_payload(self, **kwargs: Any) -> JobPayload:
        return JobPayload(
            job_class=get_class_path(self.__class__),
            data=kwargs,
            queue=self.queue,
            max_attempts=self.max_attempts,
            backoff=self.backoff,
            timeout=self.timeout,
            tags=list(self.tags),
        )

    @classmethod
    def as_job(
        cls,
        queue: str = "default",
        max_attempts: int = 3,
        backoff: str | list[int] = "exponential",
        timeout: int = 60,
        tags: list[str] | None = None,
    ) -> Callable:
        """Decorator to turn an async function into a dispatchable job.

        Usage::

            @Job.as_job(queue="emails", max_attempts=3)
            async def send_email(to, subject, body):
                ...

            await Queue.push(send_email, to="a@b.com", subject="Hi", body="Hello")
        """
        def decorator(func: Callable[..., Coroutine]) -> FunctionJob:
            job = FunctionJob(
                func=func,
                queue=queue,
                max_attempts=max_attempts,
                backoff=backoff,
                timeout=timeout,
                tags=tags or [],
            )
            functools.update_wrapper(job, func)
            return job
        return decorator


class FunctionJob(Job):
    """Wraps a plain async function as a Job."""

    def __init__(
        self,
        func: Callable[..., Coroutine],
        queue: str = "default",
        max_attempts: int = 3,
        backoff: str | list[int] = "exponential",
        timeout: int = 60,
        tags: list[str] | None = None,
    ):
        self._func = func
        self.queue = queue
        self.max_attempts = max_attempts
        self.backoff = backoff
        self.timeout = timeout
        self.tags = tags or []
        self.__name__ = func.__name__
        self.__qualname__ = func.__qualname__
        self.__module__ = func.__module__

    async def handle(self, **kwargs: Any) -> Any:
        return await self._func(**kwargs)

    def build_payload(self, **kwargs: Any) -> JobPayload:
        return JobPayload(
            job_class=f"{self._func.__module__}.{self._func.__qualname__}",
            data=kwargs,
            queue=self.queue,
            max_attempts=self.max_attempts,
            backoff=self.backoff,
            timeout=self.timeout,
            tags=list(self.tags),
        )

    async def __call__(self, *args: Any, **kwargs: Any) -> Any:
        return await self._func(*args, **kwargs)
