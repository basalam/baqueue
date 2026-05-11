"""Tests for the Job base class, @Job.as_job decorator, and FunctionJob."""

from __future__ import annotations

import pytest

from baqueue.job import FunctionJob, Job
from baqueue.serializer import JobPayload


class Greeter(Job):
    queue = "greetings"
    max_attempts = 5
    backoff = "linear"
    timeout = 30
    tags = ["greet"]

    async def handle(self, name: str = "world") -> str:
        return f"Hello, {name}!"


class TestJobClass:
    def test_default_attributes(self):
        assert Job.queue == "default"
        assert Job.max_attempts == 3
        assert Job.backoff == "exponential"
        assert Job.timeout == 60
        assert Job.tags == []

    def test_subclass_attributes(self):
        assert Greeter.queue == "greetings"
        assert Greeter.max_attempts == 5
        assert Greeter.backoff == "linear"
        assert Greeter.tags == ["greet"]

    def test_handle_not_implemented_on_base(self):
        async def _run():
            await Job().handle()  # type: ignore[arg-type]
        with pytest.raises(NotImplementedError):
            import asyncio
            asyncio.run(_run())

    def test_build_payload_propagates_class_attributes(self):
        p = Greeter().build_payload(name="Ada")
        assert isinstance(p, JobPayload)
        assert p.queue == "greetings"
        assert p.max_attempts == 5
        assert p.backoff == "linear"
        assert p.timeout == 30
        assert p.tags == ["greet"]
        assert p.data == {"name": "Ada"}
        assert p.job_class.endswith("Greeter")

    def test_build_payload_tags_isolated(self):
        # Repeated build_payload calls must not mutate Greeter.tags class attribute
        Greeter().build_payload()
        Greeter().build_payload()
        assert Greeter.tags == ["greet"]

    async def test_default_on_success_and_on_failure_are_noops(self):
        job = Greeter()
        # Should not raise
        await job.on_success(None, JobPayload())
        await job.on_failure(RuntimeError("x"), JobPayload())


@Job.as_job(queue="emails", max_attempts=2, tags=["email"], backoff="fixed")
async def send_email(to: str, subject: str) -> str:
    return f"sent:{to}:{subject}"


class TestFunctionJob:
    def test_decorator_returns_function_job(self):
        assert isinstance(send_email, FunctionJob)

    def test_decorator_carries_metadata(self):
        assert send_email.queue == "emails"
        assert send_email.max_attempts == 2
        assert send_email.backoff == "fixed"
        assert "email" in send_email.tags

    def test_build_payload_uses_func_qualname(self):
        p = send_email.build_payload(to="x@y.com", subject="hi")
        # Path should resolve back to this module + qualname
        assert "send_email" in p.job_class
        assert p.queue == "emails"
        assert p.data == {"to": "x@y.com", "subject": "hi"}

    async def test_callable_invocation(self):
        result = await send_email(to="x@y.com", subject="hi")
        assert result == "sent:x@y.com:hi"

    async def test_handle_invokes_underlying_func(self):
        result = await send_email.handle(to="z@y.com", subject="hello")
        assert result == "sent:z@y.com:hello"

    def test_tags_default_to_empty_list(self):
        @Job.as_job(queue="x")
        async def fn():
            pass

        assert fn.tags == []
