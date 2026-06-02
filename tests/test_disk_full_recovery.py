"""Tests for automatic driver cleanup on storage-full errors."""

from __future__ import annotations

import asyncio
import sqlite3

import pytest

from baqueue.config import BaQueueConfig
from baqueue.drivers.memory_driver import MemoryDriver
from baqueue.queue import Queue
from baqueue.serializer import JobPayload

pytestmark = pytest.mark.asyncio


class StorageFullError(Exception):
    pass


class RecoveringMemoryDriver(MemoryDriver):
    def __init__(self) -> None:
        super().__init__()
        self.cleanup_calls = 0

    def is_storage_full_error(self, exc: BaseException) -> bool:
        return isinstance(exc, StorageFullError)

    async def emergency_cleanup(self) -> int:
        self.cleanup_calls += 1
        return await super().emergency_cleanup()


def _make_payload(**kw) -> JobPayload:
    return JobPayload(job_class="tests.fake.Job", data={"x": 1}, **kw)


async def test_retries_once_after_emergency_cleanup():
    driver = RecoveringMemoryDriver()
    completed = _make_payload(queue="default")
    await driver.push(completed)
    await driver.complete(completed)

    attempts = 0

    async def write_operation():
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise StorageFullError("disk full")
        return "ok"

    result = await driver._with_disk_full_recovery(write_operation)

    assert result == "ok"
    assert attempts == 2
    assert driver.cleanup_calls == 1
    assert await driver.count_jobs(status="completed") == 0


async def test_does_not_cleanup_when_recovery_disabled():
    driver = RecoveringMemoryDriver()
    driver.auto_cleanup_on_disk_full = False
    attempts = 0

    async def write_operation():
        nonlocal attempts
        attempts += 1
        raise StorageFullError("disk full")

    with pytest.raises(StorageFullError):
        await driver._with_disk_full_recovery(write_operation)

    assert attempts == 1
    assert driver.cleanup_calls == 0


async def test_sqlite_recovery_does_not_reenter_lock(sqlite_driver):
    completed = _make_payload(queue="default")
    await sqlite_driver.push(completed)
    await sqlite_driver.complete(completed)

    attempts = 0

    def write_operation():
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise sqlite3.OperationalError("database or disk is full")
        return "ok"

    async with sqlite_driver._lock:
        result = await asyncio.wait_for(sqlite_driver._execute_with_retry(write_operation), timeout=1.0)

    assert result == "ok"
    assert attempts == 2
    assert await sqlite_driver.count_jobs(status="completed") == 0


async def test_configure_applies_disk_full_cleanup_to_explicit_driver():
    driver = MemoryDriver()

    Queue.configure(BaQueueConfig(auto_cleanup_on_disk_full=False), driver=driver)

    assert driver.auto_cleanup_on_disk_full is False
