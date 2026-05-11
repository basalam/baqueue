"""Shared pytest fixtures for the BaQueue test suite.

Most fixtures yield a fresh, isolated environment so tests can run in any order
without leaking driver state into each other.
"""

from __future__ import annotations

from typing import AsyncIterator, Iterator

import pytest
import pytest_asyncio

from baqueue.config import BaQueueConfig, DriverConfig
from baqueue.drivers.memory_driver import MemoryDriver
from baqueue.drivers.sqlite_driver import SqliteDriver
from baqueue.events import EventBus
from baqueue.queue import Queue


# ── Reset singletons between tests ───────────────────────────────────────

@pytest.fixture(autouse=True)
def _reset_singletons() -> Iterator[None]:
    """Wipe the Queue + EventBus singletons before every test."""
    Queue.reset()
    EventBus._instance = None
    yield
    Queue.reset()
    EventBus._instance = None


# ── Memory driver (default for most tests) ───────────────────────────────

@pytest_asyncio.fixture
async def memory_driver() -> AsyncIterator[MemoryDriver]:
    driver = MemoryDriver()
    await driver.connect()
    try:
        yield driver
    finally:
        await driver.disconnect()


# ── SQLite driver (file-backed; tmp_path scoped) ─────────────────────────

@pytest_asyncio.fixture
async def sqlite_driver(tmp_path) -> AsyncIterator[SqliteDriver]:
    db_path = tmp_path / "baqueue_test.db"
    driver = SqliteDriver(path=str(db_path))
    await driver.connect()
    try:
        yield driver
    finally:
        await driver.disconnect()


# ── Parameterized driver fixture (memory + sqlite) ───────────────────────

@pytest_asyncio.fixture(params=["memory", "sqlite"])
async def driver(request, tmp_path) -> AsyncIterator:
    """Drives the same test against both core drivers."""
    if request.param == "memory":
        d = MemoryDriver()
    else:
        d = SqliteDriver(path=str(tmp_path / "baqueue_test.db"))
    await d.connect()
    try:
        yield d
    finally:
        await d.disconnect()


# ── Queue facade configured against memory driver ────────────────────────

@pytest_asyncio.fixture
async def configured_queue() -> AsyncIterator[type[Queue]]:
    Queue.configure(BaQueueConfig(driver=DriverConfig(name="memory")))
    await Queue.connect()
    try:
        yield Queue
    finally:
        await Queue.disconnect()
        Queue.reset()


# pytest-asyncio handles event-loop policy by default; no custom override needed.
