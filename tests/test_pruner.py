"""Tests for the Pruner."""

from __future__ import annotations

import pytest

from baqueue.config import BaQueueConfig
from baqueue.drivers.memory_driver import MemoryDriver
from baqueue.pruner import Pruner
from baqueue.serializer import JobPayload, _now_ts

pytestmark = pytest.mark.asyncio


async def _push_with_status(driver, *, queue="default", status="completed", tag=None, age_seconds=0):
    """Push a job pre-set to the given status with a backdated updated_at."""
    p = JobPayload(
        job_class="tests.fake.Job",
        queue=queue,
        tags=[tag] if tag else [],
    )
    await driver.push(p)
    job = await driver.get_job(p.id)
    job.status = status
    if age_seconds:
        job.updated_at = _now_ts() - age_seconds
    return p.id


class TestPruner:
    async def test_prune_by_status_with_age(self):
        driver = MemoryDriver()
        await _push_with_status(driver, status="completed", age_seconds=3600)
        await _push_with_status(driver, status="completed", age_seconds=10)  # too fresh
        await _push_with_status(driver, status="failed", age_seconds=3600)

        # prune_completed_hours=0.5 → 1800 seconds threshold
        config = BaQueueConfig(
            prune_completed_hours=0,
            prune_failed_hours=0,
            prune_cancelled_hours=0,
            prune_metrics_hours=0,
        )
        # Force a manual prune call instead of relying on config values
        p = Pruner(driver=driver, config=config)
        # use prune_by_status directly
        n = await p.prune_by_status("completed", hours=0.5)
        # 1 of the 2 completed jobs is older than 0.5h
        assert n == 1

    async def test_prune_by_tag(self):
        driver = MemoryDriver()
        await _push_with_status(driver, tag="campaign")
        await _push_with_status(driver, tag="campaign")
        await _push_with_status(driver, tag="other")

        p = Pruner(driver=driver, config=BaQueueConfig())
        n = await p.prune_by_tag("campaign")
        assert n == 2

    async def test_prune_once_respects_config(self):
        driver = MemoryDriver()
        # 5 jobs old enough to prune
        for _ in range(5):
            await _push_with_status(driver, status="completed", age_seconds=24 * 3600 + 10)
        # 2 too-fresh
        for _ in range(2):
            await _push_with_status(driver, status="completed", age_seconds=60)

        config = BaQueueConfig(
            prune_completed_hours=24,  # threshold
            prune_failed_hours=0,
            prune_cancelled_hours=0,
            prune_metrics_hours=0,
        )
        p = Pruner(driver=driver, config=config)
        results = await p.prune_once()
        assert results["completed"] == 5
