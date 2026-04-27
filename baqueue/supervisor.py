"""Supervisor - manages a pool of workers with auto-scaling and graceful shutdown."""

from __future__ import annotations

import asyncio
import logging
import signal
import os
from typing import Any

from baqueue.config import SupervisorConfig
from baqueue.drivers.base import BaseDriver
from baqueue.events import EventBus
from baqueue.worker import Worker

logger = logging.getLogger("baqueue.supervisor")


class Supervisor:
    """Manages a pool of workers for one or more queues.

    Supports auto-balancing, scaling, and graceful shutdown.
    """

    def __init__(
        self,
        driver: BaseDriver,
        config: SupervisorConfig | None = None,
        events: EventBus | None = None,
        balancer: Any | None = None,
    ):
        self.driver = driver
        self.config = config or SupervisorConfig()
        self.events = events or EventBus.default()
        self.balancer = balancer
        self._workers: list[Worker] = []
        self._tasks: list[asyncio.Task] = []
        self._running = False
        self._delayed_task: asyncio.Task | None = None
        self._heartbeat_task: asyncio.Task | None = None
        self._balance_task: asyncio.Task | None = None

    @property
    def is_running(self) -> bool:
        return self._running

    @property
    def worker_count(self) -> int:
        return len(self._workers)

    @property
    def stats(self) -> dict[str, Any]:
        return {
            "name": self.config.name,
            "queues": self.config.queues,
            "balance": self.config.balance,
            "workers": len(self._workers),
            "running": self._running,
            "worker_stats": [w.stats for w in self._workers],
        }

    async def start(self) -> None:
        """Start the supervisor and its worker pool."""
        self._running = True
        logger.info(
            "Supervisor '%s' starting with %d workers on queues %s",
            self.config.name, self.config.min_workers, self.config.queues,
        )
        await self.events.emit("supervisor.started", supervisor=self.config.name)

        self._setup_signal_handlers()

        for i in range(self.config.min_workers):
            self._spawn_worker(i)

        await self._report_stats()
        self._delayed_task = asyncio.create_task(self._poll_delayed())
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())

        if self.balancer:
            self._balance_task = asyncio.create_task(self._balance_loop())

        try:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        except asyncio.CancelledError:
            pass

    async def stop(self) -> None:
        """Gracefully stop all workers."""
        logger.info("Supervisor '%s' shutting down...", self.config.name)
        self._running = False

        await self._report_stats()
        for w in self._workers:
            w.stop()

        if self._delayed_task:
            self._delayed_task.cancel()
        if self._heartbeat_task:
            self._heartbeat_task.cancel()
        if self._balance_task:
            self._balance_task.cancel()

        aux_tasks = [t for t in (self._delayed_task, self._heartbeat_task, self._balance_task) if t is not None]
        if aux_tasks:
            await asyncio.gather(*aux_tasks, return_exceptions=True)
        self._delayed_task = None
        self._heartbeat_task = None
        self._balance_task = None

        for task in self._tasks:
            task.cancel()

        await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()
        self._workers.clear()
        await self._report_stats()

        await self.events.emit("supervisor.stopped", supervisor=self.config.name)
        logger.info("Supervisor '%s' stopped", self.config.name)

    def _spawn_worker(self, index: int) -> Worker:
        worker = Worker(
            driver=self.driver,
            queues=list(self.config.queues),
            events=self.events,
            sleep_interval=self.config.sleep,
            timeout=self.config.timeout,
            name=f"{self.config.name}-worker-{index}",
        )
        self._workers.append(worker)
        task = asyncio.create_task(worker.start())
        self._tasks.append(task)
        return worker

    async def scale(self, count: int) -> None:
        """Scale worker pool to the specified count."""
        count = max(self.config.min_workers, min(count, self.config.max_workers))
        current = len(self._workers)

        if count > current:
            for i in range(current, count):
                self._spawn_worker(i)
            logger.info("Scaled up to %d workers", count)
        elif count < current:
            for _ in range(current - count):
                worker = self._workers.pop()
                worker.stop()
            logger.info("Scaled down to %d workers", count)

    async def _poll_delayed(self) -> None:
        """Periodically move delayed jobs into their queues."""
        while self._running:
            try:
                await self.driver.pop_delayed()
            except Exception:
                logger.exception("Error polling delayed jobs")
            await asyncio.sleep(1)

    async def _balance_loop(self) -> None:
        """Periodically rebalance workers across queues."""
        while self._running:
            try:
                if self.balancer:
                    new_count = await self.balancer.recommend(
                        self.driver, self.config.queues, len(self._workers)
                    )
                    if new_count != len(self._workers):
                        await self.scale(new_count)
            except Exception:
                logger.exception("Error in balance loop")
            await asyncio.sleep(5)

    async def _heartbeat_loop(self) -> None:
        while self._running:
            await self._report_stats()
            await asyncio.sleep(1)

    async def _report_stats(self) -> None:
        try:
            await self.driver.report_supervisor(self.stats)
        except Exception:
            logger.exception("Failed to report supervisor stats")

    def _setup_signal_handlers(self) -> None:
        if os.name == "nt":
            return
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, lambda: asyncio.create_task(self.stop()))
