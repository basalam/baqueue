"""Event bus for BaQueue lifecycle events."""

from __future__ import annotations

import asyncio
import logging
from collections import defaultdict
from typing import Any, Callable, Coroutine

logger = logging.getLogger("baqueue.events")

EventHandler = Callable[..., Coroutine[Any, Any, None]]


class EventBus:
    """Simple async event bus for job lifecycle events.

    Built-in events:
        job.pushed, job.started, job.completed, job.failed, job.retrying,
        batch.completed, batch.failed,
        worker.started, worker.stopped,
        supervisor.started, supervisor.stopped,
        queue.pruned
    """

    _handlers: dict[str, list[EventHandler]]
    _instance: EventBus | None = None

    def __init__(self) -> None:
        self._handlers = defaultdict(list)

    @classmethod
    def default(cls) -> EventBus:
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    def on(self, event: str, handler: EventHandler) -> None:
        self._handlers[event].append(handler)

    def off(self, event: str, handler: EventHandler | None = None) -> None:
        if handler is None:
            self._handlers.pop(event, None)
        else:
            handlers = self._handlers.get(event, [])
            self._handlers[event] = [h for h in handlers if h is not handler]

    async def emit(self, event: str, **kwargs: Any) -> None:
        for handler in self._handlers.get(event, []):
            try:
                await handler(**kwargs)
            except Exception:
                logger.exception("Error in event handler for %s", event)

    def emit_nowait(self, event: str, **kwargs: Any) -> None:
        """Fire-and-forget emit for use in sync contexts."""
        try:
            loop = asyncio.get_running_loop()
            loop.create_task(self.emit(event, **kwargs))
        except RuntimeError:
            pass

    def clear(self) -> None:
        self._handlers.clear()
