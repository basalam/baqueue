"""Job serialization and deserialization."""

from __future__ import annotations

import json
import importlib
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4


def _now_ts() -> float:
    return datetime.now(timezone.utc).timestamp()


class JobPayload:
    """Serializable representation of a queued job."""

    __slots__ = (
        "id",
        "job_class",
        "data",
        "queue",
        "attempts",
        "max_attempts",
        "backoff",
        "timeout",
        "tags",
        "batch_id",
        "delay_until",
        "created_at",
        "updated_at",
        "started_at",
        "completed_at",
        "failed_at",
        "status",
        "error",
    )

    def __init__(
        self,
        *,
        id: str | None = None,
        job_class: str = "",
        data: dict[str, Any] | None = None,
        queue: str = "default",
        attempts: int = 0,
        max_attempts: int = 3,
        backoff: str | list[int] = "exponential",
        timeout: int = 60,
        tags: list[str] | None = None,
        batch_id: str | None = None,
        delay_until: float | None = None,
        created_at: float | None = None,
        updated_at: float | None = None,
        started_at: float | None = None,
        completed_at: float | None = None,
        failed_at: float | None = None,
        status: str = "pending",
        error: str | None = None,
    ):
        self.id = id or uuid4().hex
        self.job_class = job_class
        self.data = data or {}
        self.queue = queue
        self.attempts = attempts
        self.max_attempts = max_attempts
        self.backoff = backoff
        self.timeout = timeout
        self.tags = tags or []
        self.batch_id = batch_id
        self.delay_until = delay_until
        self.created_at = created_at or _now_ts()
        self.updated_at = updated_at or self.created_at
        self.started_at = started_at
        self.completed_at = completed_at
        self.failed_at = failed_at
        self.status = status
        self.error = error

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "job_class": self.job_class,
            "data": self.data,
            "queue": self.queue,
            "attempts": self.attempts,
            "max_attempts": self.max_attempts,
            "backoff": self.backoff,
            "timeout": self.timeout,
            "tags": self.tags,
            "batch_id": self.batch_id,
            "delay_until": self.delay_until,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "started_at": self.started_at,
            "completed_at": self.completed_at,
            "failed_at": self.failed_at,
            "status": self.status,
            "error": self.error,
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict())

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> JobPayload:
        return cls(**data)

    @classmethod
    def from_json(cls, raw: str) -> JobPayload:
        return cls.from_dict(json.loads(raw))


def resolve_job_class(class_path: str):
    """Dynamically import and return a job class from its dotted path."""
    module_path, class_name = class_path.rsplit(".", 1)
    module = importlib.import_module(module_path)
    return getattr(module, class_name)


def get_class_path(cls: type) -> str:
    """Get the fully qualified dotted path for a class."""
    return f"{cls.__module__}.{cls.__qualname__}"
