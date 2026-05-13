"""BaQueue configuration."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class DriverConfig(BaseModel):
    """Configuration for a specific driver."""

    name: str = "memory"
    url: str = ""
    options: dict[str, Any] = Field(default_factory=dict)


class SupervisorConfig(BaseModel):
    """Configuration for a supervisor process."""

    name: str = "default"
    queues: list[str] = Field(default_factory=lambda: ["default"])
    balance: str = "auto"  # auto | simple | false
    min_workers: int = 1
    max_workers: int = 10
    max_jobs_per_worker: int = 0  # 0 = unlimited
    max_time: int = 0  # seconds, 0 = unlimited
    sleep: float = 1.0  # seconds to sleep when queue is empty
    timeout: int = 60  # max job execution time in seconds
    memory_limit: int = 128  # MB


class ScheduleEntry(BaseModel):
    """A scheduled job entry."""

    job_class: str
    cron: str = ""
    every: int = 0  # seconds
    queue: str = "default"
    payload: dict[str, Any] = Field(default_factory=dict)


class BaQueueConfig(BaseModel):
    """Main configuration for BaQueue."""

    prefix: str = "baqueue"
    driver: DriverConfig = Field(default_factory=DriverConfig)
    supervisors: list[SupervisorConfig] = Field(
        default_factory=lambda: [SupervisorConfig()]
    )
    schedules: list[ScheduleEntry] = Field(default_factory=list)
    dashboard_port: int = 9100
    dashboard_host: str = "0.0.0.0"
    metrics_trim_minutes: int = 1440  # 24 hours

    # ── Auto-prune (primary, seconds-based) ─────────────────────
    auto_prune: bool = True
    prune_interval_seconds: int = 60
    prune_completed_seconds: int = 1800  # 30 minutes
    prune_other_seconds: int = 86400  # 1 day — applies to failed + cancelled
    prune_metrics_seconds: int = 604800  # 7 days

    # ── Legacy hour-based overrides (kept for back-compat) ──────
    # When > 0, these take precedence over the seconds fields above for the
    # corresponding status. Default 0 means "use the *_seconds value".
    prune_completed_hours: float = 0
    prune_failed_hours: float = 0
    prune_cancelled_hours: float = 0
    prune_metrics_hours: float = 0

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> BaQueueConfig:
        return cls(**data)
