"""
Delayed jobs demo — pushes scheduled jobs with various delays so you can see
the "Scheduled" badge in the dashboard.

Run:
    python examples/delayed_jobs_demo.py

Then open http://localhost:9100 and look at the Jobs / Overview tab.
Hover the amber badge to see the exact execution time.
"""

import asyncio
import logging
import random
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from baqueue import BaQueueConfig, Job, Queue
from baqueue.config import DriverConfig, SupervisorConfig
from baqueue.dashboard.server import create_app
from baqueue.supervisor import Supervisor

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")


class SendReminder(Job):
    queue = "reminders"
    max_attempts = 1
    tags = ["reminder"]

    async def handle(self, user: str, message: str, **kwargs):
        print(f"  [REMINDER] {user}: {message}")


class GenerateReport(Job):
    queue = "reports"
    max_attempts = 1
    tags = ["report"]

    async def handle(self, report_type: str, **kwargs):
        await asyncio.sleep(random.uniform(0.2, 0.6))
        print(f"  [REPORT] generated: {report_type}")


class CleanupTask(Job):
    queue = "maintenance"
    max_attempts = 1
    tags = ["cleanup"]

    async def handle(self, target: str, **kwargs):
        print(f"  [CLEANUP] {target}")


async def seed_jobs():
    """Push a variety of delayed jobs with different schedule horizons."""

    # Short delays (30s - 2min) — should fire while you watch
    for i in range(3):
        delay = 30 + i * 30  # 30s, 60s, 90s
        await Queue.later(
            SendReminder,
            delay=delay,
            user=f"user-{i}",
            message=f"Quick reminder #{i}",
        )

    # Medium delays (5 - 15 min) — visible as "in 5m" / "in 15m"
    for i in range(4):
        delay = (5 + i * 3) * 60  # 5m, 8m, 11m, 14m
        await Queue.later(
            GenerateReport,
            delay=delay,
            report_type=f"hourly-batch-{i}",
        )

    # Long delays (1 - 6 hours) — visible as "in 1h" / "in 6h"
    for i in range(3):
        delay = (1 + i * 2) * 3600  # 1h, 3h, 5h
        await Queue.later(
            CleanupTask,
            delay=delay,
            target=f"cache/segment-{i}",
        )

    # Very long delays (1 - 3 days)
    for i in range(2):
        delay = (i + 1) * 86400  # 1d, 2d
        await Queue.later(
            CleanupTask,
            delay=delay,
            target=f"archive/old-logs-{i}",
        )

    # A few immediate jobs as comparison
    for i in range(3):
        await Queue.push(
            SendReminder,
            user=f"now-user-{i}",
            message="Immediate notification",
        )


async def main():
    port = 9100
    config = BaQueueConfig(
        driver=DriverConfig(name="memory"),
        dashboard_port=port,
    )
    Queue.configure(config)
    await Queue.connect()

    print("=== Seeding delayed jobs ===")
    await seed_jobs()
    print("  3 reminders : 30s / 60s / 90s")
    print("  4 reports   : 5m / 8m / 11m / 14m")
    print("  3 cleanups  : 1h / 3h / 5h")
    print("  2 archives  : 1d / 2d")
    print("  3 immediate (no delay)\n")

    supervisor = Supervisor(
        driver=Queue.get_driver(),
        config=SupervisorConfig(
            queues=["reminders", "reports", "maintenance"],
            min_workers=2,
            max_workers=2,
            sleep=0.5,
        ),
    )

    import uvicorn
    app = create_app(Queue.get_driver(), config)

    print(f"=== Dashboard available at http://localhost:{port} ===")
    print("    Look for the amber 'in Xm' badge next to pending jobs.")
    print("    Hover the badge to see the exact execution time.\n")

    server_config = uvicorn.Config(app, host="0.0.0.0", port=port, log_level="warning")
    server = uvicorn.Server(server_config)

    await asyncio.gather(
        supervisor.start(),
        server.serve(),
    )


if __name__ == "__main__":
    asyncio.run(main())
