"""
Scheduled jobs example - run jobs on intervals.

Run:
    python examples/scheduled_example.py
"""

import asyncio
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from baqueue import BaQueueConfig, Job, Queue
from baqueue.config import DriverConfig, ScheduleEntry, SupervisorConfig
from baqueue.scheduler import Scheduler
from baqueue.supervisor import Supervisor

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")


class Heartbeat(Job):
    queue = "scheduled"
    max_attempts = 1

    async def handle(self, **kwargs):
        now = datetime.now(timezone.utc).strftime("%H:%M:%S")
        print(f"  [HEARTBEAT] at {now}")


class Cleanup(Job):
    queue = "scheduled"
    max_attempts = 1

    async def handle(self, **kwargs):
        now = datetime.now(timezone.utc).strftime("%H:%M:%S")
        print(f"  [CLEANUP] at {now}")


async def main():
    config = BaQueueConfig(driver=DriverConfig(name="memory"))
    Queue.configure(config)
    await Queue.connect()

    # Scheduler: heartbeat every 3 seconds, cleanup every 5 seconds
    scheduler = Scheduler(
        driver=Queue.get_driver(),
        entries=[
            ScheduleEntry(
                job_class="examples.scheduled_example.Heartbeat",
                every=3,
                queue="scheduled",
            ),
            ScheduleEntry(
                job_class="examples.scheduled_example.Cleanup",
                every=5,
                queue="scheduled",
            ),
        ],
    )

    supervisor = Supervisor(
        driver=Queue.get_driver(),
        config=SupervisorConfig(queues=["scheduled"], min_workers=1, max_workers=1, sleep=0.5),
    )

    print("=== Scheduler running (15 seconds) ===")
    print("  Heartbeat every 3s, Cleanup every 5s\n")

    async def stop_later():
        await asyncio.sleep(15)
        scheduler.stop()
        await supervisor.stop()

    await asyncio.gather(
        scheduler.start(),
        supervisor.start(),
        stop_later(),
    )

    await Queue.disconnect()
    print("\nDone!")


if __name__ == "__main__":
    asyncio.run(main())
