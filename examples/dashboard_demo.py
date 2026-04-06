"""
Dashboard demo - pushes sample jobs and launches the monitoring dashboard.

Run:
    pip install baqueue[all]
    python examples/dashboard_demo.py

Then open http://localhost:9100 in your browser.
"""

import asyncio
import logging
import random

from baqueue import BaQueueConfig, Job, Queue, Batch
from baqueue.config import DriverConfig, SupervisorConfig
from baqueue.dashboard.server import create_app
from baqueue.supervisor import Supervisor

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")


class SendEmail(Job):
    queue = "emails"
    max_attempts = 3
    backoff = "exponential"
    tags = ["email", "notification"]

    async def handle(self, to: str, subject: str, **kwargs):
        await asyncio.sleep(random.uniform(0.1, 0.5))
        if random.random() < 0.1:  # 10% failure rate
            raise RuntimeError(f"SMTP connection timeout for {to}")
        print(f"  [EMAIL] Sent to {to}: {subject}")


class ProcessPayment(Job):
    queue = "payments"
    max_attempts = 5
    backoff = [5, 15, 60, 300, 900]
    tags = ["payment", "critical"]

    async def handle(self, order_id: int, amount: float, **kwargs):
        await asyncio.sleep(random.uniform(0.2, 0.8))
        print(f"  [PAYMENT] #{order_id}: ${amount:.2f}")


class GenerateReport(Job):
    queue = "reports"
    max_attempts = 2
    tags = ["report"]

    async def handle(self, report_type: str, **kwargs):
        await asyncio.sleep(random.uniform(0.5, 1.5))
        print(f"  [REPORT] generated: {report_type}")


async def seed_jobs():
    """Push sample jobs for the dashboard demo."""
    # Individual jobs
    for i in range(20):
        await Queue.push(
            SendEmail,
            to=f"user{i}@example.com",
            subject=f"Welcome #{i}",
        )

    for i in range(10):
        await Queue.push(
            ProcessPayment,
            order_id=1000 + i,
            amount=round(random.uniform(10, 500), 2),
        )

    # Delayed jobs
    for i in range(5):
        await Queue.later(
            GenerateReport,
            delay=random.uniform(2, 10),
            report_type=f"monthly-{i}",
        )

    # Batch
    batch_jobs = [(SendEmail, {"to": f"batch{i}@example.com", "subject": f"Batch #{i}"}) for i in range(15)]
    await Batch(Queue.get_driver(), batch_jobs).name("welcome-campaign").dispatch()


async def main():
    port = 9100
    config = BaQueueConfig(
        driver=DriverConfig(name="memory"),
        dashboard_port=port,
    )
    Queue.configure(config)
    await Queue.connect()

    # Seed sample data
    print("=== Seeding sample jobs ===")
    await seed_jobs()
    print(f"  Emails queue: {await Queue.size('emails')} jobs")
    print(f"  Payments queue: {await Queue.size('payments')} jobs")
    print(f"  Reports queue: {await Queue.size('reports')} jobs")

    # Start workers
    supervisor = Supervisor(
        driver=Queue.get_driver(),
        config=SupervisorConfig(
            queues=["emails", "payments", "reports"],
            min_workers=3,
            max_workers=3,
            sleep=0.5,
        ),
    )

    # Start dashboard
    import uvicorn
    app = create_app(Queue.get_driver(), config)

    print(f"\n=== Dashboard available at http://localhost:{port} ===\n")

    server_config = uvicorn.Config(app, host="0.0.0.0", port=port, log_level="warning")
    server = uvicorn.Server(server_config)

    await asyncio.gather(
        supervisor.start(),
        server.serve(),
    )


if __name__ == "__main__":
    asyncio.run(main())
