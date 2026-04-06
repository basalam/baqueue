"""
Simplest possible BaQueue example - using the SQLite driver.
No Redis or PostgreSQL needed! Works across multiple processes.

Run jobs (they go into .baqueue.db):
    python examples/simple_job.py

Monitor in real-time (in another terminal):
    baqueue dashboard

Run with in-process dashboard instead:
    python examples/simple_job.py --dashboard
"""

import argparse
import asyncio
import logging

from baqueue import BaQueueConfig, Job, Queue
from baqueue.config import DriverConfig
from baqueue.supervisor import Supervisor
from baqueue.config import SupervisorConfig

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")


# ── 1. Define a job ─────────────────────────────────────────

class SayHello(Job):
    queue = "greetings"
    max_attempts = 2
    backoff = "exponential"

    async def handle(self, name: str, message: str = "Hello"):
        await asyncio.sleep(0.2)  # simulate work
        print(f"  >> {message}, {name}!")

    async def on_success(self, result, payload):
        print(f"  [OK] Job {payload.id[:8]} completed successfully")

    async def on_failure(self, error, payload):
        print(f"  [FAIL] Job {payload.id[:8]} failed: {error}")


# ── 2. Define a job using decorator ─────────────────────────

@Job.as_job(queue="math", max_attempts=1)
async def compute_square(number: int):
    result = number ** 2
    print(f"  >> {number}^2 = {result}")
    return result


# ── 3. Run everything ───────────────────────────────────────

async def main(with_dashboard: bool = False):
    config = BaQueueConfig(
        driver=DriverConfig(name="sqlite"),
    )
    Queue.configure(config)
    await Queue.connect()

    # Push some jobs
    print("\n=== Dispatching jobs ===")
    await Queue.push(SayHello, name="Alice")
    await Queue.push(SayHello, name="Bob", message="Hi there")
    await Queue.push(SayHello, name="Charlie", message="Salam")

    await Queue.push(compute_square, number=7)
    await Queue.push(compute_square, number=12)

    # Push a delayed job (3 second delay)
    await Queue.later(SayHello, delay=3, name="Delayed Dave", message="Better late than never")

    print(f"Queue 'greetings' size: {await Queue.size('greetings')}")
    print(f"Queue 'math' size: {await Queue.size('math')}")

    # Start workers
    print("\n=== Starting workers ===")
    supervisor = Supervisor(
        driver=Queue.get_driver(),
        config=SupervisorConfig(
            queues=["greetings", "math"],
            min_workers=2,
            max_workers=2,
            sleep=0.5,
        ),
    )

    tasks = [supervisor.start()]

    if with_dashboard:
        import uvicorn
        from baqueue.dashboard.server import create_app

        app = create_app(Queue.get_driver(), config)
        server_config = uvicorn.Config(app, host="0.0.0.0", port=9100, log_level="warning")
        server = uvicorn.Server(server_config)
        tasks.append(server.serve())
        print("\n=== Dashboard: http://localhost:9100 ===\n")
    else:
        async def stop_after_delay():
            await asyncio.sleep(6)
            await supervisor.stop()
        tasks.append(stop_after_delay())

    await asyncio.gather(*tasks)

    # Show final metrics
    if not with_dashboard:
        print("\n=== Final Metrics ===")
        metrics = await Queue.metrics()
        for q, m in metrics.items():
            print(f"  {q}: {m}")

    await Queue.disconnect()
    print("\nDone!")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dashboard", action="store_true", help="Launch monitoring dashboard on port 9100")
    args = parser.parse_args()
    asyncio.run(main(with_dashboard=args.dashboard))
