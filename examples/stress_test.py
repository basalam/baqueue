"""
BaQueue Stress Test
===================
Dispatches thousands of jobs across multiple queues with varying
failure rates, delays, and concurrency levels.

Usage:
    python examples/stress_test.py                        # 1000 jobs, 5 workers
    python examples/stress_test.py --jobs 5000 -w 10      # 5000 jobs, 10 workers
    python examples/stress_test.py --dashboard             # with live dashboard
    python examples/stress_test.py --jobs 10000 --bulk     # bulk insert (much faster)

Monitor in another terminal:
    baqueue dashboard
"""

import argparse
import asyncio
import logging
import random
import time

from baqueue import BaQueueConfig, Job, Queue
from baqueue.config import DriverConfig, SupervisorConfig
from baqueue.supervisor import Supervisor

logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)


# ── Job definitions ──────────────────────────────────────────

class FastJob(Job):
    queue = "fast"
    max_attempts = 3
    backoff = "fixed"
    tags = ["stress", "fast"]

    async def handle(self, index: int):
        await asyncio.sleep(random.uniform(0.01, 0.05))


class SlowJob(Job):
    queue = "slow"
    max_attempts = 2
    backoff = "exponential"
    tags = ["stress", "slow"]

    async def handle(self, index: int):
        await asyncio.sleep(random.uniform(0.1, 0.3))


class FlakyJob(Job):
    """Fails ~30% of the time."""
    queue = "flaky"
    max_attempts = 3
    backoff = "exponential"
    tags = ["stress", "flaky"]

    async def handle(self, index: int):
        await asyncio.sleep(random.uniform(0.02, 0.08))
        if random.random() < 0.3:
            raise RuntimeError(f"Random failure on job #{index}")


class HeavyJob(Job):
    """Simulates CPU-heavy work."""
    queue = "heavy"
    max_attempts = 1
    tags = ["stress", "heavy"]

    async def handle(self, index: int, payload_size: int = 100):
        await asyncio.sleep(random.uniform(0.05, 0.15))


@Job.as_job(queue="notifications", max_attempts=2, tags=["stress", "notify"])
async def send_notification(user_id: int, message: str):
    await asyncio.sleep(random.uniform(0.01, 0.04))


# ── Progress tracker ────────────────────────────────────────

class ProgressTracker:
    def __init__(self, total: int):
        self.total = total
        self.start_time = time.time()
        self.last_print = 0

    async def monitor(self, driver):
        while True:
            completed = 0
            failed = 0
            pending = 0
            processing = 0

            for q in await driver.queues():
                metrics = await driver.get_metrics(q)
                m = metrics.get(q, {})
                completed += m.get("completed", 0)
                failed += m.get("failed", 0)
                pending += m.get("pending", 0)
                processing += m.get("processing", 0)

            done = completed + failed
            elapsed = time.time() - self.start_time
            rate = done / elapsed if elapsed > 0 else 0
            pct = done / self.total * 100 if self.total > 0 else 0

            if done != self.last_print or int(elapsed) % 5 == 0:
                bar_len = 40
                filled = int(bar_len * done / self.total) if self.total > 0 else 0
                bar = "#" * filled + "-" * (bar_len - filled)
                print(
                    f"\r  [{bar}] {pct:5.1f}% | "
                    f"{done}/{self.total} done | "
                    f"{completed} ok, {failed} fail | "
                    f"{pending} pending, {processing} proc | "
                    f"{rate:.0f} jobs/s | {elapsed:.1f}s",
                    end="", flush=True,
                )
                self.last_print = done

            if done >= self.total:
                print()
                return elapsed, completed, failed, rate

            await asyncio.sleep(0.5)


# ── Main ────────────────────────────────────────────────────

async def main(
    num_jobs: int,
    num_workers: int,
    with_dashboard: bool,
    use_bulk: bool,
):
    config = BaQueueConfig(driver=DriverConfig(name="sqlite"))
    Queue.configure(config)
    await Queue.connect()
    await Queue.flush()

    print(f"\n{'='*60}")
    print("  BaQueue Stress Test")
    print(f"{'='*60}")
    print(f"  Jobs:    {num_jobs}")
    print(f"  Workers: {num_workers}")
    print(f"  Bulk:    {use_bulk}")
    print("  Driver:  sqlite (.baqueue.db)")
    print(f"{'='*60}\n")

    # ── Dispatch ──────────────────────────────────────────

    job_types = [
        (FastJob,   0.40),  # 40%
        (SlowJob,   0.15),  # 15%
        (FlakyJob,  0.20),  # 20%
        (HeavyJob,  0.10),  # 10%
    ]
    print("  Dispatching jobs...")
    t0 = time.time()

    if use_bulk:
        batch = []
        for i in range(num_jobs):
            r = random.random()
            cumulative = 0
            dispatched = False
            for job_cls, pct in job_types:
                cumulative += pct
                if r < cumulative:
                    batch.append((job_cls, {"index": i}))
                    dispatched = True
                    break
            if not dispatched:
                batch.append((send_notification, {"user_id": i, "message": f"Hello #{i}"}))

        chunk_size = 500
        for start in range(0, len(batch), chunk_size):
            chunk = batch[start:start + chunk_size]
            await Queue.bulk(chunk)
            print(f"\r    Dispatched {min(start + chunk_size, len(batch))}/{num_jobs}...", end="", flush=True)
        print()
    else:
        for i in range(num_jobs):
            r = random.random()
            cumulative = 0
            dispatched = False
            for job_cls, pct in job_types:
                cumulative += pct
                if r < cumulative:
                    await Queue.push(job_cls, index=i)
                    dispatched = True
                    break
            if not dispatched:
                await Queue.push(send_notification, user_id=i, message=f"Hello #{i}")

            if (i + 1) % 200 == 0:
                print(f"\r    Dispatched {i + 1}/{num_jobs}...", end="", flush=True)
        print()

    dispatch_time = time.time() - t0
    print(f"  Dispatched {num_jobs} jobs in {dispatch_time:.2f}s ({num_jobs/dispatch_time:.0f} jobs/s)\n")

    # ── Queue sizes ───────────────────────────────────────

    for q in await Queue.queues():
        size = await Queue.size(q)
        print(f"    {q}: {size} pending")
    print()

    # ── Start workers ─────────────────────────────────────

    all_queues = ["fast", "slow", "flaky", "heavy", "notifications"]
    supervisor = Supervisor(
        driver=Queue.get_driver(),
        config=SupervisorConfig(
            queues=all_queues,
            min_workers=num_workers,
            max_workers=num_workers,
            sleep=0.1,
        ),
    )

    tracker = ProgressTracker(num_jobs)

    tasks = [
        supervisor.start(),
        _run_monitor(tracker, Queue.get_driver(), supervisor),
    ]

    if with_dashboard:
        import uvicorn
        from baqueue.dashboard.server import create_app

        app = create_app(Queue.get_driver(), config)
        server_config = uvicorn.Config(app, host="0.0.0.0", port=9100, log_level="warning")
        server = uvicorn.Server(server_config)
        tasks.append(server.serve())
        print("  Dashboard: http://localhost:9100\n")

    print("  Processing...")
    await asyncio.gather(*tasks)

    await Queue.disconnect()


async def _run_monitor(tracker, driver, supervisor):
    elapsed, completed, failed, rate = await tracker.monitor(driver)

    print(f"\n{'='*60}")
    print("  RESULTS")
    print(f"{'='*60}")
    print(f"  Total time:    {elapsed:.2f}s")
    print(f"  Completed:     {completed}")
    print(f"  Failed:        {failed}")
    print(f"  Throughput:    {rate:.1f} jobs/s")
    print(f"  Success rate:  {completed/(completed+failed)*100:.1f}%" if (completed+failed) > 0 else "  Success rate:  N/A")
    print(f"{'='*60}\n")

    await supervisor.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="BaQueue Stress Test")
    parser.add_argument("--jobs", "-j", type=int, default=1000, help="Number of jobs (default: 1000)")
    parser.add_argument("--workers", "-w", type=int, default=5, help="Number of workers (default: 5)")
    parser.add_argument("--dashboard", action="store_true", help="Launch dashboard on port 9100")
    parser.add_argument("--bulk", action="store_true", help="Use bulk insert for faster dispatching")
    args = parser.parse_args()

    asyncio.run(main(
        num_jobs=args.jobs,
        num_workers=args.workers,
        with_dashboard=args.dashboard,
        use_bulk=args.bulk,
    ))
