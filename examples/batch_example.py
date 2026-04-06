"""
Batch job example - dispatch multiple jobs as a batch with callbacks.

Run without dashboard:
    python examples/batch_example.py

Run with dashboard (open http://localhost:9100):
    python examples/batch_example.py --dashboard
"""

import argparse
import asyncio
import logging

from baqueue import BaQueueConfig, Job, Queue, Batch
from baqueue.config import DriverConfig, SupervisorConfig
from baqueue.supervisor import Supervisor

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")


class ProcessItem(Job):
    queue = "batch-work"
    max_attempts = 2

    async def handle(self, item_id: int, **kwargs):
        await asyncio.sleep(0.1)
        print(f"  >> Processed item #{item_id}")


class OnBatchDone(Job):
    queue = "batch-work"

    async def handle(self, **kwargs):
        print("\n  [DONE] Batch completed! All items processed successfully.")


class OnBatchFailed(Job):
    queue = "batch-work"

    async def handle(self, **kwargs):
        print("\n  [ERROR] Batch had failures!")


async def main(with_dashboard: bool = False):
    config = BaQueueConfig(driver=DriverConfig(name="memory"))
    Queue.configure(config)
    await Queue.connect()

    # Create a batch of 10 jobs
    print("=== Dispatching batch of 10 items ===")
    jobs = [(ProcessItem, {"item_id": i}) for i in range(1, 11)]

    result = await Batch(
        Queue.get_driver(), jobs
    ).name("my-batch").then(OnBatchDone).catch(OnBatchFailed).dispatch()

    print(f"Batch ID: {result.batch_id}")
    print(f"Total jobs: {result.total}")

    # Run workers
    print("\n=== Processing batch ===")
    supervisor = Supervisor(
        driver=Queue.get_driver(),
        config=SupervisorConfig(queues=["batch-work"], min_workers=3, max_workers=3, sleep=0.3),
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
        async def stop_later():
            await asyncio.sleep(5)
            await supervisor.stop()
        tasks.append(stop_later())

    await asyncio.gather(*tasks)

    if not with_dashboard:
        print("\n=== Batch status ===")
        batch = await Queue.get_driver().get_batch(result.batch_id)
        print(f"  Completed: {batch.get('completed_count', 0)}/{batch.get('total', 0)}")
        print(f"  Failed: {batch.get('failed_count', 0)}")

    await Queue.disconnect()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dashboard", action="store_true", help="Launch monitoring dashboard on port 9100")
    args = parser.parse_args()
    asyncio.run(main(with_dashboard=args.dashboard))
