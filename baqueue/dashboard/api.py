"""REST API endpoints for the BaQueue dashboard."""

from __future__ import annotations

from typing import Any

from baqueue.drivers.base import BaseDriver
from baqueue.serializer import _now_ts


class DashboardAPI:
    """Provides data for the dashboard REST endpoints."""

    def __init__(self, driver: BaseDriver):
        self.driver = driver
        self._supervisor_stats: list[dict[str, Any]] = []

    def set_supervisor_stats(self, stats: list[dict[str, Any]]) -> None:
        self._supervisor_stats = stats

    async def overview(self) -> dict[str, Any]:
        queues = await self.driver.queues()
        metrics = await self.driver.get_metrics()

        total_pending = 0
        total_processing = 0
        total_completed = 0
        total_failed = 0
        queue_details = []

        for q in queues:
            m = metrics.get(q, {})
            pending = m.get("pending", 0)
            processing = m.get("processing", 0)
            completed = m.get("completed", 0)
            failed = m.get("failed", 0)

            total_pending += pending
            total_processing += processing
            total_completed += completed
            total_failed += failed

            queue_details.append({
                "name": q,
                "pending": pending,
                "processing": processing,
                "completed": completed,
                "failed": failed,
            })

        return {
            "totals": {
                "pending": total_pending,
                "processing": total_processing,
                "completed": total_completed,
                "failed": total_failed,
                "queues": len(queues),
                "total": total_pending + total_processing + total_completed + total_failed,
            },
            "queues": queue_details,
            "supervisors": self._supervisor_stats,
            "timestamp": _now_ts(),
        }

    async def queue_detail(self, queue: str) -> dict[str, Any]:
        metrics = await self.driver.get_metrics(queue)
        size = await self.driver.size(queue)
        m = metrics.get(queue, {})
        return {
            "name": queue,
            "size": size,
            "metrics": m,
        }

    async def jobs_list(
        self,
        queue: str | None = None,
        status: str | None = None,
        tag: str | None = None,
        batch_id: str | None = None,
        page: int = 1,
        per_page: int = 25,
        created_from: float | None = None,
        created_to: float | None = None,
    ) -> dict[str, Any]:
        offset = (page - 1) * per_page
        jobs = await self.driver.get_jobs(
            queue=queue, status=status, tag=tag, batch_id=batch_id,
            offset=offset, limit=per_page,
            created_from=created_from, created_to=created_to,
        )
        total = await self.driver.count_jobs(
            queue=queue, status=status,
            created_from=created_from, created_to=created_to,
        )
        return {
            "jobs": [j.to_dict() for j in jobs],
            "page": page,
            "per_page": per_page,
            "count": len(jobs),
            "total": total,
        }

    async def job_detail(self, job_id: str) -> dict[str, Any] | None:
        job = await self.driver.get_job(job_id)
        return job.to_dict() if job else None

    async def retry_job(self, job_id: str) -> bool:
        job = await self.driver.get_job(job_id)
        if not job or job.status != "failed":
            return False
        await self.driver.release(job, delay=0)
        return True

    async def delete_job(self, job_id: str) -> bool:
        await self.driver.delete(job_id)
        return True

    async def batch_detail(self, batch_id: str) -> dict[str, Any] | None:
        return await self.driver.get_batch(batch_id)

    async def prune_jobs(
        self,
        status: str | None = None,
        tag: str | None = None,
        hours: float | None = None,
    ) -> int:
        older_than = hours * 3600 if hours else None
        return await self.driver.prune(status=status, tag=tag, older_than_seconds=older_than)

    async def metrics_snapshot(self) -> dict[str, Any]:
        return await self.driver.get_metrics()

    async def stats(self, created_from: float | None = None, created_to: float | None = None) -> dict[str, Any]:
        """Aggregate statistics for the stats panel."""
        queues = await self.driver.queues()
        result: dict[str, Any] = {"queues": {}, "statuses": {}}

        for st in ("pending", "processing", "completed", "failed"):
            result["statuses"][st] = await self.driver.count_jobs(status=st, created_from=created_from, created_to=created_to)

        for q in queues:
            q_stats: dict[str, int] = {}
            for st in ("pending", "processing", "completed", "failed"):
                q_stats[st] = await self.driver.count_jobs(queue=q, status=st, created_from=created_from, created_to=created_to)
            q_stats["total"] = sum(q_stats.values())
            result["queues"][q] = q_stats

        result["total"] = sum(result["statuses"].values())
        return result
