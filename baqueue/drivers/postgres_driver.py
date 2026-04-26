"""PostgreSQL driver for BaQueue - reliable transactional queue backend."""

from __future__ import annotations

import json
from typing import Any

from baqueue.drivers.base import BaseDriver
from baqueue.serializer import JobPayload, _now_ts


class PostgresDriver(BaseDriver):
    """PostgreSQL-backed driver using a single jobs table with advisory locks.

    Creates two tables:
        {prefix}_jobs    - all job data
        {prefix}_batches - batch metadata
        {prefix}_metrics - metric entries
    """

    def __init__(self, url: str = "", prefix: str = "baqueue", **kwargs: Any):
        self._url = url
        self._prefix = prefix
        self._kwargs = kwargs
        self._pool: Any = None

    @property
    def _jobs_table(self) -> str:
        return f"{self._prefix}_jobs"

    @property
    def _batches_table(self) -> str:
        return f"{self._prefix}_batches"

    @property
    def _metrics_table(self) -> str:
        return f"{self._prefix}_metrics"

    async def connect(self) -> None:
        try:
            import asyncpg
        except ImportError:
            raise ImportError(
                "PostgreSQL driver requires the 'asyncpg' package.\n"
                "Install it with: pip install baqueue[postgres]"
            ) from None
        self._pool = await asyncpg.create_pool(self._url, **self._kwargs)
        await self._ensure_tables()

    async def disconnect(self) -> None:
        if self._pool:
            await self._pool.close()
            self._pool = None

    async def _ensure_tables(self) -> None:
        async with self._pool.acquire() as conn:
            await conn.execute(f"""
                CREATE TABLE IF NOT EXISTS {self._jobs_table} (
                    id TEXT PRIMARY KEY,
                    job_class TEXT NOT NULL,
                    data JSONB NOT NULL DEFAULT '{{}}',
                    queue TEXT NOT NULL DEFAULT 'default',
                    status TEXT NOT NULL DEFAULT 'pending',
                    attempts INTEGER NOT NULL DEFAULT 0,
                    max_attempts INTEGER NOT NULL DEFAULT 3,
                    backoff TEXT NOT NULL DEFAULT 'exponential',
                    timeout INTEGER NOT NULL DEFAULT 60,
                    tags TEXT[] NOT NULL DEFAULT '{{}}',
                    batch_id TEXT,
                    delay_until DOUBLE PRECISION,
                    error TEXT,
                    created_at DOUBLE PRECISION NOT NULL,
                    updated_at DOUBLE PRECISION NOT NULL,
                    started_at DOUBLE PRECISION,
                    completed_at DOUBLE PRECISION,
                    failed_at DOUBLE PRECISION
                )
            """)
            await conn.execute(f"""
                CREATE INDEX IF NOT EXISTS idx_{self._prefix}_queue_status_created
                ON {self._jobs_table} (queue, status, created_at DESC)
            """)
            await conn.execute(f"""
                CREATE INDEX IF NOT EXISTS idx_{self._prefix}_created_at
                ON {self._jobs_table} (created_at DESC)
            """)
            await conn.execute(f"""
                CREATE INDEX IF NOT EXISTS idx_{self._prefix}_status_created
                ON {self._jobs_table} (status, created_at DESC)
            """)
            await conn.execute(f"""
                CREATE INDEX IF NOT EXISTS idx_{self._prefix}_delay
                ON {self._jobs_table} (delay_until) WHERE delay_until IS NOT NULL
            """)
            await conn.execute(f"""
                CREATE INDEX IF NOT EXISTS idx_{self._prefix}_batch
                ON {self._jobs_table} (batch_id) WHERE batch_id IS NOT NULL
            """)
            await conn.execute(f"DROP INDEX IF EXISTS idx_{self._prefix}_queue_status")
            await conn.execute(f"""
                CREATE TABLE IF NOT EXISTS {self._batches_table} (
                    id TEXT PRIMARY KEY,
                    data JSONB NOT NULL DEFAULT '{{}}'
                )
            """)
            await conn.execute(f"""
                CREATE TABLE IF NOT EXISTS {self._metrics_table} (
                    id SERIAL PRIMARY KEY,
                    queue TEXT NOT NULL,
                    metric TEXT NOT NULL,
                    value DOUBLE PRECISION NOT NULL,
                    recorded_at DOUBLE PRECISION NOT NULL
                )
            """)
            await conn.execute(f"""
                CREATE INDEX IF NOT EXISTS idx_{self._prefix}_metrics_queue_metric
                ON {self._metrics_table} (queue, metric)
            """)
            await conn.execute(f"""
                CREATE INDEX IF NOT EXISTS idx_{self._prefix}_metrics_recorded_at
                ON {self._metrics_table} (recorded_at)
            """)

    def _row_to_payload(self, row: Any) -> JobPayload:
        backoff: str | list[int] = row["backoff"]
        try:
            parsed = json.loads(backoff)
            if isinstance(parsed, list):
                backoff = parsed
        except (json.JSONDecodeError, TypeError):
            pass

        return JobPayload(
            id=row["id"],
            job_class=row["job_class"],
            data=json.loads(row["data"]) if isinstance(row["data"], str) else row["data"],
            queue=row["queue"],
            status=row["status"],
            attempts=row["attempts"],
            max_attempts=row["max_attempts"],
            backoff=backoff,
            timeout=row["timeout"],
            tags=list(row["tags"]) if row["tags"] else [],
            batch_id=row["batch_id"],
            delay_until=row["delay_until"],
            error=row["error"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
            started_at=row["started_at"],
            completed_at=row["completed_at"],
            failed_at=row["failed_at"],
        )

    # ── Push / Pop ──────────────────────────────────────────────

    async def push(self, payload: JobPayload) -> str:
        payload.status = "pending"
        payload.updated_at = _now_ts()
        backoff_str = json.dumps(payload.backoff) if isinstance(payload.backoff, list) else payload.backoff

        async with self._pool.acquire() as conn:
            await conn.execute(
                f"""INSERT INTO {self._jobs_table}
                    (id, job_class, data, queue, status, attempts, max_attempts,
                     backoff, timeout, tags, batch_id, delay_until, created_at, updated_at)
                    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)""",
                payload.id, payload.job_class, json.dumps(payload.data),
                payload.queue, payload.status, payload.attempts, payload.max_attempts,
                backoff_str, payload.timeout, payload.tags,
                payload.batch_id, payload.delay_until,
                payload.created_at, payload.updated_at,
            )
        return payload.id

    async def push_many(self, payloads: list[JobPayload]) -> list[str]:
        ids = []
        now = _now_ts()
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                for p in payloads:
                    p.status = "pending"
                    p.updated_at = now
                    backoff_str = json.dumps(p.backoff) if isinstance(p.backoff, list) else p.backoff
                    await conn.execute(
                        f"""INSERT INTO {self._jobs_table}
                            (id, job_class, data, queue, status, attempts, max_attempts,
                             backoff, timeout, tags, batch_id, delay_until, created_at, updated_at)
                            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)""",
                        p.id, p.job_class, json.dumps(p.data),
                        p.queue, p.status, p.attempts, p.max_attempts,
                        backoff_str, p.timeout, p.tags,
                        p.batch_id, p.delay_until,
                        p.created_at, p.updated_at,
                    )
                    ids.append(p.id)
        return ids

    async def pop(self, queue: str) -> JobPayload | None:
        now = _now_ts()
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                f"""UPDATE {self._jobs_table}
                    SET status='processing', started_at=$1, updated_at=$1, attempts=attempts+1
                    WHERE id = (
                        SELECT id FROM {self._jobs_table}
                        WHERE queue=$2 AND status='pending'
                          AND (delay_until IS NULL OR delay_until <= $1)
                        ORDER BY created_at ASC
                        FOR UPDATE SKIP LOCKED
                        LIMIT 1
                    )
                    RETURNING *""",
                now, queue,
            )
        if row:
            return self._row_to_payload(row)
        return None

    async def pop_delayed(self) -> list[JobPayload]:
        now = _now_ts()
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                f"""UPDATE {self._jobs_table}
                    SET delay_until=NULL, updated_at=$1
                    WHERE status='pending' AND delay_until IS NOT NULL AND delay_until <= $1
                    RETURNING *""",
                now,
            )
        return [self._row_to_payload(r) for r in rows]

    # ── Job lifecycle ───────────────────────────────────────────

    async def complete(self, payload: JobPayload) -> None:
        now = _now_ts()
        async with self._pool.acquire() as conn:
            await conn.execute(
                f"UPDATE {self._jobs_table} SET status='completed', completed_at=$1, updated_at=$1 WHERE id=$2",
                now, payload.id,
            )
        payload.status = "completed"
        payload.completed_at = now

    async def fail(self, payload: JobPayload, error: str) -> None:
        now = _now_ts()
        async with self._pool.acquire() as conn:
            await conn.execute(
                f"UPDATE {self._jobs_table} SET status='failed', failed_at=$1, updated_at=$1, error=$2 WHERE id=$3",
                now, error, payload.id,
            )
        payload.status = "failed"
        payload.failed_at = now
        payload.error = error

    async def release(self, payload: JobPayload, delay: float = 0) -> None:
        now = _now_ts()
        delay_until = now + delay if delay > 0 else None
        async with self._pool.acquire() as conn:
            await conn.execute(
                f"UPDATE {self._jobs_table} SET status='pending', updated_at=$1, delay_until=$2 WHERE id=$3",
                now, delay_until, payload.id,
            )

    async def delete(self, job_id: str) -> None:
        async with self._pool.acquire() as conn:
            await conn.execute(f"DELETE FROM {self._jobs_table} WHERE id=$1", job_id)

    # ── Query ───────────────────────────────────────────────────

    async def get_job(self, job_id: str) -> JobPayload | None:
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(f"SELECT * FROM {self._jobs_table} WHERE id=$1", job_id)
        return self._row_to_payload(row) if row else None

    def _build_where(
        self,
        queue: str | None = None,
        status: str | None = None,
        tag: str | None = None,
        batch_id: str | None = None,
        created_from: float | None = None,
        created_to: float | None = None,
        start_idx: int = 1,
    ) -> tuple[str, list[Any], int]:
        conditions: list[str] = []
        params: list[Any] = []
        idx = start_idx

        if queue:
            conditions.append(f"queue=${idx}")
            params.append(queue)
            idx += 1
        if status:
            conditions.append(f"status=${idx}")
            params.append(status)
            idx += 1
        if tag:
            conditions.append(f"${idx} = ANY(tags)")
            params.append(tag)
            idx += 1
        if batch_id:
            conditions.append(f"batch_id=${idx}")
            params.append(batch_id)
            idx += 1
        if created_from is not None:
            conditions.append(f"created_at >= ${idx}")
            params.append(created_from)
            idx += 1
        if created_to is not None:
            conditions.append(f"created_at <= ${idx}")
            params.append(created_to)
            idx += 1

        where = " AND ".join(conditions) if conditions else "TRUE"
        return where, params, idx

    async def get_jobs(
        self,
        queue: str | None = None,
        status: str | None = None,
        tag: str | None = None,
        batch_id: str | None = None,
        offset: int = 0,
        limit: int = 50,
        created_from: float | None = None,
        created_to: float | None = None,
    ) -> list[JobPayload]:
        where, params, idx = self._build_where(
            queue, status, tag, batch_id, created_from, created_to,
        )
        params.extend([limit, offset])

        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                f"SELECT * FROM {self._jobs_table} WHERE {where} ORDER BY created_at DESC LIMIT ${idx} OFFSET ${idx+1}",
                *params,
            )
        return [self._row_to_payload(r) for r in rows]

    async def count_jobs(
        self,
        queue: str | None = None,
        status: str | None = None,
        created_from: float | None = None,
        created_to: float | None = None,
    ) -> int:
        where, params, _ = self._build_where(
            queue=queue, status=status,
            created_from=created_from, created_to=created_to,
        )
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                f"SELECT COUNT(*) AS cnt FROM {self._jobs_table} WHERE {where}",
                *params,
            )
        return int(row["cnt"]) if row else 0

    async def size(self, queue: str) -> int:
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                f"SELECT COUNT(*) as cnt FROM {self._jobs_table} WHERE queue=$1 AND status='pending'",
                queue,
            )
        return row["cnt"] if row else 0

    async def queues(self) -> list[str]:
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(f"SELECT DISTINCT queue FROM {self._jobs_table} ORDER BY queue")
        return [r["queue"] for r in rows]

    # ── Metrics ─────────────────────────────────────────────────

    async def record_metric(self, queue: str, metric: str, value: float) -> None:
        async with self._pool.acquire() as conn:
            await conn.execute(
                f"INSERT INTO {self._metrics_table} (queue, metric, value, recorded_at) VALUES ($1,$2,$3,$4)",
                queue, metric, value, _now_ts(),
            )

    async def get_metrics(self, queue: str | None = None) -> dict[str, Any]:
        async with self._pool.acquire() as conn:
            if queue:
                rows = await conn.fetch(
                    f"SELECT queue, metric, COUNT(*) as cnt FROM {self._metrics_table} WHERE queue=$1 GROUP BY queue, metric",
                    queue,
                )
            else:
                rows = await conn.fetch(
                    f"SELECT queue, metric, COUNT(*) as cnt FROM {self._metrics_table} GROUP BY queue, metric"
                )

        result: dict[str, Any] = {}
        for r in rows:
            q = r["queue"]
            if q not in result:
                result[q] = {"pending": 0, "processing": 0, "completed": 0, "failed": 0}
            result[q][r["metric"]] = r["cnt"]

        if queue and queue not in result:
            pending = await self.size(queue)
            result[queue] = {"pending": pending, "processing": 0, "completed": 0, "failed": 0}
        return result

    # ── Batch helpers ───────────────────────────────────────────

    async def store_batch(self, batch_id: str, data: dict[str, Any]) -> None:
        async with self._pool.acquire() as conn:
            await conn.execute(
                f"INSERT INTO {self._batches_table} (id, data) VALUES ($1, $2)",
                batch_id, json.dumps(data),
            )

    async def get_batch(self, batch_id: str) -> dict[str, Any] | None:
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(f"SELECT data FROM {self._batches_table} WHERE id=$1", batch_id)
        if row:
            return json.loads(row["data"]) if isinstance(row["data"], str) else row["data"]
        return None

    async def update_batch(self, batch_id: str, data: dict[str, Any]) -> None:
        async with self._pool.acquire() as conn:
            await conn.execute(
                f"UPDATE {self._batches_table} SET data=$1 WHERE id=$2",
                json.dumps(data), batch_id,
            )

    async def increment_batch_counter(
        self, batch_id: str, field: str, delta: int = 1,
    ) -> dict[str, Any] | None:
        # jsonb_set with COALESCE so missing fields start at 0. Returns the
        # post-update row in a single statement -> atomic.
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                f"""UPDATE {self._batches_table}
                    SET data = jsonb_set(
                        data,
                        ARRAY[$1],
                        to_jsonb(COALESCE((data->>$1)::int, 0) + $2)
                    )
                    WHERE id = $3
                    RETURNING data""",
                field, delta, batch_id,
            )
        if row is None:
            return None
        raw = row["data"]
        return json.loads(raw) if isinstance(raw, str) else dict(raw)

    # ── Pruning ─────────────────────────────────────────────────

    async def prune(
        self,
        status: str | None = None,
        tag: str | None = None,
        older_than_seconds: float | None = None,
        queue: str | None = None,
    ) -> int:
        conditions = []
        params: list[Any] = []
        idx = 1

        if status:
            conditions.append(f"status=${idx}")
            params.append(status)
            idx += 1
        if tag:
            conditions.append(f"${idx} = ANY(tags)")
            params.append(tag)
            idx += 1
        if older_than_seconds:
            cutoff = _now_ts() - older_than_seconds
            conditions.append(f"updated_at < ${idx}")
            params.append(cutoff)
            idx += 1
        if queue:
            conditions.append(f"queue=${idx}")
            params.append(queue)
            idx += 1

        if not conditions:
            return 0

        where = " AND ".join(conditions)
        async with self._pool.acquire() as conn:
            result = await conn.execute(f"DELETE FROM {self._jobs_table} WHERE {where}", *params)
        return int(result.split()[-1])

    async def flush(self, queue: str | None = None) -> None:
        async with self._pool.acquire() as conn:
            if queue:
                await conn.execute(f"DELETE FROM {self._jobs_table} WHERE queue=$1", queue)
            else:
                await conn.execute(f"TRUNCATE {self._jobs_table}, {self._batches_table}, {self._metrics_table}")

    async def prune_metrics(self, older_than_seconds: float) -> int:
        cutoff = _now_ts() - older_than_seconds
        async with self._pool.acquire() as conn:
            result = await conn.execute(
                f"DELETE FROM {self._metrics_table} WHERE recorded_at < $1", cutoff,
            )
        return int(result.split()[-1])

    async def recent_throughput(
        self, seconds: int = 60, queue: str | None = None,
    ) -> dict[str, int]:
        cutoff = _now_ts() - seconds
        async with self._pool.acquire() as conn:
            if queue:
                rows = await conn.fetch(
                    f"""SELECT metric, COUNT(*) AS cnt FROM {self._metrics_table}
                        WHERE recorded_at > $1 AND queue = $2
                        GROUP BY metric""",
                    cutoff, queue,
                )
            else:
                rows = await conn.fetch(
                    f"""SELECT metric, COUNT(*) AS cnt FROM {self._metrics_table}
                        WHERE recorded_at > $1
                        GROUP BY metric""",
                    cutoff,
                )
        out = {"processing": 0, "completed": 0, "failed": 0}
        for r in rows:
            if r["metric"] in out:
                out[r["metric"]] = int(r["cnt"])
        return out
