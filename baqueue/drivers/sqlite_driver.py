"""SQLite driver for BaQueue - zero-dependency cross-process local storage.

Perfect for development and testing: works across multiple processes
without needing Redis or PostgreSQL.
"""

from __future__ import annotations

import asyncio
import json
import sqlite3
from pathlib import Path
from typing import Any

from baqueue.drivers.base import BaseDriver
from baqueue.serializer import JobPayload, _now_ts

DEFAULT_DB_PATH = ".baqueue.db"


class SqliteDriver(BaseDriver):
    """SQLite-backed driver. Data is stored in a local file so multiple
    processes (dashboard, workers, dispatchers) can share the same queue."""

    def __init__(self, path: str = DEFAULT_DB_PATH, **kwargs: Any):
        self._path = path
        self._conn: sqlite3.Connection | None = None
        self._lock = asyncio.Lock()

    def _get_conn(self) -> sqlite3.Connection:
        if self._conn is None:
            raise RuntimeError("Driver not connected. Call connect() first.")
        return self._conn

    async def connect(self) -> None:
        self._conn = sqlite3.connect(self._path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA busy_timeout=5000")
        self._ensure_tables()

    async def disconnect(self) -> None:
        if self._conn:
            self._conn.close()
            self._conn = None

    def _ensure_tables(self) -> None:
        c = self._get_conn()
        c.executescript("""
            CREATE TABLE IF NOT EXISTS jobs (
                id TEXT PRIMARY KEY,
                job_class TEXT NOT NULL,
                data TEXT NOT NULL DEFAULT '{}',
                queue TEXT NOT NULL DEFAULT 'default',
                status TEXT NOT NULL DEFAULT 'pending',
                attempts INTEGER NOT NULL DEFAULT 0,
                max_attempts INTEGER NOT NULL DEFAULT 3,
                backoff TEXT NOT NULL DEFAULT 'exponential',
                timeout INTEGER NOT NULL DEFAULT 60,
                tags TEXT NOT NULL DEFAULT '[]',
                batch_id TEXT,
                delay_until REAL,
                error TEXT,
                created_at REAL NOT NULL,
                updated_at REAL NOT NULL,
                started_at REAL,
                completed_at REAL,
                failed_at REAL
            );
            CREATE INDEX IF NOT EXISTS idx_jobs_queue_status ON jobs (queue, status);
            CREATE INDEX IF NOT EXISTS idx_jobs_delay ON jobs (delay_until) WHERE delay_until IS NOT NULL;
            CREATE INDEX IF NOT EXISTS idx_jobs_batch ON jobs (batch_id) WHERE batch_id IS NOT NULL;

            CREATE TABLE IF NOT EXISTS batches (
                id TEXT PRIMARY KEY,
                data TEXT NOT NULL DEFAULT '{}'
            );

            CREATE TABLE IF NOT EXISTS metrics (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                queue TEXT NOT NULL,
                metric TEXT NOT NULL,
                value REAL NOT NULL,
                recorded_at REAL NOT NULL
            );
        """)

    def _row_to_payload(self, row: sqlite3.Row) -> JobPayload:
        backoff: str | list[int] = row["backoff"]
        try:
            parsed = json.loads(backoff)
            if isinstance(parsed, list):
                backoff = parsed
        except (json.JSONDecodeError, TypeError):
            pass

        tags = json.loads(row["tags"]) if row["tags"] else []

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
            tags=tags,
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
        async with self._lock:
            self._get_conn().execute(
                """INSERT OR REPLACE INTO jobs
                   (id, job_class, data, queue, status, attempts, max_attempts,
                    backoff, timeout, tags, batch_id, delay_until, created_at, updated_at)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (payload.id, payload.job_class, json.dumps(payload.data),
                 payload.queue, payload.status, payload.attempts, payload.max_attempts,
                 backoff_str, payload.timeout, json.dumps(payload.tags),
                 payload.batch_id, payload.delay_until,
                 payload.created_at, payload.updated_at),
            )
            self._get_conn().commit()
        return payload.id

    async def push_many(self, payloads: list[JobPayload]) -> list[str]:
        now = _now_ts()
        ids = []
        async with self._lock:
            c = self._get_conn()
            for p in payloads:
                p.status = "pending"
                p.updated_at = now
                backoff_str = json.dumps(p.backoff) if isinstance(p.backoff, list) else p.backoff
                c.execute(
                    """INSERT OR REPLACE INTO jobs
                       (id, job_class, data, queue, status, attempts, max_attempts,
                        backoff, timeout, tags, batch_id, delay_until, created_at, updated_at)
                       VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (p.id, p.job_class, json.dumps(p.data),
                     p.queue, p.status, p.attempts, p.max_attempts,
                     backoff_str, p.timeout, json.dumps(p.tags),
                     p.batch_id, p.delay_until,
                     p.created_at, p.updated_at),
                )
                ids.append(p.id)
            c.commit()
        return ids

    async def pop(self, queue: str) -> JobPayload | None:
        now = _now_ts()
        async with self._lock:
            c = self._get_conn()
            row = c.execute(
                """SELECT * FROM jobs
                   WHERE queue=? AND status='pending'
                     AND (delay_until IS NULL OR delay_until <= ?)
                   ORDER BY created_at ASC LIMIT 1""",
                (queue, now),
            ).fetchone()
            if not row:
                return None
            c.execute(
                "UPDATE jobs SET status='processing', started_at=?, updated_at=?, attempts=attempts+1 WHERE id=?",
                (now, now, row["id"]),
            )
            c.commit()
            payload = self._row_to_payload(row)
            payload.status = "processing"
            payload.started_at = now
            payload.updated_at = now
            payload.attempts += 1
            return payload

    async def pop_delayed(self) -> list[JobPayload]:
        now = _now_ts()
        async with self._lock:
            c = self._get_conn()
            rows = c.execute(
                "SELECT * FROM jobs WHERE status='pending' AND delay_until IS NOT NULL AND delay_until <= ?",
                (now,),
            ).fetchall()
            if rows:
                c.execute(
                    "UPDATE jobs SET delay_until=NULL, updated_at=? WHERE status='pending' AND delay_until IS NOT NULL AND delay_until <= ?",
                    (now, now),
                )
                c.commit()
            return [self._row_to_payload(r) for r in rows]

    # ── Job lifecycle ───────────────────────────────────────────

    async def complete(self, payload: JobPayload) -> None:
        now = _now_ts()
        async with self._lock:
            self._get_conn().execute(
                "UPDATE jobs SET status='completed', completed_at=?, updated_at=? WHERE id=?",
                (now, now, payload.id),
            )
            self._get_conn().commit()
        payload.status = "completed"
        payload.completed_at = now

    async def fail(self, payload: JobPayload, error: str) -> None:
        now = _now_ts()
        async with self._lock:
            self._get_conn().execute(
                "UPDATE jobs SET status='failed', failed_at=?, updated_at=?, error=? WHERE id=?",
                (now, now, error, payload.id),
            )
            self._get_conn().commit()
        payload.status = "failed"
        payload.failed_at = now
        payload.error = error

    async def release(self, payload: JobPayload, delay: float = 0) -> None:
        now = _now_ts()
        delay_until = now + delay if delay > 0 else None
        async with self._lock:
            self._get_conn().execute(
                "UPDATE jobs SET status='pending', updated_at=?, delay_until=? WHERE id=?",
                (now, delay_until, payload.id),
            )
            self._get_conn().commit()

    async def delete(self, job_id: str) -> None:
        async with self._lock:
            self._get_conn().execute("DELETE FROM jobs WHERE id=?", (job_id,))
            self._get_conn().commit()

    # ── Query ───────────────────────────────────────────────────

    async def get_job(self, job_id: str) -> JobPayload | None:
        row = self._get_conn().execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
        return self._row_to_payload(row) if row else None

    def _build_where(
        self,
        queue: str | None = None,
        status: str | None = None,
        tag: str | None = None,
        batch_id: str | None = None,
        created_from: float | None = None,
        created_to: float | None = None,
    ) -> tuple[str, list[Any]]:
        conditions: list[str] = []
        params: list[Any] = []
        if queue:
            conditions.append("queue=?")
            params.append(queue)
        if status:
            conditions.append("status=?")
            params.append(status)
        if tag:
            conditions.append("tags LIKE ?")
            params.append(f'%"{tag}"%')
        if batch_id:
            conditions.append("batch_id=?")
            params.append(batch_id)
        if created_from is not None:
            conditions.append("created_at >= ?")
            params.append(created_from)
        if created_to is not None:
            conditions.append("created_at <= ?")
            params.append(created_to)
        where = " AND ".join(conditions) if conditions else "1=1"
        return where, params

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
        where, params = self._build_where(queue, status, tag, batch_id, created_from, created_to)
        params.extend([limit, offset])
        rows = self._get_conn().execute(
            f"SELECT * FROM jobs WHERE {where} ORDER BY created_at DESC LIMIT ? OFFSET ?",
            params,
        ).fetchall()
        return [self._row_to_payload(r) for r in rows]

    async def count_jobs(
        self,
        queue: str | None = None,
        status: str | None = None,
        created_from: float | None = None,
        created_to: float | None = None,
    ) -> int:
        where, params = self._build_where(queue=queue, status=status, created_from=created_from, created_to=created_to)
        row = self._get_conn().execute(
            f"SELECT COUNT(*) as cnt FROM jobs WHERE {where}", params,
        ).fetchone()
        return row["cnt"] if row else 0

    async def size(self, queue: str) -> int:
        row = self._get_conn().execute(
            "SELECT COUNT(*) as cnt FROM jobs WHERE queue=? AND status='pending'",
            (queue,),
        ).fetchone()
        return row["cnt"] if row else 0

    async def queues(self) -> list[str]:
        rows = self._get_conn().execute("SELECT DISTINCT queue FROM jobs ORDER BY queue").fetchall()
        return [r["queue"] for r in rows]

    # ── Metrics ─────────────────────────────────────────────────

    async def record_metric(self, queue: str, metric: str, value: float) -> None:
        async with self._lock:
            self._get_conn().execute(
                "INSERT INTO metrics (queue, metric, value, recorded_at) VALUES (?,?,?,?)",
                (queue, metric, value, _now_ts()),
            )
            self._get_conn().commit()

    async def get_metrics(self, queue: str | None = None) -> dict[str, Any]:
        if queue:
            rows = self._get_conn().execute(
                "SELECT queue, metric, COUNT(*) as cnt FROM metrics WHERE queue=? GROUP BY queue, metric",
                (queue,),
            ).fetchall()
        else:
            rows = self._get_conn().execute(
                "SELECT queue, metric, COUNT(*) as cnt FROM metrics GROUP BY queue, metric"
            ).fetchall()

        result: dict[str, Any] = {}
        for r in rows:
            q = r["queue"]
            if q not in result:
                result[q] = {"pending": 0, "processing": 0, "completed": 0, "failed": 0}
            result[q][r["metric"]] = r["cnt"]

        all_queues = await self.queues()
        for q in all_queues:
            if q not in result:
                result[q] = {"pending": 0, "processing": 0, "completed": 0, "failed": 0}
            result[q]["pending"] = await self.size(q)
            processing = self._get_conn().execute(
                "SELECT COUNT(*) as cnt FROM jobs WHERE queue=? AND status='processing'", (q,),
            ).fetchone()
            result[q]["processing"] = processing["cnt"] if processing else 0

        return result

    # ── Batch helpers ───────────────────────────────────────────

    async def store_batch(self, batch_id: str, data: dict[str, Any]) -> None:
        async with self._lock:
            self._get_conn().execute(
                "INSERT OR REPLACE INTO batches (id, data) VALUES (?, ?)",
                (batch_id, json.dumps(data)),
            )
            self._get_conn().commit()

    async def get_batch(self, batch_id: str) -> dict[str, Any] | None:
        row = self._get_conn().execute("SELECT data FROM batches WHERE id=?", (batch_id,)).fetchone()
        if row:
            return json.loads(row["data"]) if isinstance(row["data"], str) else row["data"]
        return None

    async def update_batch(self, batch_id: str, data: dict[str, Any]) -> None:
        async with self._lock:
            self._get_conn().execute(
                "UPDATE batches SET data=? WHERE id=?",
                (json.dumps(data), batch_id),
            )
            self._get_conn().commit()

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

        if status:
            conditions.append("status=?")
            params.append(status)
        if tag:
            conditions.append("tags LIKE ?")
            params.append(f'%"{tag}"%')
        if older_than_seconds:
            cutoff = _now_ts() - older_than_seconds
            conditions.append("updated_at < ?")
            params.append(cutoff)
        if queue:
            conditions.append("queue=?")
            params.append(queue)

        if not conditions:
            return 0

        where = " AND ".join(conditions)
        async with self._lock:
            c = self._get_conn()
            cursor = c.execute(f"DELETE FROM jobs WHERE {where}", params)
            c.commit()
            return cursor.rowcount

    async def flush(self, queue: str | None = None) -> None:
        async with self._lock:
            c = self._get_conn()
            if queue:
                c.execute("DELETE FROM jobs WHERE queue=?", (queue,))
            else:
                c.executescript("DELETE FROM jobs; DELETE FROM batches; DELETE FROM metrics;")
            c.commit()
