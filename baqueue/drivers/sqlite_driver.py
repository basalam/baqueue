"""SQLite driver for BaQueue - zero-dependency cross-process local storage.

Perfect for development and testing: works across multiple processes
without needing Redis or PostgreSQL.
"""

from __future__ import annotations

import asyncio
import json
import logging
import sqlite3
from typing import Any

from baqueue.drivers.base import BaseDriver
from baqueue.serializer import JobPayload, _now_ts

DEFAULT_DB_PATH = ".baqueue.db"
MAX_RETRIES = 5
RETRY_BASE_DELAY = 0.05

logger = logging.getLogger("baqueue.sqlite")


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
        self._conn = sqlite3.connect(self._path, check_same_thread=False, timeout=30)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA busy_timeout=15000")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._conn.execute("PRAGMA wal_autocheckpoint=1000")
        self._ensure_tables()

    async def disconnect(self) -> None:
        if self._conn:
            self._conn.close()
            self._conn = None

    def is_storage_full_error(self, exc: BaseException) -> bool:
        if not isinstance(exc, sqlite3.OperationalError):
            return False
        msg = str(exc).lower()
        # SQLite raises e.g. "database or disk is full".
        return (
            "database or disk is full" in msg
            or "disk full" in msg
            or "no space" in msg
            or "out of memory" in msg
        )

    async def emergency_cleanup(self) -> int:
        """Free SQLite space without going through public prune methods.

        SQLite write methods call _execute_with_retry() while holding _lock. The
        disk-full recovery path therefore uses _emergency_cleanup_locked() so it
        does not try to re-acquire the same asyncio.Lock and deadlock.
        """
        async with self._lock:
            return await self._emergency_cleanup_locked()

    async def _emergency_cleanup_locked(self) -> int:
        def _do() -> int:
            c = self._get_conn()
            total = 0
            for status in ("completed", "failed", "cancelled"):
                cur = c.execute("DELETE FROM jobs WHERE status=?", (status,))
                total += cur.rowcount
            cur = c.execute("DELETE FROM metrics WHERE recorded_at < ?", (_now_ts(),))
            total += cur.rowcount
            c.commit()
            return total

        total = await asyncio.to_thread(_do)
        logger.warning("emergency_cleanup removed %d entries due to storage pressure", total)
        return total

    async def _execute_with_retry(self, fn):
        """Execute a database operation with retry on 'database is locked'.

        The caller holds self._lock. Storage-full recovery must stay inside that
        lock and use raw SQL cleanup to avoid re-entering public prune methods.
        """
        async def _attempt():
            for attempt in range(MAX_RETRIES):
                try:
                    return await asyncio.to_thread(fn)
                except sqlite3.OperationalError as e:
                    if "locked" in str(e) and attempt < MAX_RETRIES - 1:
                        delay = RETRY_BASE_DELAY * (2 ** attempt)
                        logger.debug("Database locked, retry %d/%d in %.2fs", attempt + 1, MAX_RETRIES, delay)
                        await asyncio.sleep(delay)
                    else:
                        raise

        if not self.auto_cleanup_on_disk_full or self._in_emergency_cleanup:
            return await _attempt()
        try:
            return await _attempt()
        except Exception as e:
            if not self.is_storage_full_error(e):
                raise
            logger.warning("Storage-full error caught: %s. Running emergency cleanup.", e)
            self._in_emergency_cleanup = True
            try:
                await self._emergency_cleanup_locked()
            finally:
                self._in_emergency_cleanup = False
            return await _attempt()

    async def _run(self, fn):
        """Run a sync read-only sqlite call off the event loop.

        The lock is required because Python's sqlite3 module does not allow
        concurrent use of a single Connection across threads — even with
        check_same_thread=False. Two parallel `to_thread` callbacks hitting the
        same connection raise `InterfaceError: bad parameter or other API misuse`."""
        async with self._lock:
            return await asyncio.to_thread(fn)

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
            CREATE INDEX IF NOT EXISTS idx_jobs_queue_status_created
                ON jobs (queue, status, created_at DESC);
            CREATE INDEX IF NOT EXISTS idx_jobs_created_at ON jobs (created_at DESC);
            CREATE INDEX IF NOT EXISTS idx_jobs_status_created ON jobs (status, created_at DESC);
            CREATE INDEX IF NOT EXISTS idx_jobs_delay ON jobs (delay_until) WHERE delay_until IS NOT NULL;
            CREATE INDEX IF NOT EXISTS idx_jobs_batch ON jobs (batch_id) WHERE batch_id IS NOT NULL;
            DROP INDEX IF EXISTS idx_jobs_queue_status;

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
            CREATE INDEX IF NOT EXISTS idx_metrics_queue_metric ON metrics (queue, metric);
            CREATE INDEX IF NOT EXISTS idx_metrics_recorded_at ON metrics (recorded_at);

            CREATE TABLE IF NOT EXISTS supervisors (
                name TEXT PRIMARY KEY,
                data TEXT NOT NULL DEFAULT '{}',
                heartbeat_at REAL NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_supervisors_heartbeat ON supervisors (heartbeat_at);
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
        params = (payload.id, payload.job_class, json.dumps(payload.data),
                  payload.queue, payload.status, payload.attempts, payload.max_attempts,
                  backoff_str, payload.timeout, json.dumps(payload.tags),
                  payload.batch_id, payload.delay_until,
                  payload.created_at, payload.updated_at)

        async with self._lock:
            def _do():
                c = self._get_conn()
                c.execute(
                    """INSERT OR REPLACE INTO jobs
                       (id, job_class, data, queue, status, attempts, max_attempts,
                        backoff, timeout, tags, batch_id, delay_until, created_at, updated_at)
                       VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", params)
                c.commit()
            await self._execute_with_retry(_do)
        return payload.id

    async def push_many(self, payloads: list[JobPayload]) -> list[str]:
        now = _now_ts()
        ids = []
        rows = []
        for p in payloads:
            p.status = "pending"
            p.updated_at = now
            backoff_str = json.dumps(p.backoff) if isinstance(p.backoff, list) else p.backoff
            rows.append((p.id, p.job_class, json.dumps(p.data),
                         p.queue, p.status, p.attempts, p.max_attempts,
                         backoff_str, p.timeout, json.dumps(p.tags),
                         p.batch_id, p.delay_until, p.created_at, p.updated_at))
            ids.append(p.id)

        async with self._lock:
            def _do():
                c = self._get_conn()
                c.executemany(
                    """INSERT OR REPLACE INTO jobs
                       (id, job_class, data, queue, status, attempts, max_attempts,
                        backoff, timeout, tags, batch_id, delay_until, created_at, updated_at)
                       VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", rows)
                c.commit()
            await self._execute_with_retry(_do)
        return ids

    async def pop(self, queue: str) -> JobPayload | None:
        now = _now_ts()
        async with self._lock:
            result: list[JobPayload | None] = [None]
            def _do():
                c = self._get_conn()
                # Atomic claim: SELECT then UPDATE … WHERE status='pending' guards
                # against another process having grabbed the row between the two
                # statements. Bounded retry so we don't livelock under contention.
                for _ in range(5):
                    row = c.execute(
                        """SELECT * FROM jobs
                           WHERE queue=? AND status='pending'
                             AND (delay_until IS NULL OR delay_until <= ?)
                           ORDER BY created_at ASC LIMIT 1""",
                        (queue, now),
                    ).fetchone()
                    if not row:
                        c.commit()
                        return
                    cur = c.execute(
                        """UPDATE jobs
                           SET status='processing', started_at=?, updated_at=?, attempts=attempts+1
                           WHERE id=? AND status='pending'""",
                        (now, now, row["id"]),
                    )
                    c.commit()
                    if cur.rowcount == 1:
                        payload = self._row_to_payload(row)
                        payload.status = "processing"
                        payload.started_at = now
                        payload.updated_at = now
                        payload.attempts += 1
                        result[0] = payload
                        return
            await self._execute_with_retry(_do)
            return result[0]

    async def pop_delayed(self) -> list[JobPayload]:
        now = _now_ts()
        async with self._lock:
            results = []
            def _do():
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
                results.extend([self._row_to_payload(r) for r in rows])
            await self._execute_with_retry(_do)
            return results

    # ── Job lifecycle ───────────────────────────────────────────

    async def complete(self, payload: JobPayload) -> None:
        now = _now_ts()
        async with self._lock:
            def _do():
                c = self._get_conn()
                c.execute(
                    "UPDATE jobs SET status='completed', completed_at=?, updated_at=? WHERE id=?",
                    (now, now, payload.id),
                )
                c.commit()
            await self._execute_with_retry(_do)
        payload.status = "completed"
        payload.completed_at = now

    async def fail(self, payload: JobPayload, error: str) -> None:
        now = _now_ts()
        async with self._lock:
            def _do():
                c = self._get_conn()
                c.execute(
                    "UPDATE jobs SET status='failed', failed_at=?, updated_at=?, error=? WHERE id=?",
                    (now, now, error, payload.id),
                )
                c.commit()
            await self._execute_with_retry(_do)
        payload.status = "failed"
        payload.failed_at = now
        payload.error = error

    async def release(self, payload: JobPayload, delay: float = 0) -> None:
        now = _now_ts()
        delay_until = now + delay if delay > 0 else None
        async with self._lock:
            def _do():
                c = self._get_conn()
                c.execute(
                    "UPDATE jobs SET status='pending', updated_at=?, delay_until=? WHERE id=?",
                    (now, delay_until, payload.id),
                )
                c.commit()
            await self._execute_with_retry(_do)

    async def delete(self, job_id: str) -> None:
        async with self._lock:
            def _do():
                c = self._get_conn()
                c.execute("DELETE FROM jobs WHERE id=?", (job_id,))
                c.commit()
            await self._execute_with_retry(_do)

    # ── Query ───────────────────────────────────────────────────

    async def get_job(self, job_id: str) -> JobPayload | None:
        def _do():
            return self._get_conn().execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
        row = await self._run(_do)
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
        def _do():
            return self._get_conn().execute(
                f"SELECT * FROM jobs WHERE {where} ORDER BY created_at DESC LIMIT ? OFFSET ?",
                params,
            ).fetchall()
        rows = await self._run(_do)
        return [self._row_to_payload(r) for r in rows]

    async def count_jobs(
        self,
        queue: str | None = None,
        status: str | None = None,
        created_from: float | None = None,
        created_to: float | None = None,
    ) -> int:
        where, params = self._build_where(queue=queue, status=status, created_from=created_from, created_to=created_to)
        def _do():
            return self._get_conn().execute(
                f"SELECT COUNT(*) as cnt FROM jobs WHERE {where}", params,
            ).fetchone()
        row = await self._run(_do)
        return row["cnt"] if row else 0

    async def size(self, queue: str) -> int:
        def _do():
            return self._get_conn().execute(
                "SELECT COUNT(*) as cnt FROM jobs WHERE queue=? AND status='pending'",
                (queue,),
            ).fetchone()
        row = await self._run(_do)
        return row["cnt"] if row else 0

    async def queues(self) -> list[str]:
        def _do():
            return self._get_conn().execute(
                "SELECT DISTINCT queue FROM jobs ORDER BY queue"
            ).fetchall()
        rows = await self._run(_do)
        return [r["queue"] for r in rows]

    # ── Metrics ─────────────────────────────────────────────────

    async def record_metric(self, queue: str, metric: str, value: float) -> None:
        ts = _now_ts()
        async with self._lock:
            def _do():
                c = self._get_conn()
                c.execute("INSERT INTO metrics (queue, metric, value, recorded_at) VALUES (?,?,?,?)",
                          (queue, metric, value, ts))
                c.commit()
            await self._execute_with_retry(_do)

    async def get_metrics(self, queue: str | None = None) -> dict[str, Any]:
        """Live status counts from the jobs table — never from the metrics event log.
        Counting from metrics caused the Overview "Total Jobs" to shrink over time
        as old metric rows aged out and to plateau when metric storage filled up."""
        def _do() -> dict[str, Any]:
            c = self._get_conn()
            if queue:
                status_rows = c.execute(
                    "SELECT queue, status, COUNT(*) AS cnt FROM jobs WHERE queue=? GROUP BY queue, status",
                    (queue,),
                ).fetchall()
                queue_names = [queue]
            else:
                status_rows = c.execute(
                    "SELECT queue, status, COUNT(*) AS cnt FROM jobs GROUP BY queue, status"
                ).fetchall()
                qrows = c.execute("SELECT DISTINCT queue FROM jobs ORDER BY queue").fetchall()
                queue_names = [r["queue"] for r in qrows]

            result: dict[str, Any] = {
                q: {"pending": 0, "processing": 0, "completed": 0, "failed": 0}
                for q in queue_names
            }
            for r in status_rows:
                q = r["queue"]
                if q not in result:
                    result[q] = {"pending": 0, "processing": 0, "completed": 0, "failed": 0}
                if r["status"] in result[q]:
                    result[q][r["status"]] = r["cnt"]
            return result

        return await self._run(_do)

    async def report_supervisor(self, stats: dict[str, Any]) -> None:
        name = str(stats.get("name", "")).strip()
        if not name:
            return
        now = _now_ts()
        payload = json.dumps(stats)
        async with self._lock:
            def _do():
                c = self._get_conn()
                c.execute(
                    """INSERT INTO supervisors (name, data, heartbeat_at)
                       VALUES (?, ?, ?)
                       ON CONFLICT(name) DO UPDATE SET
                           data=excluded.data,
                           heartbeat_at=excluded.heartbeat_at""",
                    (name, payload, now),
                )
                c.commit()
            await self._execute_with_retry(_do)

    async def get_supervisor_stats(self, stale_after: float = 10.0) -> list[dict[str, Any]]:
        cutoff = _now_ts() - stale_after

        def _do():
            return self._get_conn().execute(
                "SELECT data FROM supervisors WHERE heartbeat_at >= ? ORDER BY name",
                (cutoff,),
            ).fetchall()

        rows = await self._run(_do)
        out: list[dict[str, Any]] = []
        for row in rows:
            raw = row["data"]
            try:
                data = json.loads(raw) if isinstance(raw, str) else raw
            except json.JSONDecodeError:
                continue
            if not isinstance(data, dict):
                continue
            if not data.get("running", False):
                continue
            out.append(data)
        return out

    # ── Batch helpers ───────────────────────────────────────────

    async def store_batch(self, batch_id: str, data: dict[str, Any]) -> None:
        json_data = json.dumps(data)
        async with self._lock:
            def _do():
                c = self._get_conn()
                c.execute("INSERT OR REPLACE INTO batches (id, data) VALUES (?, ?)", (batch_id, json_data))
                c.commit()
            await self._execute_with_retry(_do)

    async def get_batch(self, batch_id: str) -> dict[str, Any] | None:
        def _do():
            return self._get_conn().execute(
                "SELECT data FROM batches WHERE id=?", (batch_id,)
            ).fetchone()
        row = await self._run(_do)
        if row:
            return json.loads(row["data"]) if isinstance(row["data"], str) else row["data"]
        return None

    async def update_batch(self, batch_id: str, data: dict[str, Any]) -> None:
        json_data = json.dumps(data)
        async with self._lock:
            def _do():
                c = self._get_conn()
                c.execute("UPDATE batches SET data=? WHERE id=?", (json_data, batch_id))
                c.commit()
            await self._execute_with_retry(_do)

    async def increment_batch_counter(
        self, batch_id: str, field: str, delta: int = 1,
    ) -> dict[str, Any] | None:
        path = f"$.{field}"
        result: list[dict[str, Any] | None] = [None]
        async with self._lock:
            def _do():
                c = self._get_conn()
                c.execute(
                    """UPDATE batches
                       SET data = json_set(
                           data, ?,
                           COALESCE(json_extract(data, ?), 0) + ?
                       )
                       WHERE id = ?""",
                    (path, path, delta, batch_id),
                )
                c.commit()
                row = c.execute("SELECT data FROM batches WHERE id=?", (batch_id,)).fetchone()
                if row is None:
                    return
                raw = row["data"]
                result[0] = json.loads(raw) if isinstance(raw, str) else raw
            await self._execute_with_retry(_do)
        return result[0]

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
        result = [0]
        async with self._lock:
            def _do():
                c = self._get_conn()
                cursor = c.execute(f"DELETE FROM jobs WHERE {where}", params)
                c.commit()
                result[0] = cursor.rowcount
            await self._execute_with_retry(_do)
            return result[0]

    async def flush(self, queue: str | None = None) -> None:
        async with self._lock:
            def _do():
                c = self._get_conn()
                if queue:
                    c.execute("DELETE FROM jobs WHERE queue=?", (queue,))
                else:
                    c.executescript("DELETE FROM jobs; DELETE FROM batches; DELETE FROM metrics; DELETE FROM supervisors;")
                c.commit()
            await self._execute_with_retry(_do)

    async def prune_metrics(self, older_than_seconds: float) -> int:
        cutoff = _now_ts() - older_than_seconds
        result = [0]
        async with self._lock:
            def _do():
                c = self._get_conn()
                cur = c.execute("DELETE FROM metrics WHERE recorded_at < ?", (cutoff,))
                c.commit()
                result[0] = cur.rowcount
            await self._execute_with_retry(_do)
        return result[0]

    async def recent_throughput(
        self, seconds: int = 60, queue: str | None = None,
    ) -> dict[str, int]:
        cutoff = _now_ts() - seconds
        def _do() -> dict[str, int]:
            c = self._get_conn()
            if queue:
                rows = c.execute(
                    """SELECT metric, COUNT(*) AS cnt FROM metrics
                       WHERE recorded_at > ? AND queue = ?
                       GROUP BY metric""",
                    (cutoff, queue),
                ).fetchall()
            else:
                rows = c.execute(
                    """SELECT metric, COUNT(*) AS cnt FROM metrics
                       WHERE recorded_at > ?
                       GROUP BY metric""",
                    (cutoff,),
                ).fetchall()
            out = {"processing": 0, "completed": 0, "failed": 0}
            for r in rows:
                if r["metric"] in out:
                    out[r["metric"]] = r["cnt"]
            return out
        return await self._run(_do)
