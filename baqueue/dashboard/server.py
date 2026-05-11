"""FastAPI server for the BaQueue monitoring dashboard.

NOTE: Do NOT use `from __future__ import annotations` in this module.
FastAPI relies on runtime type inspection for WebSocket parameter resolution,
which breaks with PEP 563 deferred annotations.
"""

import asyncio
import logging
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, Optional

from baqueue.config import BaQueueConfig
from baqueue.dashboard.api import DashboardAPI
from baqueue.drivers.base import BaseDriver

logger = logging.getLogger("baqueue.dashboard")

STATIC_DIR = Path(__file__).parent / "static"

OVERVIEW_BROADCAST_INTERVAL = 2.0
JOBS_PUSH_INTERVAL = 1.5


def create_app(driver: BaseDriver, config: Optional[BaQueueConfig] = None) -> Any:
    """Create and return a FastAPI application for the dashboard."""
    from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Query
    from fastapi.responses import JSONResponse, FileResponse

    api = DashboardAPI(driver)
    connected_ws: list[WebSocket] = []

    async def _broadcast_loop() -> None:
        """One overview() per interval, fanned out to every connected WS.
        Replaces N independent driver queries when N tabs are open."""
        while True:
            try:
                await asyncio.sleep(OVERVIEW_BROADCAST_INTERVAL)
                if not connected_ws:
                    continue
                data = await api.overview()
                stale: list[WebSocket] = []
                for ws in list(connected_ws):
                    try:
                        await ws.send_json(data)
                    except Exception:
                        stale.append(ws)
                for ws in stale:
                    if ws in connected_ws:
                        connected_ws.remove(ws)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Error in overview broadcast loop")

    @asynccontextmanager
    async def lifespan(_app):
        task = asyncio.create_task(_broadcast_loop())
        try:
            yield
        finally:
            task.cancel()
            try:
                await task
            except (asyncio.CancelledError, Exception):
                pass

    app = FastAPI(title="BaQueue Dashboard", version="0.2.0", lifespan=lifespan)

    # ── Static files ────────────────────────────────────────────

    @app.get("/")
    async def index():
        return FileResponse(STATIC_DIR / "index.html")

    @app.get("/app.js")
    async def app_js():
        return FileResponse(STATIC_DIR / "app.js", media_type="application/javascript")

    @app.get("/style.css")
    async def app_css():
        return FileResponse(STATIC_DIR / "style.css", media_type="text/css")

    # ── REST API ────────────────────────────────────────────────

    @app.get("/api/overview")
    async def get_overview():
        data = await api.overview()
        return JSONResponse(data)

    @app.get("/api/stats")
    async def get_stats(
        created_from: Optional[float] = Query(None),
        created_to: Optional[float] = Query(None),
    ):
        data = await api.stats(created_from=created_from, created_to=created_to)
        return JSONResponse(data)

    @app.get("/api/queues")
    async def get_queues():
        queues = await driver.queues()
        return JSONResponse({"queues": queues})

    @app.get("/api/queues/{queue}")
    async def get_queue(queue: str):
        data = await api.queue_detail(queue)
        return JSONResponse(data)

    @app.get("/api/jobs")
    async def get_jobs(
        queue: Optional[str] = Query(None),
        status: Optional[str] = Query(None),
        tag: Optional[str] = Query(None),
        batch_id: Optional[str] = Query(None),
        page: int = Query(1),
        per_page: int = Query(25),
        created_from: Optional[float] = Query(None),
        created_to: Optional[float] = Query(None),
    ):
        data = await api.jobs_list(
            queue=queue, status=status, tag=tag,
            batch_id=batch_id, page=page, per_page=per_page,
            created_from=created_from, created_to=created_to,
        )
        return JSONResponse(data)

    @app.get("/api/jobs/{job_id}")
    async def get_job(job_id: str):
        data = await api.job_detail(job_id)
        if data is None:
            return JSONResponse({"error": "Job not found"}, status_code=404)
        return JSONResponse(data)

    @app.post("/api/jobs/retry-failed")
    async def retry_failed(
        queue: Optional[str] = Query(None),
        tag: Optional[str] = Query(None),
        created_from: Optional[float] = Query(None),
        created_to: Optional[float] = Query(None),
    ):
        count = await api.retry_failed_jobs(
            queue=queue, tag=tag,
            created_from=created_from, created_to=created_to,
        )
        return JSONResponse({"retried": count})

    @app.post("/api/jobs/{job_id}/retry")
    async def retry_job(job_id: str):
        ok = await api.retry_job(job_id)
        return JSONResponse({"success": ok})

    @app.delete("/api/jobs/{job_id}")
    async def delete_job(job_id: str):
        ok = await api.delete_job(job_id)
        return JSONResponse({"success": ok})

    @app.get("/api/batches/{batch_id}")
    async def get_batch(batch_id: str):
        data = await api.batch_detail(batch_id)
        if data is None:
            return JSONResponse({"error": "Batch not found"}, status_code=404)
        return JSONResponse(data)

    @app.post("/api/prune")
    async def prune_jobs(
        status: Optional[str] = Query(None),
        tag: Optional[str] = Query(None),
        hours: Optional[float] = Query(None),
    ):
        count = await api.prune_jobs(status=status, tag=tag, hours=hours)
        return JSONResponse({"pruned": count})

    @app.get("/api/metrics")
    async def get_metrics():
        data = await api.metrics_snapshot()
        return JSONResponse(data)

    @app.get("/api/supervisors")
    async def get_supervisors():
        data = await api.supervisors_snapshot()
        return JSONResponse({"supervisors": data})

    # ── WebSocket ───────────────────────────────────────────────

    @app.websocket("/ws")
    async def websocket_endpoint(ws: WebSocket):
        await ws.accept()
        connected_ws.append(ws)
        try:
            data = await api.overview()
            await ws.send_json(data)
            while True:
                # Keep the connection alive; the broadcaster pushes updates.
                await ws.receive_text()
        except WebSocketDisconnect:
            pass
        except Exception:
            pass
        finally:
            if ws in connected_ws:
                connected_ws.remove(ws)

    @app.websocket("/ws/jobs")
    async def jobs_websocket(ws: WebSocket):
        """Per-connection jobs feed. Subscriber sends a {"type":"subscribe","filters":{...}}
        message; server pushes filtered jobs_list snapshots on a tick until disconnect."""
        await ws.accept()
        filters: dict[str, Any] = {
            "queue": None, "status": None, "tag": None, "batch_id": None,
            "page": 1, "per_page": 25,
            "created_from": None, "created_to": None,
        }
        push_lock = asyncio.Lock()

        async def push_snapshot() -> bool:
            async with push_lock:
                try:
                    data = await api.jobs_list(**filters)
                    await ws.send_json(data)
                    return True
                except WebSocketDisconnect:
                    return False
                except Exception:
                    logger.exception("ws/jobs push failed")
                    return True

        async def push_loop() -> None:
            while True:
                await asyncio.sleep(JOBS_PUSH_INTERVAL)
                if not await push_snapshot():
                    return

        push_task = asyncio.create_task(push_loop())
        try:
            await push_snapshot()
            while True:
                msg = await ws.receive_json()
                if msg.get("type") == "subscribe":
                    incoming = msg.get("filters") or {}
                    for k in filters:
                        if k not in incoming:
                            continue
                        v = incoming[k]
                        if k in ("page", "per_page"):
                            filters[k] = int(v) if v is not None else filters[k]
                        elif k in ("created_from", "created_to"):
                            filters[k] = float(v) if v not in (None, "") else None
                        else:
                            filters[k] = v if v not in (None, "") else None
                    await push_snapshot()
        except WebSocketDisconnect:
            pass
        except Exception:
            pass
        finally:
            push_task.cancel()
            try:
                await push_task
            except (asyncio.CancelledError, Exception):
                pass

    return app
