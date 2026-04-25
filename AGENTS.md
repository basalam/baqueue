# AGENTS.md

Guidance for AI coding agents working in this repository.

## Project Overview

**BaQueue** is an async Python queue-management package inspired by Laravel Horizon. It provides job dispatching, retries, scheduling, batching, auto-scaling workers, pruning, and a real-time monitoring dashboard.

- **Language**: Python `>= 3.10` (async/await throughout)
- **Package name**: `baqueue` (defined in `pyproject.toml`)
- **Console script**: `baqueue` → `baqueue.cli:cli`
- **License**: MIT

## Repository Layout

```
baqueue/
├── __init__.py           # Public exports: BaQueueConfig, Job, Queue, Batch, EventBus, BackoffStrategy
├── config.py             # Pydantic models: BaQueueConfig, DriverConfig, SupervisorConfig, ScheduleEntry
├── job.py                # Job base class + @Job.as_job decorator + FunctionJob wrapper
├── queue.py              # Queue singleton facade: configure / connect / push / later / bulk / prune / metrics
├── batch.py              # Batch: fluent builder with .name/.then/.catch/.on_finally/.allow_failures/.tag/.dispatch
├── worker.py             # Worker loop: pop → handle → complete/fail/release with retry
├── supervisor.py         # Worker pool, graceful shutdown, delayed-job poller, balancing loop
├── scheduler.py          # Cron + interval scheduler that pushes ScheduleEntry jobs
├── pruner.py             # Periodic pruning by status/age (uses prune_*_hours config)
├── balancer.py           # AutoBalancer / SimpleBalancer / NullBalancer + create_balancer()
├── retry.py              # BackoffStrategy enum + compute_delay + should_retry
├── events.py             # EventBus singleton: on / off / emit / emit_nowait
├── serializer.py         # JobPayload (slotted), resolve_job_class, get_class_path, _now_ts
├── cli.py                # Click commands: work, schedule, dashboard, prune, status
├── drivers/
│   ├── base.py           # BaseDriver ABC — implement these methods for any new driver
│   ├── memory_driver.py  # In-process only; never share across processes
│   ├── sqlite_driver.py  # Default; cross-process via WAL; file = .baqueue.db
│   ├── redis_driver.py   # Optional extra: pip install baqueue[redis]
│   └── postgres_driver.py# Optional extra: pip install baqueue[postgres]
├── dashboard/
│   ├── api.py            # DashboardAPI: overview/stats/jobs_list/job_detail/retry/delete/prune/...
│   ├── server.py         # FastAPI app factory (create_app) + REST routes + /ws
│   └── static/           # index.html, app.js, style.css (packaged as package_data)
└── screen/               # README screenshots (1.png, 2.png, 3.png) — do not edit
examples/                 # Runnable demos: simple_job, batch_example, scheduled_example,
                          # dashboard_demo, stress_test
pyproject.toml            # Build config, deps, optional extras [redis|postgres|dashboard|all|dev]
README.md                 # User-facing docs and benchmarks
```

## Setup & Common Commands

```bash
# Install editable with everything (recommended for dev)
pip install -e ".[all,dev]"

# Run examples (SQLite is default — no extra services needed)
python examples/simple_job.py
python examples/batch_example.py
python examples/scheduled_example.py
python examples/dashboard_demo.py
python examples/stress_test.py --jobs 1000 --workers 5 --bulk

# CLI — drivers default to sqlite
baqueue work -q emails -q payments -w 3 -b auto
baqueue schedule
baqueue dashboard            # http://localhost:9100
baqueue prune --status completed --hours 24
baqueue status

# Lint (already used in repo history per "ruff check" commit)
ruff check .
```

There is no test suite committed yet; `pytest` and `pytest-asyncio` are listed under `[project.optional-dependencies].dev` for when tests are added.

## Architectural Invariants

These constraints exist for real reasons — preserve them when refactoring.

1. **Async-only API surface.** Every driver method, `Queue.*`, `Worker`, `Supervisor`, `Scheduler`, `Pruner`, `Batch.dispatch`, and dashboard handlers are coroutines. Do not introduce blocking I/O in these paths.
2. **`JobPayload` uses `__slots__`.** When adding a new field, update **all** of: the slot tuple, `__init__` params, `to_dict`, and any driver-side row mapping (`sqlite_driver`, `redis_driver`, `postgres_driver`, `memory_driver`). Missing one will silently drop the field.
3. **`Queue` is a classmethod singleton.** State lives on the class (`_driver`, `_config`, `_events`). `Queue.reset()` is the test-only escape hatch. `Queue.configure()` must be called before `connect()`.
4. **`baqueue.dashboard.server` deliberately avoids `from __future__ import annotations`.** FastAPI relies on runtime type inspection for WebSocket parameter resolution; PEP 563 deferred annotations break it. The comment at the top of `server.py` documents this — leave it.
5. **`BaseDriver` is the contract.** Adding a feature usually means adding an abstract method here and implementing it in **all four** drivers (memory, sqlite, redis, postgres). `count_jobs` has a default fallback; the rest are abstract.
6. **Driver factory lives in `queue.py::_create_driver`.** Redis and Postgres are imported lazily inside the branch so the package works without their optional extras installed. Preserve the `try/except ImportError → friendly install hint` pattern for any new optional driver.
7. **Memory driver is single-process only.** The CLI prints a warning when `dashboard` is used with `memory`. Don't claim cross-process behaviour for it.
8. **SQLite driver is the default cross-process option.** It uses WAL + a busy_timeout + `_execute_with_retry` for `database is locked`. Don't switch journal modes or remove the retry wrapper without strong justification.
9. **Backoff strategies.** `compute_delay` accepts a strategy name (`"fixed"`, `"linear"`, `"exponential"`) **or** an explicit `list[int]` of per-attempt delays. Both forms must keep working. Exponential adds jitter (≤10%) and clamps to `max_delay`.
10. **Event names are part of the public contract.** Documented in `EventBus`'s docstring: `job.pushed`, `job.started`, `job.completed`, `job.failed`, `job.retrying`, `batch.completed`, `batch.failed`, `worker.started`, `worker.stopped`, `supervisor.started`, `supervisor.stopped`, `queue.pruned`, `schedule.dispatched`, `batch.dispatched`. Don't rename them silently.
11. **Job class resolution is by dotted path.** `serializer.resolve_job_class` does `importlib.import_module` + `getattr`. Jobs must therefore be importable by their qualified name from the worker process. Lambdas / nested classes won't work.
12. **Decorator-defined jobs (`@Job.as_job`) become `FunctionJob` instances**, not classes. `Worker._instantiate` and `Queue._build_payload` both special-case this — keep both branches if you change the dispatch flow.

## Adding a New Driver

1. Subclass `BaseDriver` in `baqueue/drivers/<name>_driver.py` and implement every abstract method.
2. Add a branch in `baqueue/queue.py::_create_driver` with a lazy import + ImportError hint.
3. Add it to `VALID_DRIVERS` in `baqueue/cli.py`.
4. Add an optional-deps entry in `pyproject.toml` (`[project.optional-dependencies].<name>`) and include it in `all`.
5. Mirror metric semantics: `pending`, `processing`, `completed`, `failed`, `total_jobs` per queue (see `MemoryDriver.get_metrics`).

## Adding a New CLI Command

Extend `baqueue/cli.py`:
- Add a new `@cli.command()` function decorated with `click` options.
- Always call `_validate_driver(driver)` before doing work.
- Wrap async logic in a separate `async def _run_<name>(config, ...)` and dispatch via `_run_async(...)` so errors and `KeyboardInterrupt` are formatted consistently.
- Driver flags (`--driver/-d`, `--driver-url`) should default to `"sqlite"` and `None` respectively for parity with the existing commands.

## Code Style

- `from __future__ import annotations` everywhere **except** `dashboard/server.py` (see invariant #4).
- Public APIs use type hints (PEP 604 unions: `str | None`).
- Pydantic v2 models for config (`BaseModel`, `Field(default_factory=...)`).
- Logging: `logger = logging.getLogger("baqueue.<module>")`. Don't `print` from library code (CLI/examples may print).
- Docstrings: short module + class docstrings with usage examples in `Usage::` blocks (see `Job`, `Queue`, `Batch`, `Scheduler`).
- Don't add comments that restate what the code does. Keep the existing terse style.
- File encodings, paths: prefer `pathlib.Path`. The dashboard locates static assets via `Path(__file__).parent / "static"`.
- The repo runs `ruff check`; keep it clean.

## Things to Watch Out For

- **Windows signal handling.** `Supervisor._setup_signal_handlers` early-returns on `os.name == "nt"` because `loop.add_signal_handler` is not supported. Don't remove this guard.
- **`emit_nowait` swallows `RuntimeError`** when there's no running loop — that's intentional for sync callers (e.g. `Queue.push` from non-async contexts). Don't "fix" it by raising.
- **Batch callbacks are wired on `dispatch()`** via `self._events.on(...)`. They check `batch_id` to filter. Multiple concurrent batches all listen on the same global event bus — the filter is what isolates them.
- **`Queue.bulk` is much faster than looping `Queue.push`** because it calls `driver.push_many` once. The stress test relies on this for the ~28k–50k jobs/s dispatch rate quoted in the README.
- **Dashboard WebSocket pushes overview every 2s** (`server.py::websocket_endpoint`). If you change the cadence, update the reconnect logic in `dashboard/static/app.js` to match.
- **`.baqueue.db`, `.baqueue.db-wal`, `.baqueue.db-shm`** are gitignored. Don't commit them — they appear whenever you run an example.
- **`baqueue/screen/*.png`** are referenced from `README.md`. Don't move or rename them.

## When Making Changes

- **Edit existing files** rather than creating new ones unless a new module is genuinely needed.
- **Don't add backwards-compatibility shims** for unreleased changes — this is `0.1.0`/Alpha (see `pyproject.toml` classifiers).
- **Don't add a feature flag** when you can just change the code.
- **Run a relevant example** (`python examples/simple_job.py` or `python examples/stress_test.py --jobs 200 --workers 3 --bulk`) to sanity-check changes that touch the worker/driver/supervisor path. The SQLite driver needs no setup.
- **For dashboard changes**, run `baqueue dashboard` in one terminal and `python examples/simple_job.py` in another, then verify in a browser at `http://localhost:9100`.
