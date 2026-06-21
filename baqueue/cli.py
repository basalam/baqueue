"""CLI commands for BaQueue."""

from __future__ import annotations

import asyncio
import json
import logging
import sys
from pathlib import Path
from typing import Any

import click

from baqueue.config import BaQueueConfig, DriverConfig, SupervisorConfig
from baqueue.queue import Queue


def _load_config(config_path: str | None) -> BaQueueConfig:
    if config_path and Path(config_path).exists():
        raw = json.loads(Path(config_path).read_text())
        return BaQueueConfig.from_dict(raw)
    return BaQueueConfig()


def _setup_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )


VALID_DRIVERS = ("sqlite", "memory", "redis", "postgres")


def _validate_driver(name: str) -> str:
    if name not in VALID_DRIVERS:
        raise click.BadParameter(
            f"Unknown driver '{name}'. Choose from: {', '.join(VALID_DRIVERS)}",
            param_hint="'--driver / -d'",
        )
    return name


def _run_async(coro_fn, *args: Any, **kwargs: Any) -> Any:
    """Run an async function with clean error reporting."""
    try:
        return asyncio.run(coro_fn(*args, **kwargs))
    except KeyboardInterrupt:
        click.echo("\nShutting down...")
    except ImportError as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)
    except ConnectionError as e:
        click.echo(f"Error: Connection failed - {e}", err=True)
        sys.exit(1)
    except OSError as e:
        if "10048" in str(e) or "address already in use" in str(e).lower():
            click.echo("Error: Port is already in use. Kill the existing process or use a different port.", err=True)
        else:
            click.echo(f"Error: {e}", err=True)
        sys.exit(1)
    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


@click.group(context_settings={"help_option_names": ["-h", "--help"]})
@click.option("--config", "-c", default=None, help="Path to baqueue config JSON file.")
@click.option("--verbose", "-v", is_flag=True, help="Enable debug logging.")
@click.pass_context
def cli(ctx: click.Context, config: str | None, verbose: bool) -> None:
    """BaQueue - Python Queue Management."""
    _setup_logging(verbose)
    ctx.ensure_object(dict)
    ctx.obj["config"] = _load_config(config)


@cli.command()
@click.option("--queue", "-q", multiple=True, default=["default"], help="Queues to process.")
@click.option("--workers", "-w", default=3, help="Number of workers.")
@click.option("--balance", "-b", default="auto", help="Balancing strategy: auto, simple, false.")
@click.option("--sleep", "-s", default=1.0, help="Sleep interval when idle (seconds).")
@click.option("--timeout", "-t", default=60, help="Max job execution time (seconds).")
@click.option("--max-jobs", default=0, help="Max jobs per worker (0 = unlimited).")
@click.option("--driver", "-d", default="sqlite", help="Driver name (sqlite, memory, redis, postgres).")
@click.option("--driver-url", default=None, help="Driver connection URL.")
@click.option("--no-auto-prune", is_flag=True, help="Disable the background auto-pruner.")
@click.option(
    "--prune-completed-seconds", type=int, default=None,
    help="Override: delete completed jobs older than N seconds (default 5).",
)
@click.option(
    "--prune-other-seconds", type=int, default=None,
    help="Override: delete failed/cancelled jobs older than N seconds (default 86400).",
)
@click.option(
    "--prune-interval-seconds", type=int, default=None,
    help="Override: how often the auto-pruner runs, in seconds (default 5).",
)
@click.option(
    "--no-disk-full-cleanup", is_flag=True,
    help="Disable automatic emergency cleanup when the driver returns a disk-full error.",
)
@click.pass_context
def work(
    ctx: click.Context,
    queue: tuple[str, ...],
    workers: int,
    balance: str,
    sleep: float,
    timeout: int,
    max_jobs: int,
    driver: str,
    driver_url: str | None,
    no_auto_prune: bool,
    prune_completed_seconds: int | None,
    prune_other_seconds: int | None,
    prune_interval_seconds: int | None,
    no_disk_full_cleanup: bool,
) -> None:
    """Start processing jobs."""
    config: BaQueueConfig = ctx.obj["config"]
    config.driver = DriverConfig(name=driver, url=driver_url or "")

    if no_auto_prune:
        config.auto_prune = False
    if prune_completed_seconds is not None:
        config.prune_completed_seconds = prune_completed_seconds
    if prune_other_seconds is not None:
        config.prune_other_seconds = prune_other_seconds
    if prune_interval_seconds is not None:
        config.prune_interval_seconds = prune_interval_seconds
    if no_disk_full_cleanup:
        config.auto_cleanup_on_disk_full = False

    supervisor_config = SupervisorConfig(
        queues=list(queue),
        min_workers=workers,
        max_workers=workers * 2,
        balance=balance,
        sleep=sleep,
        timeout=timeout,
        max_jobs_per_worker=max_jobs,
    )

    _validate_driver(driver)

    click.echo("BaQueue worker starting...")
    click.echo(f"  Driver:  {config.driver.name}")
    click.echo(f"  Queues:  {', '.join(queue)}")
    click.echo(f"  Workers: {workers}")
    click.echo(f"  Balance: {balance}")
    if config.auto_prune:
        click.echo(
            f"  Auto-prune: completed>{config.prune_completed_seconds}s, "
            f"other>{config.prune_other_seconds}s, every {config.prune_interval_seconds}s"
        )
    else:
        click.echo("  Auto-prune: disabled")
    click.echo(
        f"  Disk-full cleanup: {'enabled' if config.auto_cleanup_on_disk_full else 'disabled'}"
    )
    click.echo()

    _run_async(_run_worker, config, supervisor_config)


async def _run_worker(config: BaQueueConfig, supervisor_config: SupervisorConfig) -> None:
    from baqueue.balancer import create_balancer
    from baqueue.pruner import Pruner
    from baqueue.supervisor import Supervisor

    Queue.configure(config)
    await Queue.connect()

    balancer = create_balancer(
        supervisor_config.balance,
        min_workers=supervisor_config.min_workers,
        max_workers=supervisor_config.max_workers,
    )
    pruner = Pruner(driver=Queue.get_driver(), config=config) if config.auto_prune else None
    supervisor = Supervisor(
        driver=Queue.get_driver(),
        config=supervisor_config,
        balancer=balancer,
        pruner=pruner,
    )

    try:
        await supervisor.start()
    finally:
        await supervisor.stop()
        await Queue.disconnect()


@cli.command()
@click.option("--driver", "-d", default="sqlite", help="Driver name (sqlite, memory, redis, postgres).")
@click.option("--driver-url", default=None, help="Driver connection URL.")
@click.pass_context
def schedule(ctx: click.Context, driver: str, driver_url: str | None) -> None:
    """Start the job scheduler."""
    _validate_driver(driver)
    config: BaQueueConfig = ctx.obj["config"]
    config.driver = DriverConfig(name=driver, url=driver_url or "")

    if not config.schedules:
        click.echo("No schedules configured. Add entries to your config file.")
        return

    click.echo(f"Scheduler starting with {len(config.schedules)} entries...")
    _run_async(_run_scheduler, config)


async def _run_scheduler(config: BaQueueConfig) -> None:
    from baqueue.scheduler import Scheduler

    Queue.configure(config)
    await Queue.connect()

    scheduler = Scheduler(driver=Queue.get_driver(), entries=config.schedules)
    try:
        await scheduler.start()
    finally:
        scheduler.stop()
        await Queue.disconnect()


@cli.command()
@click.option("--host", "-H", default="0.0.0.0", help="Dashboard host.")
@click.option("--port", "-p", default=9100, help="Dashboard port.")
@click.option("--driver", "-d", default="sqlite", help="Driver name (sqlite, memory, redis, postgres).")
@click.option("--driver-url", default=None, help="Driver connection URL.")
@click.pass_context
def dashboard(ctx: click.Context, host: str, port: int, driver: str, driver_url: str | None) -> None:
    """Launch the monitoring dashboard."""
    _validate_driver(driver)
    config: BaQueueConfig = ctx.obj["config"]
    config.driver = DriverConfig(name=driver, url=driver_url or "")
    config.dashboard_host = host
    config.dashboard_port = port

    if config.driver.name == "memory":
        click.echo(
            "WARNING: Memory driver only works within a single process.\n"
            "  The dashboard will NOT see jobs from other processes.\n"
            "  Use 'sqlite' driver instead, or run 'python examples/dashboard_demo.py'.\n"
        )
    click.echo(f"BaQueue Dashboard: http://{host}:{port}")
    click.echo(f"  Driver: {config.driver.name}")
    _run_async(_run_dashboard, config)


async def _run_dashboard(config: BaQueueConfig) -> None:
    from baqueue.dashboard.server import create_app
    import uvicorn

    Queue.configure(config)
    await Queue.connect()

    app = create_app(Queue.get_driver(), config)
    server_config = uvicorn.Config(app, host=config.dashboard_host, port=config.dashboard_port, log_level="info")
    server = uvicorn.Server(server_config)
    await server.serve()


@cli.command()
@click.option("--status", "-s", default=None, help="Prune jobs by status (completed, failed, cancelled).")
@click.option("--tag", "-t", default=None, help="Prune jobs by tag.")
@click.option("--hours", default=None, type=float, help="Prune jobs older than N hours.")
@click.option("--queue", "-q", default=None, help="Limit to a specific queue.")
@click.option("--driver", "-d", default="sqlite", help="Driver name (sqlite, memory, redis, postgres).")
@click.option("--driver-url", default=None, help="Driver connection URL.")
@click.pass_context
def prune(
    ctx: click.Context,
    status: str | None,
    tag: str | None,
    hours: float | None,
    queue: str | None,
    driver: str,
    driver_url: str | None,
) -> None:
    """Prune old jobs from the queue."""
    if not status and not tag:
        click.echo("Please specify --status or --tag to prune.")
        return

    _validate_driver(driver)
    config: BaQueueConfig = ctx.obj["config"]
    config.driver = DriverConfig(name=driver, url=driver_url or "")

    count = _run_async(_run_prune, config, status, tag, hours, queue)
    click.echo(f"Pruned {count or 0} jobs.")


async def _run_prune(
    config: BaQueueConfig,
    status: str | None,
    tag: str | None,
    hours: float | None,
    queue: str | None,
) -> int:
    Queue.configure(config)
    await Queue.connect()
    try:
        return await Queue.prune(status=status, tag=tag, hours=hours, queue=queue)
    finally:
        await Queue.disconnect()


@cli.command(name="retry-failed")
@click.option("--queue", "-q", default=None, help="Limit to a specific queue.")
@click.option("--tag", "-t", default=None, help="Limit to a specific tag.")
@click.option("--hours", default=None, type=float, help="Only retry jobs created within the last N hours.")
@click.option("--driver", "-d", default="sqlite", help="Driver name (sqlite, memory, redis, postgres).")
@click.option("--driver-url", default=None, help="Driver connection URL.")
@click.option("--yes", "-y", is_flag=True, help="Skip confirmation prompt.")
@click.pass_context
def retry_failed(
    ctx: click.Context,
    queue: str | None,
    tag: str | None,
    hours: float | None,
    driver: str,
    driver_url: str | None,
    yes: bool,
) -> None:
    """Retry all failed jobs (optionally filtered by queue, tag, or age)."""
    _validate_driver(driver)
    config: BaQueueConfig = ctx.obj["config"]
    config.driver = DriverConfig(name=driver, url=driver_url or "")

    scope_parts = []
    if queue:
        scope_parts.append(f"queue={queue}")
    if tag:
        scope_parts.append(f"tag={tag}")
    if hours:
        scope_parts.append(f"created within last {hours}h")
    scope_str = f" ({', '.join(scope_parts)})" if scope_parts else ""

    if not yes:
        click.confirm(f"Retry all failed jobs{scope_str}?", abort=True)

    count = _run_async(_run_retry_failed, config, queue, tag, hours)
    click.echo(f"Retried {count or 0} failed job(s).")


async def _run_retry_failed(
    config: BaQueueConfig,
    queue: str | None,
    tag: str | None,
    hours: float | None,
) -> int:
    from baqueue.serializer import _now_ts

    Queue.configure(config)
    await Queue.connect()
    try:
        created_from = (_now_ts() - hours * 3600) if hours else None
        return await Queue.retry_failed(queue=queue, tag=tag, created_from=created_from)
    finally:
        await Queue.disconnect()


@cli.command(name="reconcile-indexes")
@click.option("--batch", default=500, type=int, help="Index entries scanned per batch.")
@click.option("--driver", "-d", default="redis", help="Driver name (sqlite, memory, redis, postgres).")
@click.option("--driver-url", default=None, help="Driver connection URL.")
@click.pass_context
def reconcile_indexes(
    ctx: click.Context,
    batch: int,
    driver: str,
    driver_url: str | None,
) -> None:
    """Repair secondary indexes: remove entries pointing at jobs that no longer exist.

    Only the Redis driver maintains secondary indexes; this is a no-op elsewhere."""
    _validate_driver(driver)
    config: BaQueueConfig = ctx.obj["config"]
    config.driver = DriverConfig(name=driver, url=driver_url or "")

    removed = _run_async(_run_reconcile_indexes, config, batch)
    click.echo(f"Removed {removed or 0} stale index entr{'y' if removed == 1 else 'ies'}.")


async def _run_reconcile_indexes(config: BaQueueConfig, batch: int) -> int:
    Queue.configure(config)
    await Queue.connect()
    try:
        return await Queue.get_driver().reconcile_indexes(batch=batch)
    finally:
        await Queue.disconnect()


@cli.command()
@click.option("--driver", "-d", default="sqlite", help="Driver name (sqlite, memory, redis, postgres).")
@click.option("--driver-url", default=None, help="Driver connection URL.")
@click.pass_context
def status(ctx: click.Context, driver: str, driver_url: str | None) -> None:
    """Show queue status and metrics."""
    _validate_driver(driver)
    config: BaQueueConfig = ctx.obj["config"]
    config.driver = DriverConfig(name=driver, url=driver_url or "")

    _run_async(_show_status, config)


async def _show_status(config: BaQueueConfig) -> None:
    Queue.configure(config)
    await Queue.connect()

    try:
        queues = await Queue.queues()
        if not queues:
            click.echo("No queues found.")
            return

        metrics = await Queue.metrics()
        click.echo(f"{'Queue':<20} {'Pending':<10} {'Processing':<12} {'Completed':<12} {'Failed':<10}")
        click.echo("-" * 64)
        for q in queues:
            m = metrics.get(q, {})
            click.echo(
                f"{q:<20} {m.get('pending', 0):<10} {m.get('processing', 0):<12} "
                f"{m.get('completed', 0):<12} {m.get('failed', 0):<10}"
            )
    finally:
        await Queue.disconnect()


@cli.command()
@click.option("--match", "-k", default=None, help="Run only tests whose name matches this expression.")
@click.option("--marker", "-m", default=None, help="Run only tests with the given pytest marker (e.g. 'not slow').")
@click.option("--path", "-p", default="tests", help="Test path or file (defaults to the tests/ folder).")
@click.option("--verbose", "-v", is_flag=True, help="Enable verbose pytest output.")
@click.option("--quiet", "-q", is_flag=True, help="Quieter pytest output.")
@click.option("--stop-on-first-failure", "-x", is_flag=True, help="Stop at the first failure.")
@click.option("--last-failed", is_flag=True, help="Re-run only tests that failed in the previous run.")
@click.option(
    "--no-header", is_flag=True, default=False,
    help="Suppress pytest's session header banner.",
)
def test(
    match: str | None,
    marker: str | None,
    path: str,
    verbose: bool,
    quiet: bool,
    stop_on_first_failure: bool,
    last_failed: bool,
    no_header: bool,
) -> None:
    """Run the BaQueue test suite (pytest under the hood)."""
    try:
        import pytest as _pytest
    except ImportError:
        click.echo(
            "Error: pytest is not installed.\n"
            "  Install it with: pip install baqueue[dev]\n"
            "  Or directly:    pip install pytest pytest-asyncio",
            err=True,
        )
        sys.exit(1)

    args: list[str] = [path]
    if verbose:
        args.append("-v")
    if quiet:
        args.append("-q")
    if stop_on_first_failure:
        args.append("-x")
    if last_failed:
        args.append("--lf")
    if match:
        args.extend(["-k", match])
    if marker:
        args.extend(["-m", marker])
    if no_header:
        args.append("--no-header")

    exit_code = _pytest.main(args)
    sys.exit(int(exit_code))


if __name__ == "__main__":
    cli()
