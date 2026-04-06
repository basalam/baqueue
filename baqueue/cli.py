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
    except ConnectionError as e:
        click.echo(f"Error: Connection failed - {e}", err=True)
        sys.exit(1)
    except OSError as e:
        if "10048" in str(e) or "address already in use" in str(e).lower():
            click.echo(f"Error: Port is already in use. Kill the existing process or use a different port.", err=True)
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
) -> None:
    """Start processing jobs."""
    config: BaQueueConfig = ctx.obj["config"]
    config.driver = DriverConfig(name=driver, url=driver_url or "")

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

    click.echo(f"BaQueue worker starting...")
    click.echo(f"  Driver:  {config.driver.name}")
    click.echo(f"  Queues:  {', '.join(queue)}")
    click.echo(f"  Workers: {workers}")
    click.echo(f"  Balance: {balance}")
    click.echo()

    _run_async(_run_worker, config, supervisor_config)


async def _run_worker(config: BaQueueConfig, supervisor_config: SupervisorConfig) -> None:
    from baqueue.balancer import create_balancer
    from baqueue.supervisor import Supervisor

    Queue.configure(config)
    await Queue.connect()

    balancer = create_balancer(
        supervisor_config.balance,
        min_workers=supervisor_config.min_workers,
        max_workers=supervisor_config.max_workers,
    )
    supervisor = Supervisor(
        driver=Queue.get_driver(),
        config=supervisor_config,
        balancer=balancer,
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


if __name__ == "__main__":
    cli()
