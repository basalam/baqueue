# BaQueue

[![PyPI version](https://img.shields.io/pypi/v/baqueue.svg)](https://pypi.org/project/baqueue/)
[![Python versions](https://img.shields.io/pypi/pyversions/baqueue.svg)](https://pypi.org/project/baqueue/)
[![License: MIT](https://img.shields.io/pypi/l/baqueue.svg)](https://github.com/basalam/baqueue/blob/master/LICENSE)
[![CI](https://github.com/basalam/baqueue/actions/workflows/ci.yml/badge.svg)](https://github.com/basalam/baqueue/actions/workflows/ci.yml)

A powerful Python queue management package. Multi-driver support, batch jobs, scheduling, auto-balancing, and a beautiful real-time monitoring dashboard.

![Overview](./baqueue/screen/1.png)

### Jobs - Filterable job list with status, attempts, and duration
![Jobs](./baqueue/screen/2.png)

### Queues - Per-queue detail cards with pending/processing/completed/failed
![Queues](./baqueue/screen/3.png)

## Table of Contents

- [Features](#features)
- [Quick Start](#quick-start)
  - [Define a Job](#define-a-job)
  - [Dispatch Jobs](#dispatch-jobs)
  - [Batch Jobs](#batch-jobs)
  - [Run Workers](#run-workers)
  - [Pruning](#pruning)
    - [Auto-pruning](#auto-pruning-runs-alongside-baqueue-work)
    - [Manual pruning](#manual-pruning)
  - [Retry Failed Jobs](#retry-failed-jobs)
  - [Dashboard](#dashboard)
    - [Workers Tab (Supervisor/Worker Monitoring)](#workers-tab-supervisorworker-monitoring)
  - [Drivers](#drivers)
- [Examples](#examples)
- [Testing](#testing)
- [CLI Commands](#cli-commands)
- [Benchmarks](#benchmarks)
  - [Test 1: 1,000 jobs / 5 workers](#test-1-1000-jobs--5-workers)
  - [Test 2: 5,000 jobs / 10 workers](#test-2-5000-jobs--10-workers)
  - [Stress Test Options](#stress-test-options)
  - [Run with Live Dashboard](#run-with-live-dashboard)
- [License](#license)

## Features
- **Multi-driver**: SQLite (default), Redis, PostgreSQL, or In-Memory
- **Auto-balancing**: Dynamically scale workers based on queue pressure
- **Auto-pruning**: Completed jobs are deleted about 5 seconds after they finish; failed/cancelled jobs are kept up to 1 day — all configurable
- **Disk-full cleanup**: Storage-full/OOM driver errors trigger emergency cleanup of terminal jobs and old metrics, then retry once
- **Pruning**: Remove old jobs by status, tag, or age
- **Monitoring Dashboard**: Real-time WebSocket-powered UI with date filtering
- **CLI**: Manage workers, scheduler, dashboard, and pruning from the command line
- **Cross-process**: SQLite driver shares state between dashboard and workers without external dependencies

## Quick Start

```bash
# Install (SQLite + in-memory drivers work out of the box, zero extra dependencies)
pip install baqueue

# With Redis support
pip install "baqueue[redis]"

# With PostgreSQL support
pip install "baqueue[postgres]"

# With dashboard
pip install "baqueue[dashboard]"

# Everything
pip install "baqueue[all]"
```

> **Installing from source (development):**
> ```bash
> git clone https://github.com/basalam/baqueue.git
> cd baqueue
> pip install -e ".[dev]"   # editable install with all extras + test/build tooling
> ```

### Define a Job

```python
from baqueue import Job

class SendEmail(Job):
    queue = "emails"
    max_attempts = 3
    backoff = "exponential"

    async def handle(self, to: str, subject: str, body: str):
        await send_email(to, subject, body)

    async def on_failure(self, error, payload):
        print(f"Failed to send email: {error}")
```

Or use the decorator:

```python
from baqueue import Job

@Job.as_job(queue="emails", max_attempts=3)
async def send_email(to, subject, body):
    ...
```

### Dispatch Jobs

```python
from baqueue import Queue, BaQueueConfig
from baqueue.config import DriverConfig

# Configure (SQLite driver by default - works across processes)
Queue.configure(BaQueueConfig(driver=DriverConfig(name="sqlite")))
await Queue.connect()

# Push a job
await Queue.push(SendEmail, to="user@example.com", subject="Hi", body="Hello!")

# Push with delay (60 seconds)
await Queue.later(SendEmail, delay=60, to="user@example.com", subject="Reminder", body="...")

# Bulk push (much faster for large volumes)
await Queue.bulk([
    (SendEmail, {"to": "a@b.com", "subject": "Hi", "body": "A"}),
    (SendEmail, {"to": "c@d.com", "subject": "Hi", "body": "B"}),
])
```

### Batch Jobs

```python
from baqueue import Batch

result = await Batch(driver, [
    (SendEmail, {"to": "a@b.com", "subject": "Hi", "body": "Hey"}),
    (SendEmail, {"to": "c@d.com", "subject": "Hi", "body": "Hey"}),
]).name("newsletter").then(OnAllDone).catch(OnAnyFailed).dispatch()
```

### Run Workers

```python
from baqueue.supervisor import Supervisor
from baqueue.config import SupervisorConfig

supervisor = Supervisor(
    driver=Queue.get_driver(),
    config=SupervisorConfig(
        queues=["emails", "payments"],
        min_workers=3,
        max_workers=10,
        balance="auto",
    ),
)
await supervisor.start()
```

Or via CLI:

```bash
baqueue work -q emails -q payments -w 3 -b auto
```

### Pruning

#### Auto-pruning (runs alongside `baqueue work`)

When `baqueue work` is running, a background pruner cycles every 5s and applies these defaults across every driver:

| Status              | Default lifetime | Config field                |
|---------------------|------------------|-----------------------------|
| `completed`         | 5 seconds        | `prune_completed_seconds`   |
| `failed`, `cancelled` | 1 day          | `prune_other_seconds`       |
| metrics rows        | 7 days           | `prune_metrics_seconds`     |
| pruner cycle        | every 5s         | `prune_interval_seconds`    |
| enable/disable      | `True`           | `auto_prune`                |

Override from a JSON config file (`baqueue -c config.json work`):

```json
{
  "auto_prune": true,
  "prune_completed_seconds": 600,
  "prune_other_seconds": 172800,
  "prune_interval_seconds": 30,
  "auto_cleanup_on_disk_full": true
}
```

Or from Python:

```python
from baqueue import BaQueueConfig
config = BaQueueConfig(
    prune_completed_seconds=600,    # 10 minutes
    prune_other_seconds=172800,     # 2 days
    prune_interval_seconds=30,
    auto_cleanup_on_disk_full=True, # enabled by default
)
```

Or from the CLI:

```bash
baqueue work --prune-completed-seconds 600 --prune-other-seconds 172800
baqueue work --no-auto-prune              # disable the background pruner
baqueue work --no-disk-full-cleanup       # disable emergency storage cleanup
```

The legacy hour-based fields (`prune_completed_hours`, `prune_failed_hours`, `prune_cancelled_hours`, `prune_metrics_hours`) are still respected for backward compatibility — when set to a positive value they override the corresponding `*_seconds` field.

#### Disk-full emergency cleanup

`auto_cleanup_on_disk_full` is enabled by default. When a driver write/update/delete operation sees a storage-full style error (SQLite disk full, PostgreSQL disk/memory exhausted, Redis OOM/maxmemory), BaQueue runs an emergency cleanup that removes terminal jobs (`completed`, `failed`, `cancelled`) and old metrics, then retries the failed operation once. If cleanup does not free enough space, the original driver error is still raised.

#### Manual pruning

```python
# Remove completed jobs older than 24 hours
await Queue.prune(status="completed", hours=24)

# Remove jobs by tag
await Queue.prune(tag="batch:newsletter")
```

### Retry Failed Jobs

Bulk-retry failed jobs from the CLI, from Python, or from the dashboard.

**CLI:**

```bash
# Retry every failed job (asks for confirmation)
baqueue retry-failed

# Skip the confirmation prompt
baqueue retry-failed -y

# Limit to a specific queue
baqueue retry-failed -q emails

# Combine filters: queue + tag + age window
baqueue retry-failed -q emails -t campaign --hours 24

# Use a non-default driver
baqueue retry-failed -d redis --driver-url redis://localhost:6379/0
```

**Python:**

```python
# Retry every failed job
count = await Queue.retry_failed()

# Retry only failed jobs in a queue
count = await Queue.retry_failed(queue="emails")

# Filter by tag and creation window
from baqueue.serializer import _now_ts
count = await Queue.retry_failed(
    queue="emails",
    tag="campaign",
    created_from=_now_ts() - 24 * 3600,
)
```

**Dashboard:** open the **Jobs** tab, set the Status filter to `Failed`, then click the amber **Retry All** button. The current Queue / Tag / date-range filters are respected.

Each matched job is released back onto its queue with `delay=0`, the same path used by single-job retry.

### Dashboard

```bash
# Start the dashboard (uses SQLite by default)
baqueue dashboard

# Open http://localhost:9100
```

The dashboard includes:
- Real-time overview with pending/processing/completed/failed counters sourced from live job state (not bounded metric logs)
- Date range filtering (custom range + presets: 1h, 24h, 7d, 30d)
- Job detail modal with timeline, payload data, and error trace
- Queue breakdown with progress bars
- Worker monitoring with active/idle status
- Dark/light theme toggle
- **Scheduled-job badge** with hover tooltip showing exact execution time, plus a "Scheduled For" entry in the job timeline
- **Bulk "Retry All"** button when the Jobs view is filtered to `failed` (respects the active queue/tag/date filters)
- **Queue filter as a dropdown** auto-populated from active queues (no manual typing)
- **Mobile-friendly** sidebar drawer with hamburger toggle on screens ≤900px

Run in one terminal:
```bash
baqueue dashboard
```

Dispatch jobs in another terminal:
```bash
python examples/simple_job.py
```

#### Workers Tab (Supervisor/Worker Monitoring)

To see active supervisors/workers in the `Workers` tab, `work` and `dashboard`
must point to the same backend (same driver and same URL/path).

Example with SQLite:

Terminal 1:
```bash
baqueue work -d sqlite --driver-url .baqueue.db -q default -w 3
```

Terminal 2:
```bash
baqueue dashboard -d sqlite --driver-url .baqueue.db
```

Then open:
```text
http://localhost:9100
```

Quick troubleshooting:
- Check `http://localhost:9100/api/supervisors` (should return a non-empty `supervisors` list while workers are running).
- If `api/supervisors` is empty, `work` and `dashboard` are likely using different driver URLs/paths.
- `memory` driver is single-process only, so separate `work` and `dashboard` processes will not share worker state.

Driver-specific CLI examples:

SQLite (shared local file):
```bash
baqueue work -d sqlite --driver-url .baqueue.db -q default -w 3
baqueue dashboard -d sqlite --driver-url .baqueue.db
```

Redis (shared Redis DB):
```bash
baqueue work -d redis --driver-url redis://localhost:6379/0 -q default -w 3
baqueue dashboard -d redis --driver-url redis://localhost:6379/0
```

PostgreSQL (shared database/schema):
```bash
baqueue work -d postgres --driver-url postgresql://user:pass@localhost/dbname -q default -w 3
baqueue dashboard -d postgres --driver-url postgresql://user:pass@localhost/dbname
```

Memory (single-process only):
```bash
# Use an in-process example to run workers + dashboard together.
python examples/dashboard_demo.py
```

### Drivers

**SQLite (default, zero-config, cross-process):**
```python
Queue.configure(BaQueueConfig(
    driver=DriverConfig(name="sqlite")
))
```

**Redis:**
```python
Queue.configure(BaQueueConfig(
    driver=DriverConfig(name="redis", url="redis://localhost:6379/0")
))
```

**PostgreSQL:**
```python
Queue.configure(BaQueueConfig(
    driver=DriverConfig(name="postgres", url="postgresql://user:pass@localhost/dbname")
))
```

**Memory (single-process testing only):**
```python
Queue.configure(BaQueueConfig(
    driver=DriverConfig(name="memory")
))
```

## Examples

```bash
# Simple job processing
python examples/simple_job.py

# Batch processing
python examples/batch_example.py

# Scheduled jobs
python examples/scheduled_example.py

# Dashboard demo (open http://localhost:9100)
python examples/dashboard_demo.py

# Delayed jobs demo — shows the "Scheduled" badge with varied delays
python examples/delayed_jobs_demo.py

# Stress test (see Benchmarks section below)
python examples/stress_test.py --jobs 1000 --workers 5 --bulk
```

## Testing

The full test suite lives in `tests/` and runs with one command:

```bash
# Run everything
baqueue test

# Quiet output, stop at the first failure
baqueue test -q -x

# Run only retry-failed related tests
baqueue test -k "RetryFailed or retry_failed"

# Re-run just the tests that failed last time
baqueue test --last-failed

# Filter by marker (markers defined in pyproject.toml)
baqueue test -m "not slow"
```

`baqueue test` is a thin wrapper around `pytest`, so it picks up the project's
`tool.pytest.ini_options` config (asyncio mode, marker definitions, etc.).
You can also run pytest directly:

```bash
pip install baqueue[dev]
pytest tests/ -v
```

Coverage includes:

- Serializer / payload roundtrip (incl. `delay_until`)
- Backoff strategies (`fixed`, `linear`, `exponential`, explicit list)
- `Job` + `FunctionJob` + `@Job.as_job` decorator
- `Queue` facade — push / later / bulk / prune / `retry_failed`
- Cross-driver contract tests (memory + sqlite, parameterized)
- Worker lifecycle: success / failure / retry / timeout
- Supervisor pool + delayed-job promotion
- Scheduler interval dispatch
- Pruner by status / tag / age
- Batch builder + completion callbacks
- DashboardAPI (overview, jobs_list, retry, bulk retry-failed, prune, stats)
- CLI command surface (help text, validation, `retry-failed` abort flow)

## CLI Commands

```
baqueue work          Start processing jobs
baqueue schedule      Start the job scheduler
baqueue dashboard     Launch the monitoring dashboard
baqueue prune         Prune old jobs
baqueue retry-failed  Retry all failed jobs (filter by queue/tag/age)
baqueue status        Show queue status
baqueue test          Run the test suite
```

Use `-h` on any command for options:
```bash
baqueue -h
baqueue work -h
baqueue dashboard -h
```

## Benchmarks

Stress tests run on **Windows 10, Python 3.11, SQLite driver**, using `examples/stress_test.py`.

The stress test dispatches jobs across 5 queues (`fast`, `slow`, `flaky`, `heavy`, `notifications`) with varying execution times and a ~30% failure rate on the `flaky` queue, exercising retries and backoff.

### Test 1: 1,000 jobs / 5 workers

```bash
python examples/stress_test.py --jobs 1000 --workers 5 --bulk
```

```
============================================================
  RESULTS
============================================================
  Total time:    30.38s
  Completed:     993
  Failed:        7
  Throughput:    32.9 jobs/s
  Success rate:  99.3%
============================================================
```

| Metric            | Value          |
|-------------------|----------------|
| Dispatch speed    | 28,426 jobs/s  |
| Processing speed  | 32.9 jobs/s    |
| Total time        | 30.4s          |
| Success rate      | 99.3%          |

### Test 2: 5,000 jobs / 10 workers

```bash
python examples/stress_test.py --jobs 5000 --workers 10 --bulk
```

```
============================================================
  RESULTS
============================================================
  Total time:    49.95s
  Completed:     4965
  Failed:        35
  Throughput:    100.1 jobs/s
  Success rate:  99.3%
============================================================
```

| Metric            | Value          |
|-------------------|----------------|
| Dispatch speed    | ~50,000 jobs/s |
| Processing speed  | 100.1 jobs/s   |
| Total time        | 49.9s          |
| Success rate      | 99.3%          |

### Stress Test Options

```bash
python examples/stress_test.py [OPTIONS]

Options:
  --jobs, -j      Number of jobs to dispatch (default: 1000)
  --workers, -w   Number of concurrent workers (default: 5)
  --bulk          Use bulk insert for faster dispatching
  --dashboard     Launch live dashboard on http://localhost:9100
```

**Job types used in the stress test:**

| Job       | Queue           | Latency        | Failure Rate | Max Attempts |
|-----------|-----------------|----------------|--------------|--------------|
| FastJob   | `fast`          | 10-50ms        | 0%           | 3            |
| SlowJob   | `slow`          | 100-300ms      | 0%           | 2            |
| FlakyJob  | `flaky`         | 20-80ms        | ~30%         | 3            |
| HeavyJob  | `heavy`         | 50-150ms       | 0%           | 1            |
| Notify    | `notifications` | 10-40ms        | 0%           | 2            |

### Run with Live Dashboard

```bash
python examples/stress_test.py --jobs 3000 --workers 8 --bulk --dashboard
# Open http://localhost:9100 to watch progress in real-time
```

## License

[MIT](./LICENSE) © Basalam and BaQueue Contributors
