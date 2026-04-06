# BaQueue

A powerful Python queue management package inspired by **Laravel Horizon**. Multi-driver support, batch jobs, scheduling, auto-balancing, and a beautiful real-time monitoring dashboard.

## Features

- **Multi-driver**: SQLite (default), Redis, PostgreSQL, or In-Memory
- **Simple API**: Class-based or decorator-based job definitions
- **Batch Jobs**: Dispatch groups of jobs with `then`/`catch`/`finally` callbacks
- **Delayed Jobs**: Schedule jobs with a delay or cron expression
- **Retry & Backoff**: Fixed, linear, exponential, or custom strategies
- **Auto-balancing**: Dynamically scale workers based on queue pressure
- **Pruning**: Remove old jobs by status, tag, or age
- **Monitoring Dashboard**: Real-time WebSocket-powered UI with date filtering
- **CLI**: Manage workers, scheduler, dashboard, and pruning from the command line
- **Cross-process**: SQLite driver shares state between dashboard and workers without external dependencies

## Quick Start

```bash
# Install (SQLite driver works out of the box, zero dependencies)
pip install -e .

# With Redis support
pip install -e ".[redis]"

# With PostgreSQL support
pip install -e ".[postgres]"

# With dashboard
pip install -e ".[dashboard]"

# Everything
pip install -e ".[all]"
```

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

```python
# Remove completed jobs older than 24 hours
await Queue.prune(status="completed", hours=24)

# Remove jobs by tag
await Queue.prune(tag="batch:newsletter")
```

### Dashboard

```bash
# Start the dashboard (uses SQLite by default)
baqueue dashboard

# Open http://localhost:9100
```

The dashboard includes:
- Real-time overview with pending/processing/completed/failed counters
- Date range filtering (custom range + presets: 1h, 24h, 7d, 30d)
- Job detail modal with timeline, payload data, and error trace
- Queue breakdown with progress bars
- Worker monitoring with active/idle status
- Dark/light theme toggle

Run in one terminal:
```bash
baqueue dashboard
```

Dispatch jobs in another terminal:
```bash
python examples/simple_job.py
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

# Stress test (see Benchmarks section below)
python examples/stress_test.py --jobs 1000 --workers 5 --bulk
```

## CLI Commands

```
baqueue work       Start processing jobs
baqueue schedule   Start the job scheduler
baqueue dashboard  Launch the monitoring dashboard
baqueue prune      Prune old jobs
baqueue status     Show queue status
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

MIT
