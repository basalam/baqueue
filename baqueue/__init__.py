"""BaQueue - A powerful Python queue management package inspired by Laravel Horizon."""

from baqueue.config import BaQueueConfig
from baqueue.job import Job
from baqueue.queue import Queue
from baqueue.batch import Batch
from baqueue.events import EventBus
from baqueue.retry import BackoffStrategy

__version__ = "1.2.1"

__all__ = [
    "BaQueueConfig",
    "Job",
    "Queue",
    "Batch",
    "EventBus",
    "BackoffStrategy",
]
