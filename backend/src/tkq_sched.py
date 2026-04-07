from taskiq import TaskiqScheduler
from taskiq.schedule_sources import LabelScheduleSource

from . import tasks  # noqa: F401 - needed for task registration
from .tkq import broker

scheduler = TaskiqScheduler(
    broker,
    [
        LabelScheduleSource(broker),
    ],
)
