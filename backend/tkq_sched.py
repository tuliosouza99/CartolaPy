from taskiq import TaskiqScheduler
from taskiq.schedule_sources import LabelScheduleSource

from .tkq import broker

scheduler = TaskiqScheduler(
    broker,
    [
        LabelScheduleSource(broker),
    ],
)
