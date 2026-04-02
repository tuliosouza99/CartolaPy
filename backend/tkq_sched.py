from taskiq import TaskiqScheduler
from taskiq.schedule_sources import LabelScheduleSource

from backend.tkq import broker

scheduler = TaskiqScheduler(
    broker,
    [
        LabelScheduleSource(broker),
    ],
)
