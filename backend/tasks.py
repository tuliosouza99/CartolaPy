from taskiq import TaskiqDepends

from .dependencies import get_data_loader
from .services import DataLoader
from .tkq import broker


@broker.task(schedule=[{"cron": ""}])
async def update_data(data_loader: DataLoader = TaskiqDepends(get_data_loader)):
    pass
