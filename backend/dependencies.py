from fastapi import Request
from taskiq import TaskiqDepends

from .services import DataLoader


def get_data_loader(request: Request = TaskiqDepends()) -> DataLoader:
    return request.app.state.data_loader
