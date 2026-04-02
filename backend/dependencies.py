from fastapi import Request
from taskiq import TaskiqDepends

from .services import DataLoader


async def get_data_loader(request: Request = TaskiqDepends()) -> DataLoader:
    return request.app.state.data_loader


async def get_rodada_id_state(request: Request = TaskiqDepends()) -> dict:
    return request.app.state.rodada_id_state
