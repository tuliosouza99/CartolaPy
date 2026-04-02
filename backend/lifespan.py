from contextlib import asynccontextmanager
from typing import AsyncGenerator

from fastapi import FastAPI

from .services import DataLoader
from .tkq import broker


async def setup_dl(app: FastAPI):
    app.state.data_loader = DataLoader()
    await app.state.data_loader.fill_data()
    app.state.rodada_id_state = {"current": None, "previous": None}


async def shutdown_dl(app: FastAPI):
    await app.state.data_loader.request_handler.close()


@asynccontextmanager
async def _lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    await setup_dl(app)
    if not broker.is_worker_process:
        await broker.startup()

    yield

    if not broker.is_worker_process:
        await broker.shutdown()
    await shutdown_dl(app)
