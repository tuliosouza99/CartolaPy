import logging
from contextlib import asynccontextmanager
from typing import AsyncGenerator

from fastapi import FastAPI

from .dependencies import get_redis_store
from .services import DataLoader
from .tkq import broker

logger = logging.getLogger(__name__)


async def setup_dl(app: FastAPI):
    store = get_redis_store()
    app.state.data_loader = DataLoader()
    app.state.redis_store = store

    if app.state.data_loader.load_all_from_redis(store):
        logger.info("Loaded data from Redis cache")
    else:
        logger.info("No cached data found in Redis, fetching from Cartola API")
        await app.state.data_loader.fill_data()
        app.state.data_loader.save_all_to_redis(store)
        logger.info("Data saved to Redis cache")

    app.state.rodada_id_state = {"current": None, "previous": None}


async def shutdown_dl(app: FastAPI):
    await app.state.data_loader.request_handler.close()


@asynccontextmanager
async def _lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    broker.state.fastapi_app = app
    await setup_dl(app)
    if not broker.is_worker_process:
        await broker.startup()

    yield

    if not broker.is_worker_process:
        await broker.shutdown()
    await shutdown_dl(app)
