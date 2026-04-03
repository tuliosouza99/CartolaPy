import logging
import os

from fastapi import FastAPI
from taskiq import AsyncBroker, InMemoryBroker, TaskiqEvents
from taskiq_redis.redis_broker import RedisStreamBroker

from .lifespan import setup_dl
from .main import get_app

env = os.environ.get("ENVIRONMENT")

if env == "pytest":
    broker: AsyncBroker = InMemoryBroker(await_inplace=True)
else:
    redis_url = os.environ.get("REDIS_URL", "redis://localhost:6379")
    broker = RedisStreamBroker(url=redis_url)

from . import tasks  # noqa: E402, F401 - needed for task registration


async def startup_handler(state) -> None:

    logger = logging.getLogger(__name__)
    logger.info(f"startup_handler called, is_worker_process={broker.is_worker_process}")

    if broker.is_worker_process:
        logger.info("Setting up app for worker")
        app: FastAPI = get_app()
        await setup_dl(app)
        state.fastapi_app = app
        state.rodada_id_state = app.state.rodada_id_state
        logger.info(f"App set in state: {app}")
    else:
        logger.info("Not a worker, no setup needed")


broker.add_event_handler(TaskiqEvents.WORKER_STARTUP, startup_handler)


async def shutdown_handler(state) -> None:

    logger = logging.getLogger(__name__)
    logger.info("Worker shutting down")
    app = getattr(state, "fastapi_app", None)
    if app is not None:
        from .lifespan import shutdown_dl

        await shutdown_dl(app)


broker.add_event_handler(TaskiqEvents.WORKER_SHUTDOWN, shutdown_handler)
