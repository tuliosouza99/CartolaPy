import logging
import os

from taskiq import AsyncBroker, InMemoryBroker, TaskiqEvents
from taskiq_redis.redis_broker import RedisStreamBroker


def _get_redis_url() -> str:
    env = os.environ.get("ENVIRONMENT")
    redis_url = os.environ.get("REDIS_URL")

    if not redis_url:
        if env == "production":
            raise RuntimeError(
                "REDIS_URL environment variable must be set in production"
            )
        redis_url = "redis://localhost:6379"

    if env == "production" and not _has_redis_password(redis_url):
        raise RuntimeError(
            "REDIS_URL must include password in production (format: redis://:password@host:port)"
        )

    return redis_url


def _has_redis_password(redis_url: str) -> bool:
    if "@" not in redis_url:
        return False
    scheme_part = redis_url.split("@")[0]
    return ":" in scheme_part and scheme_part != "redis:"


env = os.environ.get("ENVIRONMENT")

if env == "pytest":
    broker: AsyncBroker = InMemoryBroker(await_inplace=True)
else:
    redis_url = _get_redis_url()
    broker = RedisStreamBroker(url=redis_url)

from . import tasks  # noqa: E402, F401 - needed for task registration


async def startup_handler(state) -> None:

    logger = logging.getLogger(__name__)
    logger.info(f"startup_handler called, is_worker_process={broker.is_worker_process}")

    if broker.is_worker_process:
        logger.info("Setting up app for worker")
        from .lifespan import setup_dl
        from .main import get_app

        app = get_app()
        await setup_dl(app)
        state.fastapi_app = app
        state.rodada_id_state = app.state.rodada_id_state
        state.redis_store = app.state.redis_store
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
