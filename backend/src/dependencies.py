import os
from functools import lru_cache

from .services import DataLoader
from .services.redis_store import RedisDataFrameStore


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


@lru_cache
def get_redis_store() -> RedisDataFrameStore:
    from .tkq import broker

    if hasattr(broker.state, "redis_store") and broker.state.redis_store is not None:
        return broker.state.redis_store
    redis_url = _get_redis_url()
    return RedisDataFrameStore(redis_url)


async def get_data_loader() -> DataLoader:
    from .tkq import broker

    app = getattr(broker.state, "fastapi_app", None)
    if app is None:
        raise RuntimeError("FastAPI app not initialized in broker state")
    return app.state.data_loader


async def get_rodada_id_state() -> dict:
    from .tkq import broker

    return broker.state.rodada_id_state
