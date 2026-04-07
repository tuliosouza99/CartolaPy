import os
from functools import lru_cache

from .services import DataLoader
from .services.redis_store import RedisDataFrameStore


@lru_cache
def get_redis_store() -> RedisDataFrameStore:
    from .tkq import broker

    if hasattr(broker.state, "redis_store") and broker.state.redis_store is not None:
        return broker.state.redis_store
    redis_url = os.environ.get("REDIS_URL", "redis://localhost:6379")
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
