from __future__ import annotations
import logging
from typing import Annotated

from taskiq import TaskiqDepends

from .dependencies import get_data_loader, get_rodada_id_state
from .services import DataLoader
from .tkq import broker

logger = logging.getLogger(__name__)


@broker.task(schedule=[{"cron": "*/5 * * * *"}])
async def update_data_task(
    data_loader: Annotated[DataLoader, TaskiqDepends(get_data_loader)],
    rodada_id_state: Annotated[dict, TaskiqDepends(get_rodada_id_state)],
) -> dict:
    logger.info("update_data_task started")
    old_rodada_id = rodada_id_state["current"]
    logger.info(f"Old rodada_id: {old_rodada_id}")

    await data_loader.atletas.fill_atletas()
    new_rodada_id = data_loader.atletas.rodada_id
    logger.info(
        f"New rodada_id: {new_rodada_id}, atletas updated at: {data_loader.atletas.last_updated}"
    )

    store = broker.state.redis_store
    data_loader.atletas.save_to_redis(store)
    logger.info("Atletas saved to Redis")

    rodada_id_state["previous"] = old_rodada_id
    rodada_id_state["current"] = new_rodada_id

    if old_rodada_id != new_rodada_id:
        logger.info("Rodada changed, updating expensive tables")
        await data_loader._update_expensive_tables()
        data_loader.save_all_to_redis(store)
        logger.info("All tables saved to Redis after rodada change")
        return {
            "rodada_changed": True,
            "old_rodada_id": old_rodada_id,
            "new_rodada_id": new_rodada_id,
            "updated_dataframes": [
                "atletas",
                "confrontos",
                "pontuacoes",
                "pontos_cedidos",
            ],
        }

    logger.info("update_data_task completed")
    return {
        "rodada_changed": False,
        "rodada_id": new_rodada_id,
    }
