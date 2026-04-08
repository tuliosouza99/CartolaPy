from __future__ import annotations
import logging
from datetime import datetime, timezone
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

    store = broker.state.redis_store

    atletas_result = await data_loader.atletas.fill_atletas()
    store.save_dataframe("atletas", atletas_result.df)
    store.save_rodada_id(atletas_result.rodada_id)
    if atletas_result.clubes:
        store.save_json("clubes", atletas_result.clubes)
    if atletas_result.posicoes:
        store.save_json("posicoes", atletas_result.posicoes)
    if atletas_result.status:
        store.save_json("status", atletas_result.status)
    store.save_last_updated("atletas", datetime.now(timezone.utc))
    logger.info(f"New rodada_id: {atletas_result.rodada_id}, atletas saved to Redis")

    new_rodada_id = atletas_result.rodada_id
    rodada_id_state["previous"] = old_rodada_id
    rodada_id_state["current"] = new_rodada_id

    if old_rodada_id is not None and old_rodada_id != new_rodada_id:
        logger.info("Rodada changed, updating expensive tables")
        confrontos_df = await data_loader.confrontos.fill_confrontos(new_rodada_id)
        pontuacoes_df = await data_loader.pontuacoes.fill_pontuacoes(new_rodada_id)
        pontos_cedidos_df = data_loader.pontos_cedidos.fill_pontos_cedidos(
            pontuacoes_df, confrontos_df
        )

        store.save_dataframe("confrontos", confrontos_df)
        store.save_last_updated("confrontos", datetime.now(timezone.utc))
        store.save_dataframe("pontuacoes", pontuacoes_df)
        store.save_last_updated("pontuacoes", datetime.now(timezone.utc))
        store.save_dataframe("pontos_cedidos", pontos_cedidos_df)
        store.save_last_updated("pontos_cedidos", datetime.now(timezone.utc))
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
