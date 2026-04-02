from __future__ import annotations

from typing import Annotated

from taskiq import TaskiqDepends

from .dependencies import get_data_loader, get_rodada_id_state
from .services import DataLoader
from .tkq import broker


@broker.task(schedule=[{"cron": "*/5 * * * *"}])
async def update_data_task(
    data_loader: Annotated[DataLoader, TaskiqDepends(get_data_loader)],
    rodada_id_state: Annotated[dict, TaskiqDepends(get_rodada_id_state)],
) -> dict:
    old_rodada_id = data_loader.atletas.rodada_id

    await data_loader.atletas.fill_atletas()
    new_rodada_id = data_loader.atletas.rodada_id

    rodada_id_state["previous"] = old_rodada_id
    rodada_id_state["current"] = new_rodada_id

    if old_rodada_id != new_rodada_id:
        await data_loader._update_expensive_tables()
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

    return {
        "rodada_changed": False,
        "rodada_id": new_rodada_id,
    }
