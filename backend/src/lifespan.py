import logging
from contextlib import asynccontextmanager
from datetime import datetime, timezone
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

    atletas_df = store.load_dataframe("atletas")
    if atletas_df is None or atletas_df.empty:
        logger.info("No cached data found in Redis, fetching from Cartola API")
        atletas_result = await app.state.data_loader.atletas.fill_atletas()
        store.save_dataframe("atletas", atletas_result.df)
        store.save_rodada_id(atletas_result.rodada_id)
        if atletas_result.clubes:
            store.save_json("clubes", atletas_result.clubes)
        if atletas_result.posicoes:
            store.save_json("posicoes", atletas_result.posicoes)
        if atletas_result.status:
            store.save_json("status", atletas_result.status)
        store.save_last_updated("atletas", datetime.now(timezone.utc))
        logger.info("Data saved to Redis cache")

        confrontos_df = await app.state.data_loader.confrontos.fill_confrontos(
            atletas_result.rodada_id
        )
        pontuacoes_df = await app.state.data_loader.pontuacoes.fill_pontuacoes(
            atletas_result.rodada_id
        )
        pontos_cedidos_df = app.state.data_loader.pontos_cedidos.fill_pontos_cedidos(
            pontuacoes_df, confrontos_df
        )

        store.save_dataframe("confrontos", confrontos_df)
        store.save_last_updated("confrontos", datetime.now(timezone.utc))
        store.save_dataframe("pontuacoes", pontuacoes_df)
        store.save_last_updated("pontuacoes", datetime.now(timezone.utc))
        store.save_dataframe("pontos_cedidos", pontos_cedidos_df)
        store.save_last_updated("pontos_cedidos", datetime.now(timezone.utc))
        logger.info("All tables populated in Redis")
    else:
        logger.info("Loaded data from Redis cache")

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
