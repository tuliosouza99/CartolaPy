import asyncio
import logging
from pathlib import Path

import pandas as pd

from backend.core.config import (
    DATA_DIR,
    CSV_DIR,
    JSON_DIR,
    PARQUET_DIR,
    PONTOS_CEDIDOS_DIR,
)
from backend.src.atletas_updater import update_atletas
from backend.src.confrontos_or_mandos_updater import ConfrontosOrMandosUpdater
from backend.src.pontos_cedidos_updater import PontosCedidosUpdater
from backend.src.pontuacoes_updater import update_pontuacoes_and_scouts
from backend.src.pre_season.dfs_creator import (
    create_clubes_and_posicoes,
    create_confrontos_or_mandos,
    create_pontos_cedidos_dfs,
)
from backend.src.pre_season.dicts_creator import create_dicts
from backend.src.utils import get_page_json

logger = logging.getLogger(__name__)


class Updater:
    def __init__(self):
        self._ensure_directories()

    def _ensure_directories(self):
        for directory in [DATA_DIR, CSV_DIR, JSON_DIR, PARQUET_DIR, PONTOS_CEDIDOS_DIR]:
            directory.mkdir(parents=True, exist_ok=True)

    def _clear_data_files(self):
        for pattern in ["data/csv/*", "data/json/*", "data/parquet/*"]:
            for file in Path(".").glob(pattern):
                if file.is_file():
                    file.unlink()
        pontos_cedidos_dir = Path("data/csv/pontos_cedidos")
        if pontos_cedidos_dir.exists():
            for file in pontos_cedidos_dir.glob("*"):
                if file.is_file():
                    file.unlink()

    async def full_rebuild(self):
        logger.info("Starting full rebuild from round 1 to current round")
        self._clear_data_files()

        await create_clubes_and_posicoes()
        await asyncio.gather(create_dicts(), update_atletas())

        market_json = await get_page_json(
            "https://api.cartola.globo.com/mercado/status"
        )
        rodada_atual = market_json["rodada_atual"]

        await asyncio.gather(
            create_pontos_cedidos_dfs(),
            asyncio.to_thread(create_confrontos_or_mandos, "confrontos"),
            asyncio.to_thread(create_confrontos_or_mandos, "mandos"),
        )

        if rodada_atual == 1:
            await asyncio.gather(
                ConfrontosOrMandosUpdater("mandos").update_table(1),
                ConfrontosOrMandosUpdater("confrontos").update_table(1),
                update_pontuacoes_and_scouts(first_round=True),
            )
        else:
            await asyncio.gather(
                ConfrontosOrMandosUpdater("mandos").update_table(
                    range(1, rodada_atual + 1)
                ),
                ConfrontosOrMandosUpdater("confrontos").update_table(
                    range(1, rodada_atual + 1)
                ),
                update_pontuacoes_and_scouts(),
            )
            pontos_cedidos_updater = PontosCedidosUpdater()
            await pontos_cedidos_updater.update_pontos_cedidos(range(1, rodada_atual))

        logger.info(f"Full rebuild completed up to round {rodada_atual}")

    async def get_market_status(self) -> dict:
        return await get_page_json("https://api.cartola.globo.com/mercado/status")

    def get_atletas(self) -> pd.DataFrame:
        return pd.read_csv("data/csv/atletas.csv", index_col=0)

    def get_pontuacoes(self) -> pd.DataFrame:
        return pd.read_csv("data/csv/pontuacoes.csv", index_col=0)

    def get_scouts(self) -> pd.DataFrame:
        return pd.read_parquet("data/parquet/scouts.parquet")

    def get_clubes(self) -> pd.DataFrame:
        return pd.read_csv("data/csv/clubes.csv", index_col=0)

    def get_confrontos(self) -> pd.DataFrame:
        return pd.read_csv("data/csv/confrontos.csv", index_col=0)

    def get_mandos(self) -> pd.DataFrame:
        return pd.read_csv("data/csv/mandos.csv", index_col=0)

    def get_posicoes(self) -> pd.DataFrame:
        return pd.read_csv("data/csv/posicoes.csv", index_col=0)

    def get_pontos_cedidos(self, posicao: int) -> pd.DataFrame:
        return pd.read_csv(f"data/csv/pontos_cedidos/{posicao}.csv", index_col=0)

    def get_status_dict(self) -> dict:
        import json

        with open("data/json/status.json") as f:
            return json.load(f)

    def get_clubes_dict(self) -> dict:
        import json

        with open("data/json/clubes.json") as f:
            return json.load(f)

    def get_posicoes_dict(self) -> dict:
        import json

        with open("data/json/posicoes.json") as f:
            return json.load(f)
