import asyncio
from typing import Iterable

import numpy as np
import pandas as pd
from stqdm import stqdm

from src.utils import get_page_json


class PontosCedidosUpdater:
    def __init__(self):
        posicoes_df = pd.read_csv("data/csv/posicoes.csv", index_col=0)
        self.posicoes = posicoes_df["id"].tolist()
        self.df = pd.read_csv("data/csv/pontos_cedidos.csv")
        self.confrontos_df = pd.read_csv("data/csv/confrontos.csv")

    def _update_pontos_cedidos_posicao_clube(
        self,
        posicao: int,
        clube: int,
        rodada_atual: int,
        rodada_posicao_df: pd.DataFrame,
    ):
        mask = (self.confrontos_df["clube_id"] == clube) & (
            self.confrontos_df["rodada"] == rodada_atual
        )
        adversario = self.confrontos_df.loc[mask, "adversario"].values

        if len(adversario) == 0 or np.isnan(adversario[0]):
            return

        adversario = int(adversario[0])
        rodada_posicao_clube_df = rodada_posicao_df.loc[
            rodada_posicao_df["clube_id"] == adversario
        ]

        mask = (
            (self.df["posicao"] == posicao)
            & (self.df["clube_id"] == clube)
            & (self.df["rodada"] == rodada_atual)
        )
        self.df.loc[mask, "pontos_cedidos"] = np.mean(
            rodada_posicao_clube_df["pontuacao"]
        )

    async def _update_pontos_cedidos_posicao(
        self, posicao: int, rodada_atual: int, rodada_df: pd.DataFrame
    ):
        rodada_posicao_df = rodada_df.loc[rodada_df["posicao_id"] == posicao]

        await asyncio.gather(
            *[
                asyncio.to_thread(
                    self._update_pontos_cedidos_posicao_clube,
                    posicao,
                    clube,
                    rodada_atual,
                    rodada_posicao_df,
                )
                for clube in self.df.loc[self.df["posicao"] == posicao, "clube_id"]
            ]
        )

    async def _update_pontos_cedidos_one_round(self, rodada: int):
        json = await get_page_json(
            f"https://api.cartola.globo.com/atletas/pontuados/{rodada}"
        )
        rodada_df = pd.DataFrame(json["atletas"]).T

        await asyncio.gather(
            *[
                self._update_pontos_cedidos_posicao(posicao, rodada, rodada_df)
                for posicao in self.posicoes
            ]
        )

    async def update_pontos_cedidos(self, rodadas: int | Iterable[int]):
        if isinstance(rodadas, int):
            rodadas = [rodadas]

        await asyncio.gather(
            *[
                self._update_pontos_cedidos_one_round(rodada)
                for rodada in stqdm(
                    rodadas,
                    desc=f"Atualizando pontos cedidos para as rodadas {rodadas}",
                    total=len(list(rodadas)),
                    backend=True,
                )
            ]
        )

        self.df.dropna(subset=["pontos_cedidos"]).to_csv(
            "data/csv/pontos_cedidos.csv", index=False
        )
