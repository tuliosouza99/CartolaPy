import asyncio
from typing import Iterable

import numpy as np
import pandas as pd
from stqdm import stqdm

from src.utils import get_page_json


class PontosCedidosUpdater:
    def __init__(self):
        self.pontos_cedidos_df = pd.read_csv("data/csv/pontos_cedidos.csv")
        self.confrontos_df = pd.read_csv("data/csv/confrontos.csv")

    def _update_pontos_cedidos_posicao_clube(
        self,
        posicao: int,
        clube: int,
        rodada_atual: int,
        rodada_posicao_df: pd.DataFrame,
    ):
        adversario_row = self.confrontos_df[
            (self.confrontos_df["clube_id"] == clube)
            & (self.confrontos_df["rodada"] == rodada_atual)
        ]

        if len(adversario_row) > 0 and not np.isnan(
            adversario_row.iloc[0]["adversario_id"]
        ):
            adversario = int(adversario_row.iloc[0]["adversario_id"])
            rodada_posicao_clube_df = rodada_posicao_df.loc[
                rodada_posicao_df["clube_id"] == adversario
            ]
            mask = (
                (self.pontos_cedidos_df["clube_id"] == clube)
                & (self.pontos_cedidos_df["posicao_id"] == posicao)
                & (self.pontos_cedidos_df["rodada"] == rodada_atual)
            )
            self.pontos_cedidos_df.loc[mask, "pontos_cedidos"] = np.mean(
                rodada_posicao_clube_df["pontuacao"]
            )

    async def _update_pontos_cedidos_posicao(
        self, posicao: int, rodada_atual: int, rodada_df: pd.DataFrame
    ):
        rodada_posicao_df = rodada_df.loc[rodada_df["posicao_id"] == posicao]
        clubes = self.pontos_cedidos_df[
            self.pontos_cedidos_df["posicao_id"] == posicao
        ]["clube_id"].unique()

        await asyncio.gather(
            *[
                asyncio.to_thread(
                    self._update_pontos_cedidos_posicao_clube,
                    posicao,
                    clube,
                    rodada_atual,
                    rodada_posicao_df,
                )
                for clube in clubes
            ]
        )

    async def _update_pontos_cedidos_one_round(self, rodada: int):
        json = await get_page_json(
            f"https://api.cartola.globo.com/atletas/pontuados/{rodada}"
        )
        rodada_df = pd.DataFrame(json["atletas"]).T

        posicoes = self.pontos_cedidos_df["posicao_id"].unique()

        await asyncio.gather(
            *[
                self._update_pontos_cedidos_posicao(posicao, rodada, rodada_df)
                for posicao in posicoes
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

        self.pontos_cedidos_df.to_csv("data/csv/pontos_cedidos.csv", index=False)
