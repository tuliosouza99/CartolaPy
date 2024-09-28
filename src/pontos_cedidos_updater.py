import asyncio
from typing import Iterable

import numpy as np
import pandas as pd
from stqdm import stqdm

from src.utils import get_page_json


class PontosCedidosUpdater:
    def __init__(self):
        posicoes_df = pd.read_csv('data/csv/posicoes.csv', index_col=0)

        self.pos_to_pontos_cedidos = self._read_pontos_cedidos(
            posicoes_df['id'].tolist()
        )
        self.confrontos_df = pd.read_csv(
            'data/csv/confrontos.csv', index_col=0
        ).set_index('clube_id')

    def _read_pontos_cedidos(self, posicoes: list) -> dict[int, pd.DataFrame]:
        dfs = [
            pd.read_csv(f'data/csv/pontos_cedidos/{posicao}.csv', index_col=0)
            for posicao in posicoes
        ]
        return dict(zip(posicoes, dfs))

    def _update_pontos_cedidos_posicao_clube(
        self,
        posicao: int,
        clube: int,
        rodada_atual: int,
        rodada_posicao_df: pd.DataFrame,
    ):
        adversario = self.confrontos_df.at[clube, str(rodada_atual)]

        if not np.isnan(adversario):
            rodada_posicao_clube_df = rodada_posicao_df.loc[
                rodada_posicao_df['clube_id'] == adversario
            ]
            self.pos_to_pontos_cedidos[posicao].loc[
                self.pos_to_pontos_cedidos[posicao]['clube_id'] == clube,
                str(rodada_atual),
            ] = np.mean(rodada_posicao_clube_df['pontuacao'])

    async def _update_pontos_cedidos_posicao(
        self, posicao: int, rodada_atual: int, rodada_df: pd.DataFrame
    ):
        # Seleciona todos os atletas de uma posição que atuaram na rodada
        rodada_posicao_df = rodada_df.loc[rodada_df['posicao_id'] == posicao]

        await asyncio.gather(
            *[
                asyncio.to_thread(
                    self._update_pontos_cedidos_posicao_clube,
                    posicao,
                    clube,
                    rodada_atual,
                    rodada_posicao_df,
                )
                for clube in self.pos_to_pontos_cedidos[posicao]['clube_id']
            ]
        )

    async def _update_pontos_cedidos_one_round(self, rodada: int):
        json = await get_page_json(
            f'https://api.cartolafc.globo.com/atletas/pontuados/{rodada}'
        )
        rodada_df = pd.DataFrame(json['atletas']).T

        await asyncio.gather(
            *[
                self._update_pontos_cedidos_posicao(posicao, rodada, rodada_df)
                for posicao in self.pos_to_pontos_cedidos.keys()
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
                    desc=f'Atualizando pontos cedidos para as rodadas {rodadas}',
                    total=len(list(rodadas)),
                    backend=True,
                )
            ]
        )

        for posicao, df in self.pos_to_pontos_cedidos.items():
            df.to_csv(f'data/csv/pontos_cedidos/{posicao}.csv')
