import asyncio
from typing import Iterable

import pandas as pd
import numpy as np
from stqdm import stqdm

from src.utils import get_page_json


class ConfrontosOrMandosUpdater:
    def __init__(self, table_name: str):
        self.table_name = table_name
        self.df = pd.read_csv(f'data/csv/{self.table_name}.csv', index_col=0).set_index(
            'clube_id'
        )

    def _update_clube(
        self,
        clube: int,
        rodada_atual: int,
        partidas_rodada: pd.DataFrame,
    ):
        if clube in partidas_rodada['clube_casa_id'].values:
            idx = int(np.where(partidas_rodada['clube_casa_id'] == clube)[0])

            if partidas_rodada.at[idx, 'valida']:
                if self.table_name == 'mandos':
                    self.df.loc[clube, str(rodada_atual)] = 1
                else:
                    self.df.loc[clube, str(rodada_atual)] = partidas_rodada.at[
                        idx, 'clube_visitante_id'
                    ]
        else:
            idx = int(np.where(partidas_rodada['clube_visitante_id'] == clube)[0])

            if partidas_rodada.at[idx, 'valida']:
                if self.table_name == 'mandos':
                    self.df.loc[clube, str(rodada_atual)] = 0
                else:
                    self.df.loc[clube, str(rodada_atual)] = partidas_rodada.at[
                        idx, 'clube_casa_id'
                    ]

    async def _update_table_one_round(self, rodada: int):
        json = await get_page_json(
            f'https://api.cartola.globo.com/partidas/{rodada}'
        )
        partidas_rodada = pd.DataFrame(json['partidas'])

        await asyncio.gather(
            *[
                asyncio.to_thread(self._update_clube, clube, rodada, partidas_rodada)
                for clube in self.df.index
            ]
        )

    async def update_table(self, rodadas: int | Iterable[int]):
        if isinstance(rodadas, int):
            rodadas = [rodadas]

        await asyncio.gather(
            *[
                self._update_table_one_round(rodada)
                async for rodada in stqdm(
                    rodadas,
                    desc=f'Atualizando {self.table_name} para as rodadas {rodadas}',
                    total=len(list(rodadas)),
                    backend=True,
                )
            ]
        )

        self.df.reset_index().to_csv(f'data/csv/{self.table_name}.csv')
