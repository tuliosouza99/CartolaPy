import os
import asyncio

import numpy as np
import pandas as pd
from stqdm import stqdm

from src.utils import get_page_json


def create_df(atletas: pd.DataFrame) -> pd.DataFrame:
    df = pd.DataFrame(
        np.empty((atletas.shape[0], 38)) * np.nan,
        index=atletas['atleta_id'],
        columns=list(map(str, range(1, 39))),
    )

    return df.reset_index()


async def update_pontuacoes_and_scouts_rodada(
    rodada: int, pontuacoes_df: pd.DataFrame, scouts_df: pd.DataFrame
):
    json = await get_page_json(
        f'https://api.cartolafc.globo.com/atletas/pontuados/{rodada}'
    )

    rodada_df = (
        pd.DataFrame(json['atletas'])
        .T.reset_index()
        .astype({'index': 'int64'})
        .sort_values(by=['index'])
        .loc[lambda _df: _df['index'].isin(pontuacoes_df['atleta_id'].to_list())]
    )

    pontuacoes_df.loc[
        pontuacoes_df['atleta_id'].isin(rodada_df['index'].to_list()),
        str(rodada),
    ] = rodada_df['pontuacao'].to_list()

    scouts_df.loc[
        scouts_df['atleta_id'].isin(rodada_df['index'].to_list()),
        str(rodada),
    ] = rodada_df['scout'].to_list()


async def update_pontuacoes_and_scouts(first_round=False):
    atletas_df = pd.read_csv('data/csv/atletas.csv', index_col=0)
    rodada_atual = int(atletas_df.at[0, 'rodada_id'])
    pontuacoes_df = create_df(atletas_df)
    scouts_df = create_df(atletas_df)

    if not first_round:
        await asyncio.gather(
            *[
                update_pontuacoes_and_scouts_rodada(rodada, pontuacoes_df, scouts_df)
                for rodada in stqdm(
                    range(1, rodada_atual + 1),
                    desc='Atualizando as pontuações dos atletas...',
                    backend=True,
                )
            ]
        )

    pontuacoes_df.to_csv('data/csv/pontuacoes.csv')
    os.makedirs('data/parquet', exist_ok=True)
    scouts_df.to_parquet('data/parquet/scouts.parquet')
