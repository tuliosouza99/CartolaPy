import sys
import asyncio

import numpy as np
import pandas as pd

if './' not in sys.path:
    sys.path.append('./')

from src.utils import get_page_json


def create_pontuacoes(atletas: pd.DataFrame) -> pd.DataFrame:
    pontuacoes = pd.DataFrame(
        np.empty((atletas.shape[0], 38)) * np.nan,
        index=atletas['atleta_id'],
        columns=list(map(str, range(1, 39))),
    )

    return pontuacoes.reset_index()


async def update_pontuacoes_rodada(rodada: int, pontuacoes_df: pd.DataFrame):
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


async def update_pontuacoes(pbar=None):
    atletas_df = pd.read_csv('data/csv/atletas.csv', index_col=0)
    rodada_atual = int(atletas_df.at[0, 'rodada_id'])
    pontuacoes_df = create_pontuacoes(atletas_df)

    await asyncio.gather(*[update_pontuacoes_rodada(rodada, pontuacoes_df) for rodada in range(1, rodada_atual + 1)])
    pontuacoes_df.to_csv('data/csv/pontuacoes.csv')

    if pbar is not None:
        pbar.progress(20)


if __name__ == '__main__':
    asyncio.run(update_pontuacoes())
