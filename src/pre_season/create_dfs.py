import asyncio
import sys

import numpy as np
import pandas as pd

if './' not in sys.path:
    sys.path.append('./')

from src.utils import get_page_json


async def create_clubes_and_posicoes():
    json = await get_page_json('https://api.cartolafc.globo.com/atletas/mercado')

    pd.DataFrame(json['clubes']).T.reset_index(drop=True).to_csv('data/csv/clubes.csv')
    pd.DataFrame(json['posicoes']).T.reset_index(drop=True).to_csv(
        'data/csv/posicoes.csv'
    )


def create_confrontos_or_mandos(table_name: str):
    clubes_df = pd.read_csv('data/csv/clubes.csv', index_col=0)

    pd.DataFrame(
        np.empty((20, 38)) * np.nan,
        index=clubes_df['id'].astype(int),
        columns=list(map(str, range(1, 39))),
    ).reset_index().rename({'id': 'clube_id'}).to_csv(f'data/csv/{table_name}.csv')


async def create_pontos_cedidos():
    posicoes_df = pd.read_csv('data/csv/posicoes.csv', index_col=0)
    clubes_df = pd.read_csv('data/csv/clubes.csv', index_col=0)

    await asyncio.gather(
        *[
            asyncio.to_thread(create_pontos_cedidos_posicao, clubes_df, posicao)
            for posicao in posicoes_df['id']
        ]
    )


def create_pontos_cedidos_posicao(clubes_df: pd.DataFrame, posicao: int):
    pontos_cedidos_posicao_df = pd.DataFrame(
        np.empty((20, 38)) * np.nan,
        index=clubes_df['id'].astype(int),
        columns=list(map(str, range(1, 39))),
    )

    pontos_cedidos_posicao_df.reset_index().rename(columns={'id': 'clube_id'}).to_csv(
        f'data/csv/pontos_cedidos/{posicao}.csv'
    )


async def main():
    await create_clubes_and_posicoes()
    await asyncio.gather(
        asyncio.to_thread(create_confrontos_or_mandos, 'confrontos'),
        asyncio.to_thread(create_confrontos_or_mandos, 'mandos'),
        create_pontos_cedidos(),
    )


if __name__ == '__main__':
    asyncio.run(main())
