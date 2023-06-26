import asyncio
import argparse
import os
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


async def create_pontos_cedidos_dfs():
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


async def main(create_mandos_and_confrontos: bool, create_pontos_cedidos: bool):
    os.makedirs('data/csv/pontos_cedidos', exist_ok=True)
    os.makedirs('data/parquet', exist_ok=True)

    await create_clubes_and_posicoes()

    if create_pontos_cedidos:
        await create_pontos_cedidos_dfs()

    if create_mandos_and_confrontos:
        await asyncio.gather(
            asyncio.to_thread(create_confrontos_or_mandos, 'confrontos'),
            asyncio.to_thread(create_confrontos_or_mandos, 'mandos'),
        )


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--create_mandos_and_confrontos', action='store_true')
    parser.add_argument('--create_pontos_cedidos', action='store_true')
    args = parser.parse_args()

    asyncio.run(
        main(
            create_mandos_and_confrontos=args.create_mandos_and_confrontos,
            create_pontos_cedidos=args.create_pontos_cedidos,
        )
    )
