import sys
import asyncio
import argparse

import numpy as np
import pandas as pd
from tqdm import tqdm

if './' not in sys.path:
    sys.path.append('./')

from src.utils import get_page_json


def read_pontos_cedidos(posicoes: list) -> dict[int, pd.DataFrame]:
    dfs = [
        pd.read_csv(f'data/csv/pontos_cedidos/{posicao}.csv', index_col=0)
        for posicao in posicoes
    ]
    return dict(zip(posicoes, dfs))


def update_pontos_cedidos_posicao_clube(
    clube: int,
    pontos_cedidos_df: pd.DataFrame,
    rodada_atual: int,
    rodada_posicao_df: pd.DataFrame,
    confrontos_df: pd.DataFrame,
):
    adversario = confrontos_df.at[clube, str(rodada_atual)]

    if not np.isnan(adversario):
        rodada_posicao_clube_df = rodada_posicao_df.loc[
            rodada_posicao_df['clube_id'] == adversario
        ]
        pontos_cedidos_df.loc[
            pontos_cedidos_df['clube_id'] == clube, str(rodada_atual)
        ] = np.mean(rodada_posicao_clube_df['pontuacao'])


async def update_pontos_cedidos_posicao(
    pontos_cedidos_df: pd.DataFrame,
    posicao: int,
    rodada_atual: int,
    rodada_df: pd.DataFrame,
    confrontos_df: pd.DataFrame,
):
    # Seleciona todos os atletas de uma posição que atuaram na rodada
    rodada_posicao_df = rodada_df.loc[rodada_df['posicao_id'] == posicao]

    await asyncio.gather(
        *[
            asyncio.to_thread(
                update_pontos_cedidos_posicao_clube,
                clube,
                pontos_cedidos_df,
                rodada_atual,
                rodada_posicao_df,
                confrontos_df,
            )
            for clube in pontos_cedidos_df['clube_id']
        ]
    )
    pontos_cedidos_df.to_csv(f'data/csv/pontos_cedidos/{posicao}.csv')


async def update_pontos_cedidos(pbar=None, rodada=None):
    if rodada is None:
        atletas_df = pd.read_csv('data/csv/atletas.csv', index_col=0)
        rodada = int(atletas_df.at[0, 'rodada_id'])

    json = await get_page_json(
        f'https://api.cartolafc.globo.com/atletas/pontuados/{rodada}'
    )
    rodada_df = pd.DataFrame(json['atletas']).T

    posicoes_df = pd.read_csv('data/csv/posicoes.csv', index_col=0)
    pontos_cedidos = read_pontos_cedidos(posicoes_df['id'].tolist())

    confrontos_df = pd.read_csv('data/csv/confrontos.csv', index_col=0).set_index(
        'clube_id'
    )

    await asyncio.gather(
        *[
            update_pontos_cedidos_posicao(
                df,
                posicao,
                rodada,
                rodada_df,
                confrontos_df,
            )
            for posicao, df in pontos_cedidos.items()
        ]
    )

    if pbar is not None:
        pbar.progress(20)


async def update_pontos_cedidos_multiple_rounds(rodadas: list[int]):
    for rodada in tqdm(rodadas):
        await update_pontos_cedidos(rodada=rodada)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--rounds', nargs='+', type=int)
    parser.add_argument('--all_rounds', action='store_true')
    args = parser.parse_args()

    if args.all_rounds and args.rounds is not None:
        raise ValueError('Cannot use --rounds with --all_rounds')

    elif args.all_rounds:
        print('Atualizando pontos cedidos de todas as rodadas...')

        atletas_df = pd.read_csv('data/csv/atletas.csv', index_col=0)
        rodada_atual = int(atletas_df.at[0, 'rodada_id'])
        asyncio.run(
            update_pontos_cedidos_multiple_rounds(list(range(1, rodada_atual + 1)))
        )

    elif args.rounds is not None:
        atletas_df = pd.read_csv('data/csv/atletas.csv', index_col=0)
        rodada_atual = int(atletas_df.at[0, 'rodada_id'])
        rodadas = [rodada for rodada in args.rounds if rodada <= rodada_atual]

        print(f'Atualizando pontos cedidos das rodadas {rodadas}...')
        asyncio.run(update_pontos_cedidos_multiple_rounds(rodadas))

    else:
        print('Atualizando pontos cedidos da rodada atual...')
        asyncio.run(update_pontos_cedidos())
