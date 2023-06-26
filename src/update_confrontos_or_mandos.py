import asyncio
import argparse
import sys

import pandas as pd
import numpy as np
from tqdm import tqdm

if './' not in sys.path:
    sys.path.append('./')

from src.utils import get_page_json


def update_clube(
    clube: int,
    df: pd.DataFrame,
    rodada_atual: int,
    partidas_rodada: pd.DataFrame,
    table_name: str,
):
    if clube in partidas_rodada['clube_casa_id'].values:
        idx = int(np.where(partidas_rodada['clube_casa_id'] == clube)[0])

        if partidas_rodada.at[idx, 'valida']:
            if table_name == 'mandos':
                df.loc[clube, rodada_atual] = 1
            else:
                df.loc[clube, rodada_atual] = partidas_rodada.at[
                    idx, 'clube_visitante_id'
                ]
    else:
        idx = int(np.where(partidas_rodada['clube_visitante_id'] == clube)[0])

        if partidas_rodada.at[idx, 'valida']:
            if table_name == 'mandos':
                df.loc[clube, rodada_atual] = 0
            else:
                df.loc[clube, rodada_atual] = partidas_rodada.at[idx, 'clube_casa_id']


async def update_confrontos_or_mandos(table_name: str, pbar=None, rodada=None):
    df = pd.read_csv(f'data/csv/{table_name}.csv', index_col=0).set_index('clube_id')

    if rodada is None:
        json = await get_page_json('https://api.cartolafc.globo.com/partidas')
        rodada = str(json['rodada'])
    else:
        json = await get_page_json(f'https://api.cartolafc.globo.com/partidas/{rodada}')

    partidas_rodada = pd.DataFrame(json['partidas'])

    await asyncio.gather(
        *[
            asyncio.to_thread(
                update_clube, clube, df, rodada, partidas_rodada, table_name
            )
            for clube in df.index
        ]
    )
    df.reset_index().to_csv(f'data/csv/{table_name}.csv')

    if pbar is not None:
        pbar.progress(20)


async def update_confrontos_or_mandos_multiple_rounds(rodadas: list[int]):
    for rodada in tqdm(rodadas):
        await asyncio.gather(
            update_confrontos_or_mandos('confrontos', rodada=rodada),
            update_confrontos_or_mandos('mandos', rodada=rodada),
        )


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--rounds', nargs='+', type=int)
    parser.add_argument('--all_rounds', action='store_true')
    args = parser.parse_args()

    if args.all_rounds and args.rounds is not None:
        raise ValueError('Cannot use --rounds with --all_rounds')

    elif args.all_rounds:
        print('Atualizando mandos e confrontos de todas as rodadas...')

        json = await get_page_json('https://api.cartolafc.globo.com/partidas')
        rodada_atual = str(json['rodada'])
        await update_confrontos_or_mandos_multiple_rounds(
            list(range(1, rodada_atual + 1))
        )

    elif args.rounds is not None:
        json = await get_page_json('https://api.cartolafc.globo.com/partidas')
        rodada_atual = str(json['rodada'])
        rodadas = [rodada for rodada in args.rounds if rodada <= rodada_atual]

        print(f'Atualizando mandos e confrontos das rodadas {rodadas}...')
        await update_confrontos_or_mandos_multiple_rounds(rodadas)

    else:
        print('Atualizando mandos e confrontos da rodada atual...')

        await asyncio.gather(
            update_confrontos_or_mandos('confrontos'),
            update_confrontos_or_mandos('mandos'),
        )


if __name__ == '__main__':
    asyncio.run(main())
