import asyncio
import sys

import pandas as pd
import numpy as np

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


async def update_confrontos_or_mandos(table_name: str, pbar=None):
    df = pd.read_csv(f'data/csv/{table_name}.csv', index_col=0).set_index('clube_id')

    json = await get_page_json('https://api.cartolafc.globo.com/partidas')
    rodada_atual = str(json['rodada'])
    partidas_rodada = pd.DataFrame(json['partidas'])

    await asyncio.gather(
        *[
            asyncio.to_thread(
                update_clube, clube, df, rodada_atual, partidas_rodada, table_name
            )
            for clube in df.index
        ]
    )
    df.reset_index().to_csv(f'data/csv/{table_name}.csv')

    if pbar is not None:
        pbar.progress(20)


if __name__ == '__main__':
    asyncio.run(update_confrontos_or_mandos('confrontos'))
    asyncio.run(update_confrontos_or_mandos('mandos'))
