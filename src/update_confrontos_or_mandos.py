import asyncio
import sys
from tqdm import tqdm

import pandas as pd
import numpy as np

if './' not in sys.path:
    sys.path.append('./')

from src.utils import get_page_json


async def update_confrontos_or_mandos(table_name: str):
    df = pd.read_csv(f'data/csv/{table_name}.csv', index_col=0).set_index('clube_id')

    json = await get_page_json('https://api.cartolafc.globo.com/partidas')
    rodada_atual = str(json['rodada'])
    partidas_rodada = pd.DataFrame(json['partidas'])

    # Itera por todos os clubes na tabela
    for clube in tqdm(df.index, desc=f'Updating {table_name}'):
        # Clube é mandante na rodada
        if clube in partidas_rodada['clube_casa_id'].values:
            idx = int(np.where(partidas_rodada['clube_casa_id'] == clube)[0])

            if partidas_rodada.at[idx, 'valida']:
                if table_name == 'mandos':
                    # 1 para mandantes na tabela Mandos
                    df.loc[clube, rodada_atual] = 1
                else:
                    # id do clube adversário na tabela Confrontos
                    df.loc[clube, rodada_atual] = partidas_rodada.at[
                        idx, 'clube_visitante_id'
                    ]

        # Clube é visitante na rodada
        else:
            idx = int(np.where(partidas_rodada['clube_visitante_id'] == clube)[0])

            if partidas_rodada.at[idx, 'valida']:
                if table_name == 'mandos':
                    # 0 para visitantes na tabela Mandos
                    df.loc[clube, rodada_atual] = 0
                else:
                    # id do clube adversário na tabela Confrontos
                    df.loc[clube, rodada_atual] = partidas_rodada.at[
                        idx, 'clube_casa_id'
                    ]

    df.reset_index().to_csv(f'data/csv/{table_name}.csv')


if __name__ == '__main__':
    asyncio.run(update_confrontos_or_mandos('confrontos'))
    asyncio.run(update_confrontos_or_mandos('mandos'))
