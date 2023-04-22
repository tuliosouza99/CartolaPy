import asyncio
import sys

import pandas as pd

if './' not in sys.path:
    sys.path.append('./')

from src.utils import get_page_json


async def update_atletas(pbar=None):
    json = await get_page_json('https://api.cartolafc.globo.com/atletas/mercado')

    pd.DataFrame(json['atletas']).drop(
        columns=[
            'scout',
            'pontos_num',
            'variacao_num',
            'media_num',
            'jogos_num',
            'slug',
            'apelido_abreviado',
            'gato_mestre',
            'nome',
            'foto',
        ]
    ).sort_values(by=['atleta_id']).reset_index(drop=True).to_csv(
        'data/csv/atletas.csv'
    )

    if pbar is not None:
        pbar.progress(20)

if __name__ == '__main__':
    asyncio.run(update_atletas())
