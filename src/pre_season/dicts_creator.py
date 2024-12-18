import asyncio
import json
import os

import aiofiles
import pandas as pd

from src.utils import get_page_json


async def save_dict(dict_obj: dict, name: str):
    async with aiofiles.open(f'data/json/{name}.json', 'w') as f:
        await f.write(json.dumps(dict_obj, indent=4, ensure_ascii=False))


async def create_dicts():
    json_page = await get_page_json('https://api.cartola.globo.com/atletas/mercado')

    clubes_dict = (
        pd.DataFrame(json_page['clubes']).T.set_index('id')['abreviacao'].to_dict()
    )

    status_dict = pd.DataFrame(json_page['status']).T.set_index('id')['nome'].to_dict()

    posicoes_dict = (
        pd.DataFrame(json_page['posicoes'])
        .T.set_index('id')['abreviacao']
        .str.upper()
        .to_dict()
    )
    os.makedirs('data/json', exist_ok=True)

    await asyncio.gather(
        *[
            save_dict(dict_obj, name)
            for dict_obj, name in zip(
                (clubes_dict, status_dict, posicoes_dict),
                ('clubes', 'status', 'posicoes'),
            )
        ]
    )
