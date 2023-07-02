import pandas as pd

from src.utils import get_page_json


async def update_atletas():
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
            'minimo_para_valorizar',
            'nome',
            'foto',
        ]
    ).sort_values(by=['atleta_id']).reset_index(drop=True).to_csv(
        'data/csv/atletas.csv'
    )
