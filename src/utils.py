import json
from collections.abc import Mapping

import aiohttp
import aiofiles
import numpy as np
import pandas as pd

from src.enums import Scout


async def get_page_json(url: str) -> dict:
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            page_json = await response.json()

    return page_json


async def load_dict_async(name: str) -> dict[int, str]:
    async with aiofiles.open(f'data/json/{name}.json', 'r') as f:
        json_str = await f.read()

    json_dict = json.loads(json_str)
    return {int(k): v for k, v in json_dict.items()}


def load_dict(name: str) -> dict[int, str]:
    with open(f'data/json/{name}.json', 'r') as f:
        json_str = f.read()

    json_dict = json.loads(json_str)
    return {int(k): v for k, v in json_dict.items()}


def create_mando_dict(mandos_df: pd.DataFrame, mando_flag: int) -> dict[int, list[str]]:
    rodadas_mando_list = (
        mandos_df.eq(mando_flag).dot(mandos_df.columns + ',').str.rstrip(',')
    )
    rodadasMandoList = [mandosClube.split(',') for mandosClube in rodadas_mando_list]

    return dict(zip(mandos_df.index, rodadasMandoList))


def get_pontuacoes_mando(
    df: pd.DataFrame,
    partidas_mando_dict: dict[int, list[str]],
    clube_id: int,
    row_pontuacoes: tuple,
    row_scouts: tuple,
) -> pd.DataFrame:
    pontuacoes = [
        getattr(row_pontuacoes, f'round_{rodada}')
        for rodada in partidas_mando_dict[clube_id]
        if rodada != '' and ~np.isnan(getattr(row_pontuacoes, f'round_{rodada}'))
    ]
    pontuacoes_basicas = [
        getattr(row_scouts, f'round_{rodada}')
        for rodada in partidas_mando_dict[clube_id]
        if rodada != '' and ~np.isnan(getattr(row_scouts, f'round_{rodada}'))
    ]

    # Clube/Atleta atuou em alguma partida como Mandante/Visitante
    if len(pontuacoes) > 0:
        df.at[row_pontuacoes[0], 'Média'] = np.mean(pontuacoes)
        df.at[row_pontuacoes[0], 'Média Básica'] = np.mean(pontuacoes_basicas)
        df.at[row_pontuacoes[0], 'Desvio Padrão'] = np.std(pontuacoes)
        df.at[row_pontuacoes[0], 'Jogos'] = len(pontuacoes)

    return df


def atletas_clean_and_filter(
    atletas_df: pd.DataFrame,
    clubes: list[str],
    posicoes: list[str],
    status: list[str],
    min_jogos: int,
    precos: tuple[int, int],
) -> pd.DataFrame:
    clubes_dict, status_dict, posicoes_dict = (
        load_dict('clubes'),
        load_dict('status'),
        load_dict('posicoes'),
    )

    query = f'(Preço >= {precos[0]}) & (Preço <= {precos[1]}) & (Jogos >= {min_jogos})'
    if len(clubes) > 0:
        query += f' & (Clube in {clubes})'
    if len(posicoes) > 0:
        query += f' & (Posição in {posicoes})'
    if len(status) > 0:
        query += f' & (Status in {status})'

    return (
        atletas_df.assign(
            **{
                'clube_id': atletas_df['clube_id'].map(clubes_dict),
                'status_id': atletas_df['status_id'].map(status_dict),
                'posicao_id': atletas_df['posicao_id'].map(posicoes_dict),
            }
        )
        .loc[
            :,
            [
                'apelido',
                'clube_id',
                'posicao_id',
                'status_id',
                'preco_num',
                'Média',
                'Média Básica',
                'Desvio Padrão',
                'Jogos',
            ],
        ]
        .rename(
            columns={
                'apelido': 'Nome',
                'clube_id': 'Clube',
                'posicao_id': 'Posição',
                'status_id': 'Status',
                'preco_num': 'Preço',
            },
        )
        .query(query)
    )


def plot_df(df: pd.DataFrame, col: list, format: dict) -> pd.DataFrame.style:
    return (
        df.sort_values(by=col[0], ascending=False)
        .reset_index(drop=True)
        .style.background_gradient(cmap='YlGn', subset=col)
        .format(format)
    )


def color_status(status: str) -> str:
    if status == 'Provável':
        color = 'limegreen'
    elif status == 'Dúvida':
        color = 'gold'
    else:
        color = 'indianred'

    return f'color: {color}'


def get_basic_points(scouts: dict | float | None) -> float:
    if not isinstance(scouts, Mapping) and (scouts is None or np.isnan(scouts)):
        return np.nan

    valid_scouts = {
        k: v for k, v in scouts.items() if k in Scout.as_basic_scouts_list()
    }

    return sum(
        [
            v * getattr(Scout, k).value['value'] if v is not None else 0
            for k, v in valid_scouts.items()
        ]
    )
