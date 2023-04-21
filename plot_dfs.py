import json
import aiofiles

import numpy as np
import pandas as pd
import streamlit as st


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
    row: tuple,
) -> pd.DataFrame:
    pontuacoes = [
        row[int(rodada)]
        for rodada in partidas_mando_dict[clube_id]
        if rodada != '' and ~np.isnan(row[int(rodada)])
    ]

    # Clube/Atleta atuou em alguma partida como Mandante/Visitante
    if len(pontuacoes) > 0:
        df.at[row[0], 'Média'] = np.mean(pontuacoes)
        df.at[row[0], 'Desvio Padrão'] = np.std(pontuacoes)
        df.at[row[0], 'Jogos'] = len(pontuacoes)

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
                'minimo_para_valorizar',
                'Média',
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
                'minimo_para_valorizar': 'Val. com',
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


@st.cache_resource
def plot_atletas_movel(
    atletas_df: pd.DataFrame,
    clubes: list[str],
    posicoes: list[str],
    status: list[str],
    min_jogos: int,
    precos: tuple[int, int],
    rodadas: tuple[int, int],
) -> pd.DataFrame.style:
    pontuacoes_df = (
        pd.read_csv('data/csv/pontuacoes.csv', index_col=0)
        .set_index('atleta_id')
        .loc[:, str(rodadas[0]) : str(rodadas[1])]
    )

    return (
        atletas_df.assign(
            **{
                'Média': np.nanmean(np.array(pontuacoes_df), axis=1, keepdims=True),
                'Desvio Padrão': np.nanstd(
                    np.array(pontuacoes_df), axis=1, keepdims=True
                ),
                'Jogos': np.count_nonzero(
                    ~np.isnan(pontuacoes_df), axis=1, keepdims=True
                ),
            }
        )
        .dropna(subset=['Média'])
        .pipe(atletas_clean_and_filter, clubes, posicoes, status, min_jogos, precos)
        .pipe(
            plot_df,
            ['Média'],
            {
                'Preço': '{:.2f} C$',
                'Val. com': '{:.2f}',
                'Média': '{:.2f}',
                'Desvio Padrão': '{:.2f}',
            },
        )
        .applymap(color_status, subset=['Status'])
    )


@st.cache_resource
def plot_atletas_mando(
    atletas_df: pd.DataFrame,
    rodada_inicial: int,
    rodada_atual: int,
    clubes: list[str],
    posicoes: list[str],
    status: list[str],
    min_jogos: int,
    precos: tuple[int, int],
    mando_flag: int,
) -> pd.DataFrame.style:
    pontuacoes_df = (
        pd.read_csv('data/csv/pontuacoes.csv', index_col=0)
        .set_index('atleta_id')
        .loc[:, str(rodada_inicial) : str(rodada_atual)]
    )
    mandos_df = (
        pd.read_csv('data/csv/mandos.csv', index_col=0)
        .set_index('clube_id')
        .loc[:, str(rodada_inicial) : str(rodada_atual)]
    )
    rodadas_mando_dict = create_mando_dict(mandos_df, mando_flag)

    atletas_df = atletas_df.assign(
        **{
            'Média': np.nan,
            'Desvio Padrão': np.nan,
            'Jogos': np.nan,
        }
    )

    for row in pontuacoes_df.itertuples():
        clube_id = atletas_df.at[row[0], 'clube_id']
        atletas_df = get_pontuacoes_mando(atletas_df, rodadas_mando_dict, clube_id, row)

    return (
        atletas_df.dropna(subset=['Média'])
        .pipe(
            atletas_clean_and_filter,
            clubes,
            posicoes,
            status,
            min_jogos,
            precos,
        )
        .pipe(
            plot_df,
            ['Média'],
            {
                'Preço': '{:.2f} C$',
                'Val. com': '{:.2f}',
                'Média': '{:.2f}',
                'Desvio Padrão': '{:.2f}',
                'Jogos': '{:.0f}',
            },
        )
        .applymap(color_status, subset=['Status'])
    )


@st.cache_resource
def plot_pontos_cedidos_movel(
    pontos_cedidos_posicao: pd.DataFrame, rodadas: tuple
) -> pd.DataFrame.style:
    pontos_cedidos_posicao = pontos_cedidos_posicao.loc[
        :, str(rodadas[0]) : str(rodadas[1])
    ]

    return (
        pd.DataFrame(load_dict('clubes').values(), columns=['Clube'])
        .assign(
            **{
                'Média': np.nanmean(
                    np.array(pontos_cedidos_posicao), axis=1, keepdims=True
                ),
                'Desvio Padrão': np.nanstd(
                    np.array(pontos_cedidos_posicao), axis=1, keepdims=True
                ),
                'Jogos': np.count_nonzero(
                    ~np.isnan(pontos_cedidos_posicao), axis=1, keepdims=True
                ),
            }
        )
        .dropna(subset=['Média'])
        .pipe(plot_df, ['Média'], {'Média': '{:.2f}', 'Desvio Padrão': '{:.2f}'})
    )


@st.cache_resource
def plot_pontos_cedidos_mando(
    pontos_cedidos_posicao: pd.DataFrame,
    rodada_inicial: int,
    rodada_atual: int,
    mando_flag: int,
) -> pd.DataFrame.style:
    pontos_cedidos_posicao = pontos_cedidos_posicao.loc[
        :, str(rodada_inicial) : str(rodada_atual)
    ]
    clubes_dict = load_dict('clubes')

    mandos_df = (
        pd.read_csv('data/csv/mandos.csv', index_col=0)
        .set_index('clube_id')
        .loc[:, str(rodada_inicial) : str(rodada_atual)]
    )
    rodadas_mando_dict = create_mando_dict(mandos_df, mando_flag)

    pontos_cedidos_plot = (
        pd.DataFrame(clubes_dict.keys(), columns=['Clube'])
        .set_index('Clube')
        .assign(**{'Média': np.nan, 'Desvio Padrão': np.nan, 'Jogos': np.nan})
    )

    for row, clube_id in zip(
        pontos_cedidos_posicao.itertuples(), rodadas_mando_dict.keys()
    ):
        pontos_cedidos_plot = get_pontuacoes_mando(
            pontos_cedidos_plot, rodadas_mando_dict, clube_id, row
        )

    return (
        pontos_cedidos_plot.dropna(subset=['Média'])
        .reset_index()
        .assign(Clube=lambda _df: _df['Clube'].map(clubes_dict))
        .pipe(
            plot_df,
            ['Média'],
            {'Média': '{:.2f}', 'Desvio Padrão': '{:.2f}', 'Jogos': '{:.0f}'},
        )
    )
