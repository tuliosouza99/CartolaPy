import numpy as np
import pandas as pd
import plotly.graph_objects as go
import streamlit as st

import src.utils as U

MAX_CACHE_ENTRIES = 3


def plot_df(
    df: pd.DataFrame, col: list, format: dict, drop_index: bool = False
):
    return (
        df.sort_values(by=col[0], ascending=False)
        .reset_index(drop=drop_index)
        .rename(columns={'atleta_id': 'ID'})
        .style.background_gradient(cmap='YlGn', subset=col)
        .format(format)
    )


@st.cache_resource(max_entries=MAX_CACHE_ENTRIES)
def plot_atletas_geral(
    atletas_df: pd.DataFrame,
    clubes: list[str],
    posicoes: list[str],
    status: list[str],
    min_jogos: int,
    precos: tuple[int, int],
    rodadas: tuple[int, int],
):
    pontuacoes_df = (
        pd.read_csv('data/csv/pontuacoes.csv', index_col=0)
        .set_index('atleta_id')
        .loc[:, str(rodadas[0]) : str(rodadas[1])]
    )
    scouts_df = (
        pd.read_parquet('data/parquet/scouts.parquet')
        .set_index('atleta_id')
        .loc[:, str(rodadas[0]) : str(rodadas[1])]
        .assign(
            **{
                str(rodada): lambda df_, rodada_=rodada: list(
                    map(U.get_basic_points, df_[str(rodada_)])
                )
                for rodada in range(rodadas[0], rodadas[1] + 1)
            }
        )
    )

    atletas_df = (
        atletas_df.assign(
            **{
                'Média': np.nanmean(np.array(pontuacoes_df), axis=1, keepdims=True),
                'Média Básica': np.nanmean(np.array(scouts_df), axis=1, keepdims=True),
                'Desvio Padrão': np.nanstd(
                    np.array(pontuacoes_df), axis=1, keepdims=True
                ),
                'Jogos': np.count_nonzero(
                    ~np.isnan(pontuacoes_df), axis=1, keepdims=True
                ),
            }
        )
        .dropna(subset=['Média'])
        .pipe(U.atletas_clean_and_filter, clubes, posicoes, status, min_jogos, precos)
    )

    return (
        dict(zip(atletas_df.index.to_series(), atletas_df['Nome'])),
        atletas_df.pipe(
            plot_df,
            ['Média'],
            {
                'Preço': '{:.2f} C$',
                'Média': '{:.2f}',
                'Média Básica': '{:.2f}',
                'Desvio Padrão': '{:.2f}',
            },
        ).applymap(U.color_status, subset=['Status']),
    )


@st.cache_resource(max_entries=MAX_CACHE_ENTRIES)
def plot_atletas_mando(
    atletas_df: pd.DataFrame,
    clubes: list[str],
    posicoes: list[str],
    status: list[str],
    min_jogos: int,
    precos: tuple[int, int],
    rodadas: tuple[int, int],
    mando_flag: int,
):
    pontuacoes_df = (
        pd.read_csv('data/csv/pontuacoes.csv', index_col=0)
        .set_index('atleta_id')
        .loc[:, str(rodadas[0]) : str(rodadas[1])]
        .rename(lambda col: f'round_{col}', axis='columns')
    )
    scouts_df = (
        pd.read_parquet('data/parquet/scouts.parquet')
        .set_index('atleta_id')
        .loc[:, str(rodadas[0]) : str(rodadas[1])]
        .assign(
            **{
                str(rodada): lambda df_, rodada_=rodada: list(
                    map(U.get_basic_points, df_[str(rodada_)])
                )
                for rodada in range(rodadas[0], rodadas[1] + 1)
            }
        )
        .rename(lambda col: f'round_{col}', axis='columns')
    )
    mandos_df = (
        pd.read_csv('data/csv/mandos.csv', index_col=0)
        .set_index('clube_id')
        .loc[:, str(rodadas[0]) : str(rodadas[1])]
    )
    rodadas_mando_dict = U.create_mando_dict(mandos_df, mando_flag)

    atletas_df = atletas_df.assign(
        **{
            'Média': np.nan,
            'Média Básica': np.nan,
            'Desvio Padrão': np.nan,
            'Jogos': np.nan,
        }
    )

    for row_pontuacoes, row_scouts in zip(
        pontuacoes_df.itertuples(), scouts_df.itertuples()
    ):
        clube_id = atletas_df.at[row_pontuacoes[0], 'clube_id']
        atletas_df = U.get_pontuacoes_mando(
            atletas_df, rodadas_mando_dict, clube_id, row_pontuacoes, row_scouts
        )

    atletas_df = atletas_df.dropna(subset=['Média']).pipe(
        U.atletas_clean_and_filter,
        clubes,
        posicoes,
        status,
        min_jogos,
        precos,
    )

    return (
        dict(zip(atletas_df.index.to_series(), atletas_df['Nome'])),
        atletas_df.pipe(
            plot_df,
            ['Média'],
            {
                'Preço': '{:.2f} C$',
                'Média': '{:.2f}',
                'Média Básica': '{:.2f}',
                'Desvio Padrão': '{:.2f}',
                'Jogos': '{:.0f}',
            },
        ).applymap(U.color_status, subset=['Status']),
    )


@st.cache_resource(max_entries=MAX_CACHE_ENTRIES)
def plot_pontos_cedidos_geral(pontos_cedidos_posicao: pd.DataFrame, rodadas: tuple):
    pontos_cedidos_posicao = pontos_cedidos_posicao.loc[
        :, str(rodadas[0]) : str(rodadas[1])
    ]

    return (
        pd.DataFrame(U.load_dict('clubes').values(), columns=['Clube'])
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
        .pipe(
            plot_df,
            ['Média'],
            {'Média': '{:.2f}', 'Desvio Padrão': '{:.2f}'},
            drop_index=True,
        )
    )


@st.cache_resource(max_entries=MAX_CACHE_ENTRIES)
def plot_pontos_cedidos_mando(
    pontos_cedidos_posicao: pd.DataFrame,
    rodadas: tuple,
    mando_flag: int,
):
    pontos_cedidos_posicao = pontos_cedidos_posicao.loc[
        :, str(rodadas[0]) : str(rodadas[1])
    ].rename(lambda col: f'round_{col}', axis='columns')

    clubes_dict = U.load_dict('clubes')

    mandos_df = (
        pd.read_csv('data/csv/mandos.csv', index_col=0)
        .set_index('clube_id')
        .loc[:, str(rodadas[0]) : str(rodadas[1])]
    )
    rodadas_mando_dict = U.create_mando_dict(mandos_df, mando_flag)

    pontos_cedidos_plot = (
        pd.DataFrame(clubes_dict.keys(), columns=['Clube'])
        .set_index('Clube')
        .assign(**{'Média': np.nan, 'Desvio Padrão': np.nan, 'Jogos': np.nan})
    )

    for row, clube_id in zip(
        pontos_cedidos_posicao.itertuples(), rodadas_mando_dict.keys()
    ):
        pontos_cedidos_plot = U.get_pontuacoes_mando(
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
            drop_index=True,
        )
    )


@st.cache_resource(max_entries=MAX_CACHE_ENTRIES)
def plot_player_scouts(
    atletas_ids: list[str],
    index2name: dict,
    rodadas: tuple[int, int],
    atletas_df: pd.DataFrame | None = None,
    mando_flag: int | None = None,
):
    if mando_flag is not None:
        mandos_df = (
            pd.read_csv('data/csv/mandos.csv', index_col=0)
            .set_index('clube_id')
            .loc[:, str(rodadas[0]) : str(rodadas[1])]
        )
        rodadas_mando_dict = U.create_mando_dict(mandos_df, mando_flag)
        rodadas_atletas = [
            rodadas_mando_dict[atletas_df.at[int(atleta_id), 'clube_id']]
            for atleta_id in atletas_ids
        ]
    else:
        rodadas_atletas = [
            [str(rodada) for rodada in range(rodadas[0], rodadas[1] + 1)]
            for _ in atletas_ids
        ]

    df = (
        pd.DataFrame(
            [
                (
                    pd.DataFrame(
                        pd.read_parquet('data/parquet/scouts.parquet')
                        .set_index('atleta_id')
                        .loc[int(atleta_id), rodadas_atleta]
                        .dropna()
                        .tolist()
                    )
                    .dropna(axis='columns', how='all')
                    .sum()
                    .astype(int)
                    .to_dict()
                )
                for atleta_id, rodadas_atleta in zip(atletas_ids, rodadas_atletas)
            ]
        )
        .fillna(0)
        .assign(Nome=[index2name[int(atleta_id)] for atleta_id in atletas_ids])
    )

    fig = go.Figure()

    for row in df.itertuples(index=False):
        fig.add_trace(
            go.Scatterpolar(
                r=row[:-1],
                theta=df.columns[:-1],
                name=row[-1],
                meta=[row[-1]],
                hovertemplate='<b>%{meta[0]}</b><br>Scout: %{theta}<br>Total: %{r}<extra></extra>',
            )
        )

    fig.update_layout(
        polar=dict(radialaxis=dict(visible=True)),
        template='plotly_dark',
        width=960,
        height=720,
    )

    return fig
