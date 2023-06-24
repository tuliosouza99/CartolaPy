import numpy as np
import pandas as pd
import streamlit as st

import src.utils as U


@st.cache_resource
def plot_atletas_geral(
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

    return (
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
        .pipe(
            U.plot_df,
            ['Média'],
            {
                'Preço': '{:.2f} C$',
                'Média': '{:.2f}',
                'Média Básica': '{:.2f}',
                'Desvio Padrão': '{:.2f}',
            },
        )
        .applymap(U.color_status, subset=['Status'])
    )


@st.cache_resource
def plot_atletas_mando(
    atletas_df: pd.DataFrame,
    clubes: list[str],
    posicoes: list[str],
    status: list[str],
    min_jogos: int,
    precos: tuple[int, int],
    rodadas: tuple[int, int],
    mando_flag: int,
) -> pd.DataFrame.style:
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

    return (
        atletas_df.dropna(subset=['Média'])
        .pipe(
            U.atletas_clean_and_filter,
            clubes,
            posicoes,
            status,
            min_jogos,
            precos,
        )
        .pipe(
            U.plot_df,
            ['Média'],
            {
                'Preço': '{:.2f} C$',
                'Média': '{:.2f}',
                'Média Básica': '{:.2f}',
                'Desvio Padrão': '{:.2f}',
                'Jogos': '{:.0f}',
            },
        )
        .applymap(U.color_status, subset=['Status'])
    )


@st.cache_resource
def plot_pontos_cedidos_geral(
    pontos_cedidos_posicao: pd.DataFrame, rodadas: tuple
) -> pd.DataFrame.style:
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
            U.plot_df,
            ['Média'],
            {'Média': '{:.2f}', 'Desvio Padrão': '{:.2f}'},
            drop_index=True,
        )
    )


@st.cache_resource
def plot_pontos_cedidos_mando(
    pontos_cedidos_posicao: pd.DataFrame,
    rodadas: tuple,
    mando_flag: int,
) -> pd.DataFrame.style:
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
            U.plot_df,
            ['Média'],
            {'Média': '{:.2f}', 'Desvio Padrão': '{:.2f}', 'Jogos': '{:.0f}'},
            drop_index=True,
        )
    )


@st.cache_resource
def get_player_scouts(atleta_id: int, rodadas: tuple[int, int]):
    try:
        return (
            pd.DataFrame(
                pd.read_parquet('data/parquet/scouts.parquet')
                .set_index('atleta_id')
                .loc[atleta_id, str(rodadas[0]) : str(rodadas[1])]
                .dropna()
                .tolist()
            )
            .dropna(axis='columns', how='all')
            .sum()
            .astype(int)
            .to_dict()
        )
    except Exception:
        return {}
