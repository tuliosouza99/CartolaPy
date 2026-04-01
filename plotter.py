import numpy as np
import pandas as pd
import plotly.graph_objects as go
import streamlit as st

import src.utils as U

MAX_CACHE_ENTRIES = 3


def plot_df(df: pd.DataFrame, col: list, format: dict, drop_index: bool = False):
    return (
        df.sort_values(by=col[0], ascending=False)
        .reset_index(drop=drop_index)
        .rename(columns={"atleta_id": "ID"})
        .style.background_gradient(cmap="YlGn", subset=col)
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
    pontuacoes_df = pd.read_csv("data/csv/pontuacoes.csv")
    pontuacoes_df = pontuacoes_df[
        (pontuacoes_df["rodada"] >= rodadas[0])
        & (pontuacoes_df["rodada"] <= rodadas[1])
    ]

    scouts_df = pd.read_parquet("data/parquet/scouts.parquet")
    scouts_df = scouts_df[
        (scouts_df["rodada"] >= rodadas[0]) & (scouts_df["rodada"] <= rodadas[1])
    ]
    scouts_df = scouts_df.assign(
        pontuacao_basica=scouts_df["scout"].apply(U.get_basic_points)
    )

    pontuacoes_agg = (
        pontuacoes_df.groupby("atleta_id")["pontuacao"]
        .agg(["mean", "std", "count"])
        .rename(columns={"mean": "Média", "std": "Desvio Padrão", "count": "Jogos"})
    )

    scouts_agg = (
        scouts_df.groupby("atleta_id")["pontuacao_basica"].mean().rename("Média Básica")
    )

    atletas_df = atletas_df.join(pontuacoes_agg).join(scouts_agg)
    atletas_df = atletas_df.dropna(subset=["Média"]).pipe(
        U.atletas_clean_and_filter, clubes, posicoes, status, min_jogos, precos
    )

    return (
        dict(zip(atletas_df.index.to_series(), atletas_df["Nome"])),
        atletas_df.pipe(
            plot_df,
            ["Média"],
            {
                "Preço": "{:.2f} C$",
                "Média": "{:.2f}",
                "Média Básica": "{:.2f}",
                "Desvio Padrão": "{:.2f}",
            },
        ).applymap(U.color_status, subset=["Status"]),
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
    pontuacoes_df = pd.read_csv("data/csv/pontuacoes.csv")
    pontuacoes_df = pontuacoes_df[
        (pontuacoes_df["rodada"] >= rodadas[0])
        & (pontuacoes_df["rodada"] <= rodadas[1])
    ]

    scouts_df = pd.read_parquet("data/parquet/scouts.parquet")
    scouts_df = scouts_df[
        (scouts_df["rodada"] >= rodadas[0]) & (scouts_df["rodada"] <= rodadas[1])
    ]

    mandatos_df = pd.read_csv("data/csv/mandos.csv")
    mandatos_df = mandatos_df[
        (mandatos_df["rodada"] >= rodadas[0]) & (mandatos_df["rodada"] <= rodadas[1])
    ]
    rodadas_mando_dict = U.create_mando_dict(mandatos_df, mando_flag)

    atletas_df = atletas_df.assign(
        **{
            "Média": np.nan,
            "Média Básica": np.nan,
            "Desvio Padrão": np.nan,
            "Jogos": np.nan,
        }
    )

    for atleta_id in pontuacoes_df["atleta_id"].unique():
        clube_id = atletas_df.at[atleta_id, "clube_id"]
        if clube_id in rodadas_mando_dict:
            atletas_df = U.get_pontuacoes_mando_long(
                atletas_df,
                pontuacoes_df,
                scouts_df,
                rodadas_mando_dict,
                atleta_id,
                clube_id,
            )

    atletas_df = atletas_df.dropna(subset=["Média"]).pipe(
        U.atletas_clean_and_filter,
        clubes,
        posicoes,
        status,
        min_jogos,
        precos,
    )

    return (
        dict(zip(atletas_df.index.to_series(), atletas_df["Nome"])),
        atletas_df.pipe(
            plot_df,
            ["Média"],
            {
                "Preço": "{:.2f} C$",
                "Média": "{:.2f}",
                "Média Básica": "{:.2f}",
                "Desvio Padrão": "{:.2f}",
                "Jogos": "{:.0f}",
            },
        ).applymap(U.color_status, subset=["Status"]),
    )


@st.cache_resource(max_entries=MAX_CACHE_ENTRIES)
def plot_pontos_cedidos_geral(
    pontos_cedidos_df: pd.DataFrame, rodadas: tuple, posicao_id: int
):
    pontos_cedidos_df = pontos_cedidos_df[
        (pontos_cedidos_df["posicao_id"] == posicao_id)
        & (pontos_cedidos_df["rodada"] >= rodadas[0])
        & (pontos_cedidos_df["rodada"] <= rodadas[1])
    ]

    clubes_dict = U.load_dict("clubes")

    return (
        pd.DataFrame(clubes_dict.values(), columns=["Clube"])
        .assign(Clube_id=list(clubes_dict.keys()))
        .merge(
            pontos_cedidos_df.groupby("clube_id")["pontos_cedidos"]
            .agg(["mean", "std", "count"])
            .rename(columns={"mean": "Média", "std": "Desvio Padrão", "count": "Jogos"})
            .reset_index(),
            left_on="Clube_id",
            right_on="clube_id",
            how="left",
        )
        .dropna(subset=["Média"])
        .pipe(
            plot_df,
            ["Média"],
            {"Média": "{:.2f}", "Desvio Padrão": "{:.2f}"},
            drop_index=True,
        )
    )


@st.cache_resource(max_entries=MAX_CACHE_ENTRIES)
def plot_pontos_cedidos_mando(
    pontos_cedidos_df: pd.DataFrame,
    rodadas: tuple,
    mando_flag: int,
    posicao_id: int,
):
    pontos_cedidos_df = pontos_cedidos_df[
        (pontos_cedidos_df["posicao_id"] == posicao_id)
        & (pontos_cedidos_df["rodada"] >= rodadas[0])
        & (pontos_cedidos_df["rodada"] <= rodadas[1])
    ]

    clubes_dict = U.load_dict("clubes")

    mandatos_df = pd.read_csv("data/csv/mandos.csv")
    mandatos_df = mandatos_df[
        (mandatos_df["rodada"] >= rodadas[0]) & (mandatos_df["rodada"] <= rodadas[1])
    ]
    rodadas_mando_dict = U.create_mando_dict(mandatos_df, mando_flag)

    pontos_cedidos_plot = (
        pd.DataFrame(clubes_dict.keys(), columns=["Clube_id"])
        .set_index("Clube_id")
        .assign(**{"Média": np.nan, "Desvio Padrão": np.nan, "Jogos": np.nan})
    )

    for clube_id in rodadas_mando_dict.keys():
        home_rodadas = [int(r) for r in rodadas_mando_dict[clube_id]]
        club_pontos = pontos_cedidos_df[
            (pontos_cedidos_df["clube_id"] == clube_id)
            & (pontos_cedidos_df["rodada"].isin(home_rodadas))
        ]["pontos_cedidos"].dropna()

        pontuacoes_list = club_pontos.tolist()

        if len(pontuacoes_list) > 0:
            pontos_cedidos_plot.at[clube_id, "Média"] = np.mean(pontuacoes_list)
            pontos_cedidos_plot.at[clube_id, "Desvio Padrão"] = np.std(pontuacoes_list)
            pontos_cedidos_plot.at[clube_id, "Jogos"] = len(pontuacoes_list)

    return (
        pontos_cedidos_plot.dropna(subset=["Média"])
        .reset_index()
        .assign(Clube=lambda _df: _df["Clube_id"].map(clubes_dict))
        .pipe(
            plot_df,
            ["Média"],
            {"Média": "{:.2f}", "Desvio Padrão": "{:.2f}", "Jogos": "{:.0f}"},
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
    scouts_df = pd.read_parquet("data/parquet/scouts.parquet")
    scouts_df = scouts_df[
        (scouts_df["rodada"] >= rodadas[0]) & (scouts_df["rodada"] <= rodadas[1])
    ]

    if mando_flag is not None:
        mandatos_df = pd.read_csv("data/csv/mandos.csv")
        mandatos_df = mandatos_df[
            (mandatos_df["rodada"] >= rodadas[0])
            & (mandatos_df["rodada"] <= rodadas[1])
        ]
        rodadas_mando_dict = U.create_mando_dict(mandatos_df, mando_flag)
        rodadas_atletas = [
            rodadas_mando_dict.get(atletas_df.at[int(atleta_id), "clube_id"], [])
            for atleta_id in atletas_ids
        ]
    else:
        rodadas_atletas = [list(range(rodadas[0], rodadas[1] + 1)) for _ in atletas_ids]

    df = (
        pd.DataFrame(
            [
                (
                    pd.DataFrame(
                        scouts_df[
                            (scouts_df["atleta_id"] == int(atleta_id))
                            & (scouts_df["rodada"].isin(rodadas_atleta))
                        ]["scout"]
                        .dropna()
                        .tolist()
                    )
                    .dropna(axis="columns", how="all")
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
                hovertemplate="<b>%{meta[0]}</b><br>Scout: %{theta}<br>Total: %{r}<extra></extra>",
            )
        )

    fig.update_layout(
        polar=dict(radialaxis=dict(visible=True)),
        template="plotly_dark",
        width=960,
        height=720,
    )

    return fig
