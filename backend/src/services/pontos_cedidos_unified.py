from typing import Literal

import pandas as pd

from src.services.enums import Scout


def compute_pontos_cedidos_unified(
    pontos_cedidos_df: pd.DataFrame,
    rodada_min: int,
    rodada_max: int,
    is_mandante: Literal["geral", "mandante", "visitante"],
    posicao_id: int,
) -> pd.DataFrame:
    scout_cols = Scout.as_list()

    pont_filtered = (
        pontos_cedidos_df.loc[
            (pontos_cedidos_df["rodada_id"] >= rodada_min)
            & (pontos_cedidos_df["rodada_id"] <= rodada_max)
            & (pontos_cedidos_df["posicao_id"] == posicao_id)
        ]
        .pipe(
            lambda df_: (
                df_.loc[df_["is_mandante"]]
                if is_mandante == "mandante"
                else df_.loc[~df_["is_mandante"]]
                if is_mandante == "visitante"
                else df_
            )
        )
        .copy()
    )

    if pont_filtered.empty:
        return pd.DataFrame(
            columns=[
                "clube_id",
                "media_cedida",
                "media_cedida_basica",
                "total_jogos",
                *scout_cols,
            ]
        )

    pont_agg = (
        pont_filtered.groupby("clube_id")
        .agg(
            media_cedida=("pontuacao", "mean"),
            media_cedida_basica=("pontuacao_basica", "mean"),
            total_jogos=("rodada_id", "nunique"),
            **{scout: (scout, "mean") for scout in scout_cols},
        )
        .reset_index()
        .assign(
            clube_id=lambda df_: pd.to_numeric(df_["clube_id"], errors="coerce").astype(
                "Int64"
            ),
            media_cedida=lambda df_: df_["media_cedida"].round(2),
            media_cedida_basica=lambda df_: df_["media_cedida_basica"].round(2),
            scouts=lambda df_: df_[scout_cols].round(2).to_dict(orient="records"),
        )
    )

    return pont_agg
