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

    pont_filtered = pontos_cedidos_df[
        (pontos_cedidos_df["rodada_id"] >= rodada_min)
        & (pontos_cedidos_df["rodada_id"] <= rodada_max)
    ].copy()

    pont_filtered = pont_filtered[pont_filtered["posicao_id"] == posicao_id]

    if is_mandante == "mandante":
        pont_filtered = pont_filtered[~pont_filtered["is_mandante"]]
    elif is_mandante == "visitante":
        pont_filtered = pont_filtered[pont_filtered["is_mandante"]]

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
    )

    pont_agg["clube_id"] = pd.to_numeric(pont_agg["clube_id"], errors="coerce").astype(
        "Int64"
    )

    pont_agg["media_cedida"] = pont_agg["media_cedida"].round(2)
    pont_agg["media_cedida_basica"] = pont_agg["media_cedida_basica"].round(2)

    pont_agg["scouts"] = pont_agg.apply(
        lambda row: {scout: round(row[scout], 2) for scout in scout_cols}, axis=1
    )

    return pont_agg
