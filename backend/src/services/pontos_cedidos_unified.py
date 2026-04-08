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
                "scouts",
                "scout_contributions",
                "total_points",
            ]
        )

    pont_agg = pont_filtered.groupby("clube_id").agg(
        media_cedida=("pontuacao", "mean"),
        media_cedida_basica=("pontuacao_basica", "mean"),
        total_jogos=("rodada_id", "nunique"),
        **{scout: (scout, "sum") for scout in scout_cols},
    )

    pont_agg = pont_agg.reset_index()
    pont_agg["clube_id"] = pd.to_numeric(pont_agg["clube_id"], errors="coerce").astype(
        "Int64"
    )

    scout_values = {scout: Scout.get_value(scout) for scout in scout_cols}

    total_points_raw = {}
    scout_contributions = {}

    for idx, row in pont_agg.iterrows():
        clube_id = row["clube_id"]
        total_raw = 0.0
        contributions = {}

        for scout in scout_cols:
            scout_sum = row[scout]
            if pd.notna(scout_sum) and scout_sum != 0:
                scout_pts = scout_sum * scout_values[scout]
                total_raw += scout_pts
                contributions[scout] = {
                    "raw_sum": round(scout_sum, 2),
                    "points_contribution": round(scout_pts, 2),
                }

        total_points_raw[clube_id] = total_raw
        scout_contributions[clube_id] = contributions

    for idx, row in pont_agg.iterrows():
        clube_id = row["clube_id"]
        total_raw = total_points_raw[clube_id]
        contributions = scout_contributions[clube_id]

        for scout in contributions:
            if total_raw != 0:
                contributions[scout]["percentage"] = round(
                    (contributions[scout]["points_contribution"] / total_raw) * 100, 1
                )
            else:
                contributions[scout]["percentage"] = 0.0

    pont_agg["scouts"] = pont_agg[scout_cols].round(2).to_dict(orient="records")
    pont_agg["scout_contributions"] = pont_agg["clube_id"].map(scout_contributions)
    pont_agg["total_points"] = pont_agg["clube_id"].map(total_points_raw)
    pont_agg["media_cedida"] = pont_agg["media_cedida"].round(2)
    pont_agg["media_cedida_basica"] = pont_agg["media_cedida_basica"].round(2)

    return pont_agg
