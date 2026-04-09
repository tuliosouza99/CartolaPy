from typing import Literal

import pandas as pd

from src.services.enums import Scout


def compute_pontos_conquistados_unified(
    pontuacoes_df: pd.DataFrame,
    rodada_min: int,
    rodada_max: int,
    is_mandante: Literal["geral", "mandante", "visitante"],
    posicao_id: int,
    status_ids: list[int] | None = None,
    scout: str | None = None,
    scout_ascending: bool = False,
) -> pd.DataFrame:
    scout_cols = Scout.as_list()

    pont_filtered = (
        pontuacoes_df.loc[
            (pontuacoes_df["rodada_id"] >= rodada_min)
            & (pontuacoes_df["rodada_id"] <= rodada_max)
            & (pontuacoes_df["posicao_id"] == posicao_id)
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

    if status_ids is not None and len(status_ids) > 0:
        pont_filtered = pont_filtered.loc[pont_filtered["status_id"].isin(status_ids)]

    if pont_filtered.empty:
        return pd.DataFrame(
            columns=[
                "clube_id",
                "media_conquistada",
                "media_conquistada_basica",
                "total_jogos",
                "scouts",
                "scout_contributions",
                "total_points",
            ]
        )

    scout_value_series = pd.Series(
        {s: Scout.get_value(s) for s in scout_cols}, index=scout_cols
    )

    pont_agg = (
        pont_filtered.groupby("clube_id")
        .agg(
            media_conquistada=("pontuacao", "mean"),
            media_conquistada_basica=("pontuacao_basica", "mean"),
            total_jogos=("rodada_id", "nunique"),
            total_atletas=("atleta_id", "nunique"),
            **{scout: (scout, "sum") for scout in scout_cols},
        )
        .reset_index()
        .assign(
            clube_id=lambda df_: pd.to_numeric(df_["clube_id"], errors="coerce").astype(
                "Int64"
            ),
        )
    )

    scout_sums = pont_agg[scout_cols].fillna(0)
    scout_pts = scout_sums.mul(scout_value_series, axis=1)
    total_points = scout_pts.sum(axis=1)

    scout_avgs = scout_sums.div(pont_agg["total_jogos"], axis=0).div(
        pont_agg["total_atletas"].where(pont_agg["total_atletas"] > 0, 1), axis=0
    )

    scout_avg_pts = scout_avgs.mul(scout_value_series, axis=1)
    avg_total_points = scout_avg_pts.sum(axis=1)

    long = (
        pd.concat(
            [
                scout_avgs.stack().rename("raw_sum"),
                scout_avg_pts.stack().rename("points_contribution"),
            ],
            axis=1,
        )
        .reset_index()
        .rename(columns={"level_0": "row_idx", "level_1": "scout"})
        .query("raw_sum != 0")
        .assign(
            total_raw=lambda df_: df_["row_idx"].map(avg_total_points),
            raw_sum=lambda df_: df_["raw_sum"].round(2),
            points_contribution=lambda df_: df_["points_contribution"].round(2),
            percentage=lambda df_: (
                df_["points_contribution"]
                .div(df_["total_raw"])
                .mul(100)
                .round(1)
                .where(df_["total_raw"] != 0, 0.0)
            ),
        )
    )

    contributions_by_row = long.groupby("row_idx").apply(
        lambda g: g.set_index("scout")[
            ["raw_sum", "points_contribution", "percentage"]
        ].to_dict(orient="index"),
        include_groups=False,
    )

    scout_contributions_list = [
        contributions_by_row.iloc[i] if i in contributions_by_row.index else None
        for i in range(len(pont_agg))
    ]

    result = pont_agg.assign(
        media_conquistada=lambda df_: df_["media_conquistada"].round(2),
        media_conquistada_basica=lambda df_: df_["media_conquistada_basica"].round(2),
        scouts=lambda df_: scout_avgs.round(2).to_dict(orient="records"),
        scout_contributions=scout_contributions_list,
        total_points=total_points.values,
    )

    if scout is not None and scout in Scout.as_list():
        result = result.sort_values(
            by=scout, ascending=scout_ascending, na_position="last"
        )

    return result
