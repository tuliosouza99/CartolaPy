import unicodedata
from typing import Literal

import pandas as pd

from src.services.enums import Scout


def normalize_string(s: str) -> str:
    if not isinstance(s, str):
        return s
    nfkd = unicodedata.normalize("NFKD", s)
    return "".join(c for c in nfkd if not unicodedata.combining(c)).lower()


def compute_atletas_unified(
    atletas_df: pd.DataFrame,
    pontuacoes_df: pd.DataFrame,
    confrontos_df: pd.DataFrame,
    rodada_min: int,
    rodada_max: int,
    is_mandante: Literal["geral", "mandante", "visitante"],
    rodada_atual: int,
    clubes_cache: dict | None,
    posicoes_cache: dict | None,
    status_cache: dict | None,
    proximo_jogo_cache: dict | None = None,
    search: str | None = None,
    clube_ids: list[int] | None = None,
    posicao_ids: list[int] | None = None,
    status_ids: list[int] | None = None,
    preco_min: int | None = None,
    preco_max: int | None = None,
) -> pd.DataFrame:

    if clubes_cache is None:
        clubes_cache = {}
    if posicoes_cache is None:
        posicoes_cache = {}
    if status_cache is None:
        status_cache = {}
    if proximo_jogo_cache is None:
        proximo_jogo_cache = {}

    atletas_unique = atletas_df.sort_values(
        "rodada_id", ascending=False
    ).drop_duplicates(subset=["atleta_id"], keep="first")

    pont_filtered = (
        pontuacoes_df.loc[
            (pontuacoes_df["rodada_id"] >= rodada_min)
            & (pontuacoes_df["rodada_id"] <= rodada_max)
        ]
        .copy()
        .drop_duplicates(subset=["atleta_id", "rodada_id"], keep="last")
    )

    if is_mandante != "geral":
        confrontos_unique = confrontos_df.drop_duplicates(
            subset=["clube_id", "rodada_id"], keep="last"
        )
        pont_filtered = (
            pont_filtered.merge(
                confrontos_unique[["clube_id", "rodada_id", "is_mandante"]],
                on=["clube_id", "rodada_id"],
                how="left",
            )
            .pipe(
                lambda df_: (
                    df_.loc[df_["is_mandante"]]
                    if is_mandante == "mandante"
                    else df_.loc[~df_["is_mandante"]]
                )
            )
            .drop(columns=["is_mandante"])
        )

    scout_cols = Scout.as_list()
    pont_agg = pd.DataFrame(
        columns=["atleta_id", "media", "media_basica", "total_jogos", *scout_cols]
    )

    if not pont_filtered.empty:
        pont_agg = (
            pont_filtered.groupby("atleta_id")
            .agg(
                media=("pontuacao", "mean"),
                media_basica=("pontuacao_basica", "mean"),
                total_jogos=("pontuacao", "count"),
                **{scout: (scout, "sum") for scout in scout_cols},
            )
            .reset_index()
            .assign(
                atleta_id=lambda df_: pd.to_numeric(
                    df_["atleta_id"], errors="coerce"
                ).astype("Int64")
            )
        )

    raw_result = atletas_unique.merge(pont_agg, on="atleta_id", how="left")

    zero_cols = ["media", "media_basica", "total_jogos", *scout_cols]
    status_map = {
        "Provável": "green",
        "Dúvida": "yellow",
    }
    next_rodada = rodada_atual + 1
    default_jogo = {
        "mandante_escudo": "",
        "visitante_escudo": "",
        "mandante_id": 0,
        "visitante_id": 0,
        "rodada": next_rodada,
    }
    proximo_jogo_lookup = {}
    for match in proximo_jogo_cache:
        info = {
            "mandante_escudo": match.get("mandante_escudo", ""),
            "visitante_escudo": match.get("visitante_escudo", ""),
            "mandante_id": match["mandante_id"],
            "visitante_id": match["visitante_id"],
            "rodada": next_rodada,
        }
        proximo_jogo_lookup[int(match["mandante_id"])] = info
        proximo_jogo_lookup[int(match["visitante_id"])] = info

    result = raw_result.assign(
        **{col: raw_result[col].fillna(0) for col in zero_cols}
    ).assign(
        media=lambda df_: df_["media"].round(2),
        media_basica=lambda df_: df_["media_basica"].round(2),
        total_jogos=lambda df_: df_["total_jogos"].astype(int),
        scouts=lambda df_: df_[scout_cols].astype(int).to_dict(orient="records"),
        clube_escudo=lambda df_: (
            df_.loc[:, "clube_id"]
            .astype(str)
            .map(lambda x: clubes_cache.get(x, {}).get("escudos", {}).get("60x60", ""))
        ),
        posicao_abreviacao=lambda df_: (
            df_.loc[:, "posicao_id"]
            .astype(str)
            .map(lambda x: posicoes_cache.get(x, {}).get("abreviacao", "").upper())
        ),
        status_nome=lambda df_: (
            df_.loc[:, "status_id"]
            .astype(str)
            .map(lambda x: status_cache.get(x, {}).get("nome", ""))
        ),
        status_cor=lambda df_: (
            df_.loc[:, "status_id"]
            .astype(str)
            .map(
                lambda x: status_map.get(status_cache.get(x, {}).get("nome", ""), "red")
            )
        ),
        preco=lambda df_: "C$ " + df_["preco_num"].map("{:.2f}".format),
        proximo_jogo=lambda df_: df_["clube_id"].map(
            lambda x: proximo_jogo_lookup.get(x, default_jogo)
        ),
    )

    if search:
        normalized_search = normalize_string(search)
        result = result.loc[
            result["apelido"]
            .fillna("")
            .map(normalize_string)
            .str.contains(normalized_search, regex=False)
        ]

    if clube_ids is not None and len(clube_ids) > 0:
        result = result.loc[result["clube_id"].isin(clube_ids)]

    if posicao_ids is not None and len(posicao_ids) > 0:
        result = result.loc[result["posicao_id"].isin(posicao_ids)]

    if status_ids is not None and len(status_ids) > 0:
        result = result.loc[result["status_id"].isin(status_ids)]

    if preco_min is not None:
        result = result.loc[result["preco_num"] >= preco_min]

    if preco_max is not None:
        result = result.loc[result["preco_num"] <= preco_max]

    return result


def compute_proximo_jogo(
    clube_id: int,
    rodada_atual: int,
    confrontos_df: pd.DataFrame,
    clubes_cache: dict | None,
) -> dict:
    next_rodada = rodada_atual + 1
    next_matches = confrontos_df.loc[confrontos_df["rodada_id"] == next_rodada]

    match = next_matches.loc[next_matches["clube_id"] == clube_id]

    if match.empty:
        match = next_matches[next_matches["opponent_clube_id"] == clube_id]

    if match.empty:
        return {
            "mandante_escudo": "",
            "visitante_escudo": "",
            "mandante_id": 0,
            "visitante_id": 0,
            "rodada": next_rodada,
        }

    row = match.iloc[0]

    if row["clube_id"] == clube_id:
        mandante_id = clube_id
        visitante_id = row["opponent_clube_id"]
    else:
        mandante_id = row["clube_id"]
        visitante_id = clube_id

    mandante_escudo = (
        clubes_cache.get(str(mandante_id), {}).get("escudos", {}).get("60x60", "")
        if clubes_cache
        else ""
    )
    visitante_escudo = (
        clubes_cache.get(str(visitante_id), {}).get("escudos", {}).get("60x60", "")
        if clubes_cache
        else ""
    )

    return {
        "mandante_escudo": mandante_escudo,
        "visitante_escudo": visitante_escudo,
        "mandante_id": mandante_id,
        "visitante_id": visitante_id,
        "rodada": next_rodada,
    }
