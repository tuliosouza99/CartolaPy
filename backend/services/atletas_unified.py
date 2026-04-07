import unicodedata
from typing import Literal

import pandas as pd

from backend.services.enums import Scout


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
    atletas_unique = atletas_df.sort_values(
        "rodada_id", ascending=False
    ).drop_duplicates(subset=["atleta_id"], keep="first")

    pont_filtered = pontuacoes_df[
        (pontuacoes_df["rodada_id"] >= rodada_min)
        & (pontuacoes_df["rodada_id"] <= rodada_max)
    ].copy()

    pont_filtered = pont_filtered.drop_duplicates(
        subset=["atleta_id", "rodada_id"], keep="last"
    )

    if is_mandante != "geral":
        confrontos_unique = confrontos_df.drop_duplicates(
            subset=["clube_id", "rodada_id"], keep="last"
        )
        pont_with_mando = pont_filtered.merge(
            confrontos_unique[["clube_id", "rodada_id", "is_mandante"]],
            on=["clube_id", "rodada_id"],
            how="left",
        )
        if is_mandante == "mandante":
            pont_with_mando = pont_with_mando[pont_with_mando["is_mandante"]]
        else:
            pont_with_mando = pont_with_mando[~pont_with_mando["is_mandante"]]
        pont_filtered = pont_with_mando.drop(columns=["is_mandante"])

    scout_cols = Scout.as_list()

    if pont_filtered.empty:
        pont_agg = pd.DataFrame(
            columns=["atleta_id", "media", "media_basica", "total_jogos", *scout_cols]
        )
    else:
        pont_agg = (
            pont_filtered.groupby("atleta_id")
            .agg(
                media=("pontuacao", "mean"),
                media_basica=("pontuacao_basica", "mean"),
                total_jogos=("pontuacao", "count"),
                **{scout: (scout, "sum") for scout in scout_cols},
            )
            .reset_index()
        )
        pont_agg["atleta_id"] = pd.to_numeric(
            pont_agg["atleta_id"], errors="coerce"
        ).astype("Int64")

    result = atletas_unique.merge(pont_agg, on="atleta_id", how="left")

    zero_cols = ["media", "media_basica", "total_jogos", *scout_cols]
    result[zero_cols] = result[zero_cols].fillna(0)

    result["media"] = result["media"].round(2)
    result["media_basica"] = result["media_basica"].round(2)
    result["total_jogos"] = result["total_jogos"].astype(int)

    result["scouts"] = result.apply(
        lambda row: {scout: int(row[scout]) for scout in scout_cols}, axis=1
    )

    if clubes_cache:
        result["clube_escudo"] = (
            result["clube_id"]
            .astype(str)
            .map(lambda x: clubes_cache.get(x, {}).get("escudos", {}).get("60x60", ""))
        )
    else:
        result["clube_escudo"] = ""

    if posicoes_cache:
        result["posicao_abreviacao"] = (
            result["posicao_id"]
            .astype(str)
            .map(lambda x: posicoes_cache.get(x, {}).get("abreviacao", "").upper())
        )
    else:
        result["posicao_abreviacao"] = ""

    if status_cache:
        status_map = {
            "Provável": "green",
            "Dúvida": "yellow",
        }
        result["status_nome"] = (
            result["status_id"]
            .astype(str)
            .map(lambda x: status_cache.get(x, {}).get("nome", ""))
        )
        result["status_cor"] = (
            result["status_id"]
            .astype(str)
            .map(
                lambda x: status_map.get(status_cache.get(x, {}).get("nome", ""), "red")
            )
        )
    else:
        result["status_nome"] = ""
        result["status_cor"] = "red"

    result["preco"] = result["preco_num"].apply(lambda x: f"C$ {x:.2f}")

    next_rodada = rodada_atual + 1

    def get_proximo_jogo(clube_id):
        if not proximo_jogo_cache:
            return {
                "mandante_escudo": "",
                "visitante_escudo": "",
                "mandante_id": 0,
                "visitante_id": 0,
                "rodada": next_rodada,
            }
        clube_id_str = str(clube_id)
        for match in proximo_jogo_cache:
            if (
                str(match["mandante_id"]) == clube_id_str
                or str(match["visitante_id"]) == clube_id_str
            ):
                return {
                    "mandante_escudo": match.get("mandante_escudo", ""),
                    "visitante_escudo": match.get("visitante_escudo", ""),
                    "mandante_id": match["mandante_id"],
                    "visitante_id": match["visitante_id"],
                    "rodada": next_rodada,
                }
        return {
            "mandante_escudo": "",
            "visitante_escudo": "",
            "mandante_id": 0,
            "visitante_id": 0,
            "rodada": next_rodada,
        }

    result["proximo_jogo"] = result["clube_id"].apply(get_proximo_jogo)

    if search:
        normalized_search = normalize_string(search)
        result = result[
            result["apelido"].apply(
                lambda x: (
                    normalize_string(x).find(normalized_search) >= 0
                    if pd.notna(x)
                    else False
                )
            )
        ]

    if clube_ids is not None and len(clube_ids) > 0:
        result = result[result["clube_id"].isin(clube_ids)]

    if posicao_ids is not None and len(posicao_ids) > 0:
        result = result[result["posicao_id"].isin(posicao_ids)]

    if status_ids is not None and len(status_ids) > 0:
        result = result[result["status_id"].isin(status_ids)]

    if preco_min is not None:
        result = result[result["preco_num"] >= preco_min]

    if preco_max is not None:
        result = result[result["preco_num"] <= preco_max]

    return result


def compute_proximo_jogo(
    clube_id: int,
    rodada_atual: int,
    confrontos_df: pd.DataFrame,
    clubes_cache: dict | None,
) -> dict:
    next_rodada = rodada_atual + 1
    next_matches = confrontos_df[confrontos_df["rodada_id"] == next_rodada]

    match = next_matches[next_matches["clube_id"] == clube_id]

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
