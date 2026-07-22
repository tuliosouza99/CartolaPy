from typing import Any, Literal

import pandas as pd

from .enums import Scout


def _safe_float(value: Any) -> float:
    return float(value) if pd.notna(value) else 0.0


def build_cartola_player_view(
    player: dict[str, Any],
    pontuacoes_df: pd.DataFrame,
    confrontos_df: pd.DataFrame,
    clubes_cache: dict[str, Any],
    posicoes_cache: dict[str, Any],
    status_cache: dict[str, Any],
    rodada_min: int,
    rodada_max: int,
    is_mandante: Literal["geral", "mandante", "visitante"],
) -> dict[str, Any]:
    atleta_id = int(player["atleta_id"])
    clube_id = int(player["clube_id"])
    posicao_id = int(player["posicao_id"])
    status_id = int(player["status_id"])

    filtered = pontuacoes_df.loc[
        (pd.to_numeric(pontuacoes_df["atleta_id"], errors="coerce") == atleta_id)
        & (pd.to_numeric(pontuacoes_df["rodada_id"], errors="coerce") >= rodada_min)
        & (pd.to_numeric(pontuacoes_df["rodada_id"], errors="coerce") <= rodada_max)
    ].copy()

    confrontation_columns = [
        column
        for column in [
            "clube_id",
            "rodada_id",
            "partida_id",
            "opponent_clube_id",
            "is_mandante",
        ]
        if column in confrontos_df.columns
    ]
    if not filtered.empty and {"clube_id", "rodada_id", "is_mandante"}.issubset(
        confrontation_columns
    ):
        confrontations = confrontos_df.loc[:, confrontation_columns].drop_duplicates(
            subset=["clube_id", "rodada_id"], keep="last"
        )
        filtered = filtered.merge(
            confrontations,
            on=["clube_id", "rodada_id"],
            how="left",
            suffixes=("", "_confronto"),
        )
        if is_mandante == "mandante":
            filtered = filtered.loc[filtered["is_mandante"].fillna(False)]
        elif is_mandante == "visitante":
            filtered = filtered.loc[~filtered["is_mandante"].fillna(True)]

    scout_columns = [column for column in Scout.as_list() if column in filtered.columns]
    scout_totals = {
        scout: int(pd.to_numeric(filtered[scout], errors="coerce").fillna(0).sum())
        for scout in scout_columns
        if pd.to_numeric(filtered[scout], errors="coerce").fillna(0).sum() != 0
    }

    matches = []
    for _, row in filtered.sort_values("rodada_id", ascending=False).iterrows():
        opponent_id = (
            int(row["opponent_clube_id"])
            if "opponent_clube_id" in row and pd.notna(row["opponent_clube_id"])
            else 0
        )
        opponent = clubes_cache.get(str(opponent_id), {})
        match_scouts = {
            scout: int(row[scout])
            for scout in scout_columns
            if pd.notna(row.get(scout)) and int(row[scout]) != 0
        }
        matches.append(
            {
                "round": int(row["rodada_id"]),
                "match_id": int(row["partida_id"])
                if "partida_id" in row and pd.notna(row["partida_id"])
                else 0,
                "is_home": bool(row["is_mandante"])
                if "is_mandante" in row and pd.notna(row["is_mandante"])
                else None,
                "opponent_id": opponent_id,
                "opponent_name": opponent.get("nome_fantasia")
                or opponent.get("nome", ""),
                "opponent_badge": opponent.get("escudos", {}).get("60x60", ""),
                "points": round(_safe_float(row.get("pontuacao", 0)), 2),
                "basic_points": round(_safe_float(row.get("pontuacao_basica", 0)), 2),
                "scouts": match_scouts,
            }
        )

    clube = clubes_cache.get(str(clube_id), {})
    posicao = posicoes_cache.get(str(posicao_id), {})
    status = status_cache.get(str(status_id), {})
    total_matches = len(filtered)
    return {
        "data_provider": "Cartola FC",
        "profile": {
            "id": atleta_id,
            "nickname": player.get("apelido", ""),
            "full_name": player.get("nome") or player.get("apelido", ""),
            "photo": player.get("foto", ""),
            "club_id": clube_id,
            "club_name": clube.get("nome_fantasia") or clube.get("nome", ""),
            "club_badge": clube.get("escudos", {}).get("60x60", ""),
            "position": posicao.get("abreviacao", "").upper(),
            "position_name": posicao.get("nome", ""),
            "status": status.get("nome", ""),
            "price": round(_safe_float(player.get("preco_num", 0)), 2),
        },
        "summary": {
            "matches": total_matches,
            "average_points": round(
                _safe_float(filtered["pontuacao"].mean())
                if total_matches and "pontuacao" in filtered
                else 0,
                2,
            ),
            "average_basic_points": round(
                _safe_float(filtered["pontuacao_basica"].mean())
                if total_matches and "pontuacao_basica" in filtered
                else 0,
                2,
            ),
            "total_points": round(
                _safe_float(filtered["pontuacao"].sum())
                if total_matches and "pontuacao" in filtered
                else 0,
                2,
            ),
            "scouts": scout_totals,
        },
        "matches": matches,
    }
