from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

import pandas as pd

from .dicas_da_rodada import build_matchup_insights_from_store
from .enums import Scout
from .fotmob_mappings import build_predefined_club_mapping
from .fotmob_team_stats import (
    METRIC_DEFINITIONS,
)
from .fotmob_team_stats import (
    TABLE_KEY as FOTMOB_TEAM_STATS_TABLE,
)
from .pontos_cedidos_unified import compute_pontos_cedidos_unified

FOTMOB_METRICS = [
    {
        **definition,
        "label": definition["title"],
        "lower_is_better": not definition.get("higher_is_better", True),
    }
    for definition in METRIC_DEFINITIONS
]
FOTMOB_METRICS.extend(
    [
        {
            "key": "win_rate",
            "source_key": "win",
            "label": "Vitórias",
            "group": "overview",
            "precision": 0,
            "suffix": "%",
            "transform": "percentage",
        },
        {
            "key": "shots_outside_box",
            "label": "Finalizações de fora da área",
            "group": "attack",
            "precision": 1,
        },
        {
            "key": "big_chances_missed",
            "label": "Grandes chances perdidas",
            "group": "attack",
            "precision": 1,
            "lower_is_better": True,
        },
        {
            "key": "accurate_passes",
            "label": "Passes certos",
            "group": "attack",
            "precision": 1,
        },
    ]
)

FOTMOB_GROUP_ORDER = {"overview": 0, "attack": 1, "defense": 2}
FOTMOB_METRICS.sort(
    key=lambda item: (FOTMOB_GROUP_ORDER.get(item.get("group"), 9), item["label"])
)

FOTMOB_METRICS_BY_KEY = {definition["key"]: definition for definition in FOTMOB_METRICS}

# Preserve the raw collection's clean-sheet column while exposing the public name
# used by the team-stat view.
FOTMOB_METRICS_BY_KEY["clean_sheets"] = {
    **FOTMOB_METRICS_BY_KEY["clean_sheets"],
    "source_key": "clean_sheet",
    "transform": "sum",
}

FOTMOB_METRICS = list(FOTMOB_METRICS_BY_KEY.values())


def _clean_number(value: Any, precision: int = 2) -> int | float | None:
    try:
        if pd.isna(value):
            return None
        rounded = round(float(value), precision)
        return int(rounded) if precision == 0 else rounded
    except (TypeError, ValueError):
        return None


def _club_details(clubs: dict[str, Any], club_id: int, fallback: dict) -> dict:
    club = clubs.get(str(club_id), {})
    mapping = build_predefined_club_mapping(club_id)
    return {
        "id": club_id,
        "name": fallback.get("name")
        or club.get("nome_fantasia")
        or club.get("nome")
        or f"Clube {club_id}",
        "badge": fallback.get("badge") or club.get("escudos", {}).get("60x60", ""),
        "fotmob_id": mapping.get("fotmob_id") if mapping else None,
    }


def _average_stats(rows: pd.DataFrame) -> dict[str, Any]:
    if rows.empty:
        return {"games": 0, "metrics": {}}
    metrics = {}
    for definition in FOTMOB_METRICS:
        key = definition["key"]
        source_key = definition.get("source_key", key)
        if source_key not in rows.columns:
            metrics[key] = None
            continue
        values = pd.to_numeric(rows[source_key], errors="coerce").dropna()
        if values.empty:
            metrics[key] = None
            continue
        transform = definition.get("transform")
        value = (
            values.sum()
            if transform == "sum"
            else values.mean() * 100
            if transform == "percentage"
            else values.mean()
        )
        metrics[key] = _clean_number(value, int(definition.get("precision", 1)))
    return {"games": int(rows["match_id"].nunique()), "metrics": metrics}


def build_fotmob_team_form(
    df: pd.DataFrame,
    fotmob_team_id: int | None,
    upcoming_venue: Literal["home", "away"],
    rodada_min: int | None = None,
    rodada_max: int | None = None,
    contextual_venue: bool = False,
) -> dict[str, Any]:
    empty = {
        "available": False,
        "l5": {"games": 0, "metrics": {}},
        "l10": {"games": 0, "metrics": {}},
        "venue_l10": {"games": 0, "metrics": {}},
        "selected": {"games": 0, "metrics": {}},
    }
    if (
        fotmob_team_id is None
        or not isinstance(df, pd.DataFrame)
        or df.empty
        or not {"team_id", "round", "match_id", "is_home"}.issubset(df.columns)
    ):
        return empty

    team_rows = df.loc[
        pd.to_numeric(df["team_id"], errors="coerce") == fotmob_team_id
    ].copy()
    if team_rows.empty:
        return empty
    team_rows["round"] = pd.to_numeric(team_rows["round"], errors="coerce")
    team_rows = team_rows.sort_values(["round", "date"], na_position="first")
    venue_rows = team_rows.loc[
        team_rows["is_home"].astype(bool) == (upcoming_venue == "home")
    ]
    selected_rows = team_rows
    if rodada_min is not None:
        selected_rows = selected_rows.loc[selected_rows["round"] >= rodada_min]
    if rodada_max is not None:
        selected_rows = selected_rows.loc[selected_rows["round"] <= rodada_max]
    if contextual_venue:
        selected_rows = selected_rows.loc[
            selected_rows["is_home"].astype(bool) == (upcoming_venue == "home")
        ]
    return {
        "available": True,
        "l5": _average_stats(team_rows.tail(5)),
        "l10": _average_stats(team_rows.tail(10)),
        "venue_l10": _average_stats(venue_rows.tail(10)),
        "selected": _average_stats(selected_rows),
    }


def _scout_highlights(conceded: dict[str, Any] | None) -> list[dict[str, Any]]:
    if not conceded:
        return []
    contributions = conceded.get("scout_contributions") or {}
    averages = conceded.get("scouts") or {}
    highlights = []
    for code in Scout.as_list():
        item = contributions.get(code, {})
        average = _clean_number(averages.get(code, item.get("raw_sum", 0)), 2) or 0.0
        if average == 0:
            continue
        points = (
            _clean_number(
                item.get("points_contribution", average * Scout.get_value(code)), 2
            )
            or 0.0
        )
        label = Scout[code].value["name"]
        highlights.append(
            {
                "code": code,
                "label": label,
                "average": average,
                "points": points,
                "percentage": _clean_number(item.get("percentage"), 1),
            }
        )
    return sorted(highlights, key=lambda item: abs(item["points"]), reverse=True)


def _concession_by_position(
    pontos_cedidos_df: pd.DataFrame,
    opponent_id: int,
    position_id: int,
    rodada_min: int,
    rodada_max: int,
    player_is_home: bool,
    contextual_venue: bool,
) -> dict[str, Any]:
    required = {
        "clube_id",
        "posicao_id",
        "rodada_id",
        "is_mandante",
        "pontuacao",
        "pontuacao_basica",
        *Scout.as_list(),
    }
    if pontos_cedidos_df.empty or not required.issubset(pontos_cedidos_df.columns):
        return {
            "games": 0,
            "points": None,
            "basic_points": None,
            "scouts": [],
        }
    aggregated = compute_pontos_cedidos_unified(
        pontos_cedidos_df=pontos_cedidos_df,
        rodada_min=rodada_min,
        rodada_max=rodada_max,
        is_mandante=(
            "mandante"
            if contextual_venue and player_is_home
            else "visitante"
            if contextual_venue
            else "geral"
        ),
        posicao_id=position_id,
    )
    row = aggregated.loc[
        pd.to_numeric(aggregated.get("clube_id"), errors="coerce") == opponent_id
    ]
    if row.empty:
        return {"games": 0, "points": None, "basic_points": None, "scouts": []}
    item = row.iloc[0].to_dict()
    return {
        "games": int(item.get("total_jogos") or 0),
        "points": _clean_number(item.get("media_cedida")),
        "basic_points": _clean_number(item.get("media_cedida_basica")),
        "scouts": _scout_highlights(item),
    }


def _position_label(positions: dict[str, Any], position_id: int) -> str:
    position = positions.get(str(position_id), {})
    abbreviation = str(position.get("abreviacao") or "").upper()
    return abbreviation or str(position.get("nome") or f"POS {position_id}")


def _duel_side(
    *,
    team_id: int,
    opponent_id: int,
    player_is_home: bool,
    position_id: int,
    candidates: list[dict],
    pontos_cedidos_df: pd.DataFrame,
    rodada_min: int,
    rodada_max: int,
    contextual_venue: bool,
) -> dict[str, Any]:
    targets = [
        candidate
        for candidate in candidates
        if int(candidate.get("clube_id") or 0) == team_id
        and int(candidate.get("posicao_id") or 0) == position_id
    ][:3]
    concession = _concession_by_position(
        pontos_cedidos_df,
        opponent_id,
        position_id,
        rodada_min,
        rodada_max,
        player_is_home,
        contextual_venue,
    )
    return {
        "team_id": team_id,
        "opponent_id": opponent_id,
        "conceded": concession,
        "targets": targets,
        "edge": _clean_number(
            pd.Series(
                [target.get("score") for target in targets], dtype="float64"
            ).mean()
            if targets
            else None
        ),
    }


def _build_duels(
    *,
    match: dict,
    candidates: list[dict],
    pontos_cedidos_df: pd.DataFrame,
    positions: dict[str, Any],
    rodada_min: int,
    rodada_max: int,
    contextual_venue: bool,
) -> list[dict]:
    home_id = int(match["mandante_id"])
    away_id = int(match["visitante_id"])
    conceded_position_values = (
        pd.to_numeric(pontos_cedidos_df["posicao_id"], errors="coerce").dropna()
        if "posicao_id" in pontos_cedidos_df.columns
        else pd.Series(dtype="float64")
    )
    position_ids = sorted(
        {
            *(int(value) for value in conceded_position_values),
            *(int(key) for key in positions if str(key).isdigit()),
        }
    )
    duels = []
    for position_id in position_ids:
        home = _duel_side(
            team_id=home_id,
            opponent_id=away_id,
            player_is_home=True,
            position_id=position_id,
            candidates=candidates,
            pontos_cedidos_df=pontos_cedidos_df,
            rodada_min=rodada_min,
            rodada_max=rodada_max,
            contextual_venue=contextual_venue,
        )
        away = _duel_side(
            team_id=away_id,
            opponent_id=home_id,
            player_is_home=False,
            position_id=position_id,
            candidates=candidates,
            pontos_cedidos_df=pontos_cedidos_df,
            rodada_min=rodada_min,
            rodada_max=rodada_max,
            contextual_venue=contextual_venue,
        )
        duels.append(
            {
                "position_id": position_id,
                "position": _position_label(positions, position_id),
                "home": home,
                "away": away,
            }
        )
    return duels


def build_match_analysis(
    *,
    store,
    rodada: int,
    matches: list[dict],
    selected_match_id: int | None = None,
    rodada_min: int | None = None,
    rodada_max: int | None = None,
    contextual_venue: bool = True,
) -> dict[str, Any]:
    rodada_max = min(rodada - 1, rodada_max or rodada - 1)
    rodada_min = max(1, rodada_min or max(1, rodada_max - 4))
    summaries = [
        {
            "id": match.get("partida_id"),
            "home": {
                "id": int(match["mandante_id"]),
                "name": match.get("mandante_nome", ""),
                "badge": match.get("mandante_escudo", ""),
            },
            "away": {
                "id": int(match["visitante_id"]),
                "name": match.get("visitante_nome", ""),
                "badge": match.get("visitante_escudo", ""),
            },
            "kickoff": match.get("partida_data"),
            "venue": match.get("local"),
        }
        for match in matches
    ]
    if not matches:
        return {"round": rodada, "matches": [], "selected": None}

    selected = next(
        (
            match
            for match in matches
            if selected_match_id is not None
            and int(match.get("partida_id") or 0) == selected_match_id
        ),
        matches[0],
    )
    clubs = store.load_json("clubes") or {}
    positions = store.load_json("posicoes") or {}
    pontos_cedidos_df = store.load_dataframe("pontos_cedidos")
    if not isinstance(pontos_cedidos_df, pd.DataFrame):
        pontos_cedidos_df = pd.DataFrame()

    insights = build_matchup_insights_from_store(
        store=store,
        rodada=rodada,
        span=max(1, min(10, rodada_max - rodada_min + 1)),
        limit=500,
        next_matches=matches,
        rodada_min_override=rodada_min,
        rodada_max_override=rodada_max,
    )
    all_candidates = insights.get("matchups", [])
    home_id = int(selected["mandante_id"])
    away_id = int(selected["visitante_id"])
    candidates = sorted(
        [
            item
            for item in all_candidates
            if int(item.get("clube_id") or 0) in {home_id, away_id}
            and item.get("status") == "Provável"
        ],
        key=lambda item: float(item.get("score") or 0),
        reverse=True,
    )

    home = _club_details(
        clubs,
        home_id,
        {
            "name": selected.get("mandante_nome"),
            "badge": selected.get("mandante_escudo"),
        },
    )
    away = _club_details(
        clubs,
        away_id,
        {
            "name": selected.get("visitante_nome"),
            "badge": selected.get("visitante_escudo"),
        },
    )
    fotmob_df = store.load_dataframe(FOTMOB_TEAM_STATS_TABLE)
    if not isinstance(fotmob_df, pd.DataFrame):
        fotmob_df = pd.DataFrame()
    home["form"] = build_fotmob_team_form(
        fotmob_df,
        home["fotmob_id"],
        "home",
        rodada_min,
        rodada_max,
        contextual_venue,
    )
    away["form"] = build_fotmob_team_form(
        fotmob_df,
        away["fotmob_id"],
        "away",
        rodada_min,
        rodada_max,
        contextual_venue,
    )

    selected_payload = {
        "id": selected.get("partida_id"),
        "kickoff": selected.get("partida_data"),
        "venue": selected.get("local"),
        "home": home,
        "away": away,
        "duels": _build_duels(
            match=selected,
            candidates=candidates,
            pontos_cedidos_df=pontos_cedidos_df,
            positions=positions,
            rodada_min=rodada_min,
            rodada_max=rodada_max,
            contextual_venue=contextual_venue,
        ),
        "top_targets": candidates,
    }
    updated_at = store.load_last_updated(FOTMOB_TEAM_STATS_TABLE)
    return {
        "round": rodada,
        "filters": {
            "rodada_min": rodada_min,
            "rodada_max": rodada_max,
            "contextual_venue": contextual_venue,
        },
        "matches": summaries,
        "selected": selected_payload,
        "fotmob": {
            "source": "Opta via FotMob",
            "updated_at": updated_at.isoformat()
            if isinstance(updated_at, datetime)
            else updated_at,
            "metrics": FOTMOB_METRICS,
        },
        "methodology": [
            "O intervalo usa somente rodadas anteriores ao confronto.",
            "O filtro de mando aplica casa para o mandante e fora para o visitante.",
            "O duelo combina forma do atleta, pontos e scouts cedidos pelo rival na posição, status e amostra.",
        ],
    }
