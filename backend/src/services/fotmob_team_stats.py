from __future__ import annotations

import asyncio
import math
import re
from datetime import UTC, datetime
from typing import Any, Literal

import pandas as pd

from .fotmob import BRASILEIRAO_LEAGUE_ID, FotmobService, _as_int

TABLE_KEY = "fotmob_team_stats"
META_KEY = "fotmob_team_stats_meta"

TEAM_STAT_COLUMNS = [
    "season",
    "match_id",
    "round",
    "date",
    "team_id",
    "team_name",
    "opponent_id",
    "opponent_name",
    "is_home",
    "rating",
    "goals_for",
    "goals_against",
    "clean_sheet",
    "win",
    "possession",
    "xg",
    "xg_against",
    "shots",
    "shots_on_target",
    "shots_outside_box",
    "shots_against",
    "shots_on_target_against",
    "shots_outside_box_against",
    "big_chances",
    "big_chances_missed",
    "touches_opposition_box",
    "accurate_passes",
    "pass_accuracy",
    "corners",
    "corners_against",
    "tackles",
    "interceptions",
    "clearances",
    "keeper_saves",
    "fouls",
    "fouls_suffered",
]

MATCH_STAT_KEYS = {
    "possession": "BallPossesion",
    "xg": "expected_goals",
    "shots": "total_shots",
    "shots_on_target": "ShotsOnTarget",
    "shots_outside_box": "shots_outside_box",
    "big_chances": "big_chance",
    "big_chances_missed": "big_chance_missed_title",
    "touches_opposition_box": "touches_opp_box",
    "accurate_passes": "accurate_passes",
    "corners": "corners",
    "tackles": "matchstats.headers.tackles",
    "interceptions": "interceptions",
    "clearances": "clearances",
    "keeper_saves": "keeper_saves",
    "fouls": "fouls",
}

METRIC_DEFINITIONS = [
    {"key": "rating", "title": "Nota FotMob", "group": "overview", "precision": 2},
    {"key": "goals_for", "title": "Gols por jogo", "group": "overview", "precision": 2},
    {
        "key": "goals_against",
        "title": "Gols sofridos por jogo",
        "group": "overview",
        "precision": 2,
        "higher_is_better": False,
    },
    {
        "key": "possession",
        "title": "Posse média",
        "group": "overview",
        "precision": 1,
        "suffix": "%",
    },
    {
        "key": "clean_sheets",
        "title": "Jogos sem sofrer gol",
        "group": "overview",
        "precision": 0,
        "aggregate": "sum",
    },
    {"key": "xg", "title": "xG por jogo", "group": "overview", "precision": 2},
    {"key": "shots", "title": "Finalizações", "group": "attack", "precision": 1},
    {
        "key": "shots_on_target",
        "title": "Finalizações no alvo",
        "group": "attack",
        "precision": 1,
    },
    {
        "key": "big_chances",
        "title": "Grandes chances",
        "group": "attack",
        "precision": 1,
    },
    {
        "key": "touches_opposition_box",
        "title": "Toques na área rival",
        "group": "attack",
        "precision": 1,
    },
    {
        "key": "pass_accuracy",
        "title": "Precisão de passe",
        "group": "attack",
        "precision": 1,
        "suffix": "%",
    },
    {"key": "corners", "title": "Escanteios", "group": "attack", "precision": 1},
    {
        "key": "fouls_suffered",
        "title": "Faltas sofridas",
        "group": "attack",
        "precision": 1,
    },
    {
        "key": "xg_against",
        "title": "xG sofrido por jogo",
        "group": "defense",
        "precision": 2,
        "higher_is_better": False,
    },
    {
        "key": "shots_against",
        "title": "Finalizações sofridas",
        "group": "defense",
        "precision": 1,
        "higher_is_better": False,
    },
    {
        "key": "shots_on_target_against",
        "title": "Finalizações sofridas no alvo",
        "group": "defense",
        "precision": 1,
        "higher_is_better": False,
    },
    {
        "key": "shots_outside_box_against",
        "title": "Finalizações sofridas de fora da área",
        "group": "defense",
        "precision": 1,
        "higher_is_better": False,
    },
    {
        "key": "corners_against",
        "title": "Escanteios cedidos",
        "group": "defense",
        "precision": 1,
        "higher_is_better": False,
    },
    {"key": "tackles", "title": "Desarmes Opta", "group": "defense", "precision": 1},
    {
        "key": "interceptions",
        "title": "Interceptações",
        "group": "defense",
        "precision": 1,
    },
    {"key": "clearances", "title": "Cortes", "group": "defense", "precision": 1},
    {"key": "keeper_saves", "title": "Defesas", "group": "defense", "precision": 1},
    {
        "key": "fouls",
        "title": "Faltas cometidas",
        "group": "defense",
        "precision": 1,
        "higher_is_better": False,
    },
]


def _numeric(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, int | float):
        return float(value)
    match = re.search(r"-?\d+(?:[.,]\d+)?", str(value))
    return float(match.group(0).replace(",", ".")) if match else None


def _percentage(value: Any) -> float | None:
    if value is None:
        return None
    match = re.search(r"\((\d+(?:[.,]\d+)?)%\)", str(value))
    return float(match.group(1).replace(",", ".")) if match else None


def _all_period_stats(details: dict[str, Any]) -> dict[str, list[Any]]:
    groups = (
        details.get("content", {})
        .get("stats", {})
        .get("Periods", {})
        .get("All", {})
        .get("stats", [])
    )
    result: dict[str, list[Any]] = {}
    for group in groups if isinstance(groups, list) else []:
        for item in group.get("stats", []):
            key = item.get("key")
            values = item.get("stats")
            if (
                key
                and isinstance(values, list)
                and len(values) >= 2
                and any(value is not None for value in values[:2])
                and key not in result
            ):
                result[key] = values[:2]
    return result


def parse_match_team_rows(
    fixture: dict[str, Any], details: dict[str, Any], season: str
) -> list[dict[str, Any]]:
    header = details.get("header", {})
    teams = header.get("teams", [])
    if not isinstance(teams, list) or len(teams) < 2:
        return []

    match_id = _as_int(details.get("general", {}).get("matchId") or fixture.get("id"))
    round_id = _as_int(
        details.get("general", {}).get("matchRound")
        or fixture.get("roundName")
        or fixture.get("round")
    )
    if match_id is None or round_id is None:
        return []

    lineup = details.get("content", {}).get("lineup", {})
    lineup_teams = [lineup.get("homeTeam", {}), lineup.get("awayTeam", {})]
    stats = _all_period_stats(details)
    status = header.get("status", {})
    date = status.get("utcTime") or fixture.get("status", {}).get("utcTime")

    rows = []
    for index, team in enumerate(teams[:2]):
        opponent_index = 1 - index
        opponent = teams[opponent_index]
        goals_for = _numeric(team.get("score")) or 0.0
        goals_against = _numeric(opponent.get("score")) or 0.0
        row: dict[str, Any] = {
            "season": str(season),
            "match_id": match_id,
            "round": round_id,
            "date": date,
            "team_id": _as_int(team.get("id")) or 0,
            "team_name": team.get("name", ""),
            "opponent_id": _as_int(opponent.get("id")) or 0,
            "opponent_name": opponent.get("name", ""),
            "is_home": index == 0,
            "rating": _numeric(lineup_teams[index].get("rating")),
            "goals_for": goals_for,
            "goals_against": goals_against,
            "clean_sheet": int(goals_against == 0),
            "win": int(goals_for > goals_against),
        }
        for column, stat_key in MATCH_STAT_KEYS.items():
            values = stats.get(stat_key, [None, None])
            row[column] = _numeric(values[index])

        xg_values = stats.get("expected_goals", [None, None])
        row["xg_against"] = _numeric(xg_values[opponent_index])
        shots_values = stats.get("total_shots", [None, None])
        row["shots_against"] = _numeric(shots_values[opponent_index])
        shots_on_target_values = stats.get("ShotsOnTarget", [None, None])
        row["shots_on_target_against"] = _numeric(
            shots_on_target_values[opponent_index]
        )
        shots_outside_box_values = stats.get("shots_outside_box", [None, None])
        row["shots_outside_box_against"] = _numeric(
            shots_outside_box_values[opponent_index]
        )
        corners_values = stats.get("corners", [None, None])
        row["corners_against"] = _numeric(corners_values[opponent_index])
        fouls_values = stats.get("fouls", [None, None])
        row["fouls_suffered"] = _numeric(fouls_values[opponent_index])
        accurate_values = stats.get("accurate_passes", [None, None])
        row["pass_accuracy"] = _percentage(accurate_values[index])
        rows.append(row)
    return rows


def _backfill_opponent_stats(df: pd.DataFrame) -> pd.DataFrame:
    """Derive conceded metrics from the opponent row already in Redis."""
    if df.empty or not {
        "season",
        "match_id",
        "team_id",
        "opponent_id",
        "shots",
        "shots_on_target",
        "shots_outside_box",
        "corners",
        "fouls",
    }.issubset(df.columns):
        return df

    opponent_stats = (
        df[
            [
                "season",
                "match_id",
                "team_id",
                "shots",
                "shots_on_target",
                "shots_outside_box",
                "corners",
                "fouls",
            ]
        ]
        .rename(
            columns={
                "team_id": "opponent_id",
                "shots": "shots_against",
                "shots_on_target": "shots_on_target_against",
                "shots_outside_box": "shots_outside_box_against",
                "corners": "corners_against",
                "fouls": "fouls_suffered",
            }
        )
        .drop_duplicates(subset=["season", "match_id", "opponent_id"], keep="last")
    )
    return df.drop(
        columns=[
            "shots_against",
            "shots_on_target_against",
            "shots_outside_box_against",
            "corners_against",
            "fouls_suffered",
        ],
        errors="ignore",
    ).merge(
        opponent_stats,
        on=["season", "match_id", "opponent_id"],
        how="left",
        validate="many_to_one",
    )


async def sync_fotmob_team_stats(store, request_handler) -> dict[str, Any]:
    service = FotmobService(store, request_handler)
    # The league payload is the single cheap freshness check. Match details are
    # fetched only for completed fixture IDs that are not yet persisted.
    league = await service.get_league(force_refresh=True)
    season = str(
        league.get("details", {}).get("selectedSeason")
        or league.get("details", {}).get("latestSeason")
        or datetime.now(UTC).year
    )
    fixtures = [
        fixture
        for fixture in league.get("fixtures", {}).get("allMatches", [])
        if fixture.get("status", {}).get("finished") and _as_int(fixture.get("id"))
    ]

    current = store.load_dataframe(TABLE_KEY)
    if (
        not isinstance(current, pd.DataFrame)
        or current.empty
        or "season" not in current
        or not (current["season"].astype(str) == season).any()
    ):
        current = pd.DataFrame(columns=TEAM_STAT_COLUMNS)
    else:
        current = current.loc[current["season"].astype(str) == season].copy()

    needs_detail_backfill = (
        not current.empty and "shots_outside_box" not in current.columns
    )
    existing_ids = set(
        pd.to_numeric(current.get("match_id"), errors="coerce").dropna().astype(int)
    )
    pending = (
        fixtures
        if needs_detail_backfill
        else [fixture for fixture in fixtures if int(fixture["id"]) not in existing_ids]
    )
    fetched = await asyncio.gather(
        *(service._get_match(fixture) for fixture in pending),
        return_exceptions=True,
    )

    rows = []
    failed = []
    for fixture, details in zip(pending, fetched, strict=True):
        if isinstance(details, Exception):
            failed.append(int(fixture["id"]))
            continue
        parsed = parse_match_team_rows(fixture, details, season)
        if parsed:
            rows.extend(parsed)
        else:
            failed.append(int(fixture["id"]))

    if rows:
        current = pd.concat([current, pd.DataFrame(rows)], ignore_index=True)
    current = _backfill_opponent_stats(current)
    current = current.reindex(columns=TEAM_STAT_COLUMNS).drop_duplicates(
        subset=["season", "match_id", "team_id"], keep="last"
    )
    store.save_dataframe(TABLE_KEY, current)
    updated_at = datetime.now(UTC)
    store.save_last_updated(TABLE_KEY, updated_at)
    store.save_json(
        META_KEY,
        {
            "league_id": BRASILEIRAO_LEAGUE_ID,
            "season": season,
            "completed_matches": len(fixtures),
            "stored_matches": int(current["match_id"].nunique())
            if not current.empty
            else 0,
            "updated_at": updated_at.isoformat(),
        },
    )
    return {
        "season": season,
        "fetched_matches": len(pending),
        "stored_matches": int(current["match_id"].nunique())
        if not current.empty
        else 0,
        "failed_match_ids": failed,
        "schema_backfill": needs_detail_backfill,
    }


def _clean_number(value: Any, precision: int) -> int | float | None:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return None
    rounded = round(float(value), precision)
    return int(rounded) if precision == 0 else rounded


def aggregate_team_stats(
    df: pd.DataFrame,
    rodada_min: int,
    rodada_max: int,
    is_mandante: Literal["geral", "mandante", "visitante"],
    updated_at: datetime | None = None,
) -> dict[str, Any]:
    filtered = df.loc[
        (pd.to_numeric(df["round"], errors="coerce") >= rodada_min)
        & (pd.to_numeric(df["round"], errors="coerce") <= rodada_max)
    ].copy()
    if is_mandante == "mandante":
        filtered = filtered.loc[filtered["is_home"]]
    elif is_mandante == "visitante":
        filtered = filtered.loc[~filtered["is_home"]]

    if filtered.empty:
        return {
            "source": "Opta via FotMob",
            "updated_at": updated_at.isoformat() if updated_at else None,
            "filters": {
                "rodada_min": rodada_min,
                "rodada_max": rodada_max,
                "is_mandante": is_mandante,
            },
            "sample_matches": 0,
            "metrics": [],
        }

    grouped = filtered.groupby(["team_id", "team_name"], as_index=False).agg(
        games=("match_id", "nunique"),
        rating=("rating", "mean"),
        goals_for=("goals_for", "mean"),
        goals_against=("goals_against", "mean"),
        clean_sheets=("clean_sheet", "sum"),
        xg=("xg", "mean"),
        xg_against=("xg_against", "mean"),
        possession=("possession", "mean"),
        shots=("shots", "mean"),
        shots_on_target=("shots_on_target", "mean"),
        shots_against=("shots_against", "mean"),
        shots_on_target_against=("shots_on_target_against", "mean"),
        shots_outside_box_against=("shots_outside_box_against", "mean"),
        big_chances=("big_chances", "mean"),
        touches_opposition_box=("touches_opposition_box", "mean"),
        pass_accuracy=("pass_accuracy", "mean"),
        corners=("corners", "mean"),
        corners_against=("corners_against", "mean"),
        tackles=("tackles", "mean"),
        interceptions=("interceptions", "mean"),
        clearances=("clearances", "mean"),
        keeper_saves=("keeper_saves", "mean"),
        fouls=("fouls", "mean"),
        fouls_suffered=("fouls_suffered", "mean"),
    )

    metrics = []
    for definition in METRIC_DEFINITIONS:
        key = definition["key"]
        precision = int(definition.get("precision", 1))
        higher_is_better = bool(definition.get("higher_is_better", True))
        values = grouped.dropna(subset=[key]).sort_values(
            key, ascending=not higher_is_better
        )
        teams = [
            {
                "rank": rank,
                "team_id": int(row.team_id),
                "team_name": str(row.team_name),
                "games": int(row.games),
                "value": _clean_number(getattr(row, key), precision),
                "logo": f"https://images.fotmob.com/image_resources/logo/teamlogo/{int(row.team_id)}.png",
            }
            for rank, row in enumerate(values.itertuples(index=False), start=1)
        ]
        metrics.append(
            {**definition, "higher_is_better": higher_is_better, "teams": teams}
        )

    return {
        "source": "Opta via FotMob",
        "updated_at": updated_at.isoformat() if updated_at else None,
        "season": str(filtered["season"].iloc[0]),
        "filters": {
            "rodada_min": rodada_min,
            "rodada_max": rodada_max,
            "is_mandante": is_mandante,
        },
        "sample_matches": int(filtered["match_id"].nunique()),
        "metrics": metrics,
    }
