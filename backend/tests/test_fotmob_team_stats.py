from copy import deepcopy
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock

import pandas as pd
import pytest

from src.services.fotmob_team_stats import (
    TEAM_STAT_COLUMNS,
    aggregate_team_stats,
    parse_match_team_rows,
    sync_fotmob_team_stats,
)


def _match_details():
    return {
        "general": {"matchId": "5103369", "matchRound": "4"},
        "header": {
            "status": {"utcTime": "2026-04-12T19:00:00.000Z"},
            "teams": [
                {"id": 10272, "name": "Bahia", "score": 2},
                {"id": 10273, "name": "Vitória", "score": 0},
            ],
        },
        "content": {
            "lineup": {
                "homeTeam": {"rating": 7.2},
                "awayTeam": {"rating": 6.1},
            },
            "stats": {
                "Periods": {
                    "All": {
                        "stats": [
                            {
                                "stats": [
                                    {"key": "BallPossesion", "stats": [61, 39]},
                                    {
                                        "key": "expected_goals",
                                        "stats": [1.84, 0.42],
                                    },
                                    {"key": "total_shots", "stats": [17, 6]},
                                    {"key": "ShotsOnTarget", "stats": [7, 2]},
                                    {"key": "shots_outside_box", "stats": [7, 5]},
                                    {"key": "corners", "stats": [8, 3]},
                                    {"key": "fouls", "stats": [14, 9]},
                                    {
                                        "key": "accurate_passes",
                                        "stats": ["421 (89%)", "237 (76%)"],
                                    },
                                ]
                            }
                        ]
                    }
                }
            },
        },
    }


def test_parse_match_creates_home_and_away_team_rows():
    fixture = {"id": "5103369", "roundName": "4"}

    home, away = parse_match_team_rows(fixture, _match_details(), "2026")

    assert home["team_id"] == 10272
    assert home["is_home"] is True
    assert home["rating"] == 7.2
    assert home["goals_for"] == 2
    assert home["goals_against"] == 0
    assert home["clean_sheet"] == 1
    assert home["possession"] == 61
    assert home["xg"] == 1.84
    assert home["xg_against"] == 0.42
    assert home["shots_against"] == 6
    assert home["shots_on_target_against"] == 2
    assert home["shots_outside_box_against"] == 5
    assert home["corners_against"] == 3
    assert home["fouls_suffered"] == 9
    assert home["pass_accuracy"] == 89

    assert away["team_id"] == 10273
    assert away["is_home"] is False
    assert away["goals_for"] == 0
    assert away["goals_against"] == 2
    assert away["xg_against"] == 1.84
    assert away["shots_against"] == 17
    assert away["shots_on_target_against"] == 7
    assert away["shots_outside_box_against"] == 7
    assert away["corners_against"] == 8
    assert away["fouls_suffered"] == 14
    assert away["pass_accuracy"] == 76


def test_aggregate_team_stats_filters_venue_and_ranks_defense_ascending():
    rows = [
        {
            **row,
            "season": "2026",
            "date": "2026-04-12T19:00:00.000Z",
            "big_chances": 2,
            "touches_opposition_box": 24,
            "corners": 5,
            "tackles": 11,
            "interceptions": 8,
            "clearances": 13,
            "keeper_saves": 3,
            "fouls": 12,
        }
        for row in parse_match_team_rows(
            {"id": "5103369", "roundName": "4"}, _match_details(), "2026"
        )
    ]
    frame = pd.DataFrame(rows)
    updated_at = datetime(2026, 4, 12, tzinfo=UTC)

    general = aggregate_team_stats(frame, 1, 10, "geral", updated_at)
    home = aggregate_team_stats(frame, 1, 10, "mandante", updated_at)
    visitor = aggregate_team_stats(frame, 1, 3, "visitante", updated_at)

    conceded = next(
        metric for metric in general["metrics"] if metric["key"] == "goals_against"
    )
    assert [team["team_name"] for team in conceded["teams"]] == ["Bahia", "Vitória"]
    assert general["sample_matches"] == 1
    assert general["updated_at"] == "2026-04-12T00:00:00+00:00"
    assert all(
        team["team_name"] == "Bahia"
        for metric in home["metrics"]
        for team in metric["teams"]
    )
    assert visitor["sample_matches"] == 0
    assert visitor["metrics"] == []


@pytest.mark.anyio
async def test_sync_fetches_only_new_completed_matches(monkeypatch):
    from src.services.fotmob import FotmobService

    stored_rows = parse_match_team_rows(
        {"id": "5103369", "roundName": "4"}, _match_details(), "2026"
    )
    legacy_columns = [
        column
        for column in TEAM_STAT_COLUMNS
        if column
        not in {
            "shots_against",
            "shots_on_target_against",
            "corners_against",
            "fouls_suffered",
        }
    ]
    stored = pd.DataFrame(stored_rows).reindex(columns=legacy_columns)
    new_details = deepcopy(_match_details())
    new_details["general"].update(matchId="5103370", matchRound="5")
    new_details["header"]["teams"] = [
        {"id": 109705, "name": "Red Bull Bragantino", "score": 1},
        {"id": 9788, "name": "Corinthians", "score": 1},
    ]
    league = {
        "details": {"selectedSeason": "2026"},
        "fixtures": {
            "allMatches": [
                {"id": "5103369", "roundName": "4", "status": {"finished": True}},
                {"id": "5103370", "roundName": "5", "status": {"finished": True}},
                {"id": "5103371", "roundName": "6", "status": {"finished": False}},
            ]
        },
    }
    get_league = AsyncMock(return_value=league)
    get_match = AsyncMock(return_value=new_details)
    monkeypatch.setattr(FotmobService, "get_league", get_league)
    monkeypatch.setattr(FotmobService, "_get_match", get_match)
    store = MagicMock()
    store.load_dataframe.return_value = stored

    result = await sync_fotmob_team_stats(store, MagicMock())

    get_league.assert_awaited_once_with(force_refresh=True)
    get_match.assert_awaited_once()
    assert get_match.await_args.args[0]["id"] == "5103370"
    assert result["fetched_matches"] == 1
    assert result["stored_matches"] == 2
    saved = store.save_dataframe.call_args.args[1]
    assert set(saved["match_id"]) == {5103369, 5103370}
    migrated_home = saved.loc[
        (saved["match_id"] == 5103369) & (saved["team_id"] == 10272)
    ].iloc[0]
    assert migrated_home["shots_against"] == 6
    assert migrated_home["shots_on_target_against"] == 2
    assert migrated_home["corners_against"] == 3
    assert migrated_home["fouls_suffered"] == 9


@pytest.mark.anyio
async def test_sync_replays_existing_matches_when_raw_schema_expands(monkeypatch):
    from src.services.fotmob import FotmobService

    rows = parse_match_team_rows(
        {"id": "5103369", "roundName": "4"}, _match_details(), "2026"
    )
    old_columns = [
        column
        for column in TEAM_STAT_COLUMNS
        if column not in {"shots_outside_box", "shots_outside_box_against"}
    ]
    store = MagicMock()
    store.load_dataframe.return_value = pd.DataFrame(rows).reindex(columns=old_columns)
    monkeypatch.setattr(
        FotmobService,
        "get_league",
        AsyncMock(
            return_value={
                "details": {"selectedSeason": "2026"},
                "fixtures": {
                    "allMatches": [
                        {
                            "id": "5103369",
                            "roundName": "4",
                            "status": {"finished": True},
                        }
                    ]
                },
            }
        ),
    )
    get_match = AsyncMock(return_value=_match_details())
    monkeypatch.setattr(FotmobService, "_get_match", get_match)

    result = await sync_fotmob_team_stats(store, MagicMock())

    get_match.assert_awaited_once()
    assert result["schema_backfill"] is True
    saved = store.save_dataframe.call_args.args[1]
    home = saved.loc[saved["team_id"] == 10272].iloc[0]
    assert home["shots_outside_box"] == 7
    assert home["shots_outside_box_against"] == 5
