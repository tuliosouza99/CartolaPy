from unittest.mock import AsyncMock, MagicMock

import pytest
from src.services.fotmob import FotmobService, name_score, normalize_name
from src.services.fotmob_mappings import (
    BRASILEIRAO_CLUB_MAPPINGS,
    seed_fotmob_club_mappings,
)


def test_normalize_name_removes_accents_and_punctuation():
    assert normalize_name("Atlético-MG") == "atletico mg"


def test_name_score_accepts_short_display_name_within_full_name():
    assert name_score(["Victor Hugo Gomes Silva"], "Victor Hugo") >= 0.9


@pytest.mark.anyio
async def test_resolve_player_searches_full_name_and_persists_mapping():
    store = MagicMock()
    store.load_json.return_value = None
    request_handler = MagicMock()
    request_handler.make_get_request = AsyncMock(
        return_value={
            "squadMemberSuggest": [
                {
                    "options": [
                        {
                            "text": "Victor Hugo|1363368",
                            "payload": {
                                "id": "1363368",
                                "teamId": 10272,
                                "isCoach": False,
                            },
                        }
                    ]
                }
            ]
        }
    )
    service = FotmobService(store, request_handler)

    mapping = await service.resolve_player(
        123,
        ["Victor Hugo Gomes Silva", "Victor Hugo"],
        10272,
    )

    assert mapping["fotmob_id"] == 1363368
    assert mapping["matched_by"] == "team_squad_and_name_search"
    assert any(
        call.args[0] == "fotmob:mapping:player:123"
        for call in store.save_json.call_args_list
    )
    assert any(
        "term=Victor+Hugo+Gomes+Silva" in call.args[0]
        for call in request_handler.make_get_request.call_args_list
    )


@pytest.mark.anyio
async def test_all_predefined_clubs_resolve_without_network():
    store = MagicMock()
    store.load_json.return_value = None
    request_handler = MagicMock()
    request_handler.make_get_request = AsyncMock()
    service = FotmobService(store, request_handler)

    resolved = {
        cartola_id: await service.resolve_club(cartola_id, {})
        for cartola_id in BRASILEIRAO_CLUB_MAPPINGS
    }

    assert len(resolved) == 20
    assert resolved[293]["fotmob_id"] == 10273
    assert resolved[2305]["fotmob_id"] == 163782
    request_handler.make_get_request.assert_not_awaited()


def test_seed_fotmob_club_mappings_populates_individual_and_complete_maps():
    store = MagicMock()

    seed_fotmob_club_mappings(store)

    assert store.save_json.call_count == 21
    aggregate_call = store.save_json.call_args_list[-1]
    assert aggregate_call.args[0] == "fotmob:mapping:clubs"
    assert len(aggregate_call.args[1]) == 20


def test_aggregate_stats_builds_totals_per_90_and_weighted_rating():
    service = FotmobService(MagicMock(), MagicMock())
    appearances = [
        (
            {"round": 1},
            {
                "stats": [
                    {
                        "title": "Top stats",
                        "key": "top_stats",
                        "stats": {
                            "Minutes played": {
                                "key": "minutes_played",
                                "stat": {"value": 45, "type": "integer"},
                            },
                            "Goals": {
                                "key": "goals",
                                "stat": {"value": 1, "type": "integer"},
                            },
                            "FotMob rating": {
                                "key": "rating_title",
                                "stat": {"value": 8.0, "type": "double"},
                            },
                        },
                    }
                ]
            },
        ),
        (
            {"round": 2},
            {
                "stats": [
                    {
                        "title": "Top stats",
                        "key": "top_stats",
                        "stats": {
                            "Minutes played": {
                                "key": "minutes_played",
                                "stat": {"value": 90, "type": "integer"},
                            },
                            "Goals": {
                                "key": "goals",
                                "stat": {"value": 1, "type": "integer"},
                            },
                            "FotMob rating": {
                                "key": "rating_title",
                                "stat": {"value": 6.5, "type": "double"},
                            },
                        },
                    }
                ]
            },
        ),
    ]

    groups, minutes = service._aggregate_stats(appearances)
    metrics = {metric["key"]: metric for metric in groups[0]["metrics"]}

    assert minutes == 135
    assert metrics["goals"]["value"] == 2
    assert metrics["goals"]["per90"] == 1.33
    assert metrics["rating_title"]["value"] == 7.0
    assert metrics["rating_title"]["per90"] is None
