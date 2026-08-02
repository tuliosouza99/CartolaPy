from datetime import UTC, datetime
from unittest.mock import MagicMock

import pandas as pd

from src.services.enums import Scout
from src.services.match_analysis import (
    build_fotmob_team_form,
    build_match_analysis,
)


def _fotmob_rows(team_id: int, *, start_xg: float) -> list[dict]:
    return [
        {
            "season": "2026",
            "match_id": team_id * 100 + rodada,
            "round": rodada,
            "date": f"2026-04-{rodada:02d}T19:00:00Z",
            "team_id": team_id,
            "is_home": rodada % 2 == 0,
            "rating": 6.5 + rodada / 100,
            "xg": start_xg + rodada / 10,
            "xg_against": 1.5 - rodada / 20,
            "shots_on_target": rodada,
            "shots_on_target_against": 12 - rodada,
            "big_chances": rodada / 2,
            "touches_opposition_box": 10 + rodada,
            "corners": rodada / 2,
            "keeper_saves": 3,
        }
        for rodada in range(1, 11)
    ]


def _conceded_rows() -> pd.DataFrame:
    rows = []
    for club_id, is_home, points in [
        (276, False, 7.0),
        (264, True, 4.0),
    ]:
        for rodada in range(1, 11):
            rows.append(
                {
                    "clube_id": club_id,
                    "posicao_id": 5,
                    "is_mandante": is_home,
                    "rodada_id": rodada,
                    "partida_id": club_id * 100 + rodada,
                    "pontuacao": points,
                    "pontuacao_basica": points - 1,
                    **{
                        scout: 1 if scout in {"G", "FD"} else 0
                        for scout in Scout.as_list()
                    },
                }
            )
    return pd.DataFrame(rows)


def test_fotmob_team_form_keeps_l5_l10_and_matching_venue():
    frame = pd.DataFrame(_fotmob_rows(9808, start_xg=0.5))

    result = build_fotmob_team_form(frame, 9808, "home")

    assert result["available"] is True
    assert result["l5"]["games"] == 5
    assert result["l10"]["games"] == 10
    assert result["venue_l10"]["games"] == 5
    assert result["l5"]["metrics"]["xg"] == 1.3
    assert result["venue_l10"]["metrics"]["shots_on_target"] == 6.0


def test_match_analysis_selects_one_match_and_builds_position_duels(monkeypatch):
    from src.services import match_analysis

    candidate = {
        "atleta_id": 99,
        "apelido": "Camisa 9",
        "clube_id": 264,
        "clube_nome": "Corinthians",
        "posicao_id": 5,
        "posicao": "ATA",
        "status": "Provável",
        "score": 11.2,
        "confidence": "Alta",
        "media": 8.2,
        "media_no_mando": 9.0,
    }
    monkeypatch.setattr(
        match_analysis,
        "build_matchup_insights_from_store",
        lambda **_: {"matchups": [candidate]},
    )

    matches = [
        {
            "partida_id": 1,
            "mandante_id": 262,
            "visitante_id": 263,
            "mandante_nome": "Flamengo",
            "visitante_nome": "Botafogo",
        },
        {
            "partida_id": 2,
            "mandante_id": 264,
            "visitante_id": 276,
            "mandante_nome": "Corinthians",
            "visitante_nome": "São Paulo",
            "partida_data": "2026-04-12T19:00:00Z",
            "local": "Neo Química Arena",
        },
    ]
    fotmob = pd.DataFrame(
        [
            *_fotmob_rows(9808, start_xg=0.5),
            *_fotmob_rows(10277, start_xg=0.3),
        ]
    )
    conceded = _conceded_rows()
    store = MagicMock()
    store.load_json.side_effect = lambda key: {
        "clubes": {},
        "posicoes": {"5": {"id": 5, "nome": "Atacante", "abreviacao": "ata"}},
    }.get(key, {})
    store.load_dataframe.side_effect = lambda key: {
        "pontos_cedidos": conceded,
        "fotmob_team_stats": fotmob,
    }.get(key, pd.DataFrame())
    store.load_last_updated.return_value = datetime(2026, 4, 11, tzinfo=UTC)

    payload = build_match_analysis(
        store=store,
        rodada=11,
        matches=matches,
        selected_match_id=2,
    )

    assert len(payload["matches"]) == 2
    assert payload["selected"]["id"] == 2
    assert payload["selected"]["home"]["form"]["selected"]["games"] == 3
    assert payload["selected"]["top_targets"][0]["atleta_id"] == 99
    attacker_duel = payload["selected"]["duels"][0]
    assert attacker_duel["position"] == "ATA"
    assert attacker_duel["home"]["conceded"]["points"] == 7.0
    assert attacker_duel["away"]["conceded"]["points"] == 4.0
    assert len(attacker_duel["home"]["conceded"]["scouts"]) == 2
    assert attacker_duel["home"]["conceded"]["scouts"][0]["code"] == "G"
    assert payload["filters"] == {
        "rodada_min": 6,
        "rodada_max": 10,
        "contextual_venue": True,
    }
    assert {metric["key"] for metric in payload["fotmob"]["metrics"]} >= {
        "accurate_passes",
        "shots_outside_box",
        "win_rate",
    }
    assert payload["fotmob"]["updated_at"] == "2026-04-11T00:00:00+00:00"


def test_match_analysis_applies_custom_round_range_without_venue_filter(monkeypatch):
    from src.services import match_analysis

    monkeypatch.setattr(
        match_analysis,
        "build_matchup_insights_from_store",
        lambda **_: {
            "matchups": [
                {
                    "atleta_id": 99,
                    "clube_id": 264,
                    "posicao_id": 5,
                    "status": "Provável",
                    "score": 10,
                },
                {
                    "atleta_id": 100,
                    "clube_id": 264,
                    "posicao_id": 5,
                    "status": "Dúvida",
                    "score": 20,
                },
                {
                    "atleta_id": 101,
                    "clube_id": 264,
                    "posicao_id": 5,
                    "status": "Provável",
                    "score": 5,
                },
            ]
        },
    )
    store = MagicMock()
    store.load_json.side_effect = lambda key: {
        "posicoes": {"5": {"abreviacao": "ata"}}
    }.get(key, {})
    store.load_dataframe.side_effect = lambda key: {
        "pontos_cedidos": _conceded_rows(),
        "fotmob_team_stats": pd.DataFrame(
            [
                *_fotmob_rows(9808, start_xg=0.5),
                *_fotmob_rows(10277, start_xg=0.3),
            ]
        ),
    }.get(key, pd.DataFrame())
    matches = [
        {
            "partida_id": 2,
            "mandante_id": 264,
            "visitante_id": 276,
            "mandante_nome": "Corinthians",
            "visitante_nome": "São Paulo",
        }
    ]

    payload = build_match_analysis(
        store=store,
        rodada=11,
        matches=matches,
        rodada_min=3,
        rodada_max=8,
        contextual_venue=False,
    )

    assert payload["filters"] == {
        "rodada_min": 3,
        "rodada_max": 8,
        "contextual_venue": False,
    }
    assert payload["selected"]["home"]["form"]["selected"]["games"] == 6
    assert payload["selected"]["duels"][0]["home"]["conceded"]["games"] == 6
    assert [item["atleta_id"] for item in payload["selected"]["top_targets"]] == [
        99,
        101,
    ]
