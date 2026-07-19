import io
import json

import pandas as pd
import pytest
from src.services import dicas_memory
from src.services.dicas_memory import (
    S3DicasMemoryStore,
    build_round_source_snapshot,
    load_memories_for_prediction,
    refresh_round_memories,
)


class NoSuchKeyError(Exception):
    response = {"Error": {"Code": "NoSuchKey"}}


class FakeS3Client:
    def __init__(self):
        self.objects = {}

    def put_object(self, Bucket, Key, Body, ContentType):
        self.objects[(Bucket, Key)] = bytes(Body)
        return {"ETag": "fake"}

    def get_object(self, Bucket, Key):
        try:
            body = self.objects[(Bucket, Key)]
        except KeyError as exc:
            raise NoSuchKeyError from exc
        return {"Body": io.BytesIO(body)}

    def delete_object(self, Bucket, Key):
        self.objects.pop((Bucket, Key), None)
        return {}


class FakeRedisStore:
    def __init__(self):
        self.rodada_id = 3
        self.json = {
            "clubes": {
                "10": {"nome": "Time A"},
                "20": {"nome": "Time B"},
            },
            "posicoes": {
                "4": {"nome": "Meia"},
                "5": {"nome": "Atacante"},
            },
        }
        self.frames = {
            "atletas": pd.DataFrame(
                {
                    "atleta_id": [1, 2, 3],
                    "apelido": ["Um", "Dois", "Três"],
                }
            ),
            "pontuacoes": pd.DataFrame(
                {
                    "atleta_id": [1, 2, 3, 1, 2, 3],
                    "rodada_id": [2, 2, 2, 3, 3, 3],
                    "clube_id": [10, 10, 20, 10, 10, 20],
                    "posicao_id": [5, 4, 5, 5, 4, 5],
                    "pontuacao": [12.0, 8.0, 4.0, 6.0, 14.0, 7.0],
                    "pontuacao_basica": [7.0, 6.0, 3.0, 5.0, 8.0, 5.0],
                    "G": [1, 0, 0, 0, 1, 0],
                }
            ),
            "confrontos": pd.DataFrame(
                {
                    "clube_id": [10, 20, 10, 20],
                    "opponent_clube_id": [20, 10, 20, 10],
                    "is_mandante": [True, False, True, False],
                    "rodada_id": [2, 2, 3, 3],
                    "partida_id": [102, 102, 103, 103],
                    "placar_clube": [1, 0, 2, 1],
                    "placar_adversario": [0, 1, 1, 2],
                }
            ),
        }

    def load_rodada_id(self):
        return self.rodada_id

    def load_json(self, key):
        return self.json.get(key)

    def load_dataframe(self, key):
        return self.frames.get(key)


@pytest.fixture
def s3_store():
    return S3DicasMemoryStore(
        bucket="cartolapy-test",
        prefix="dicas",
        client=FakeS3Client(),
    )


def test_s3_report_archive_round_trip_and_index(s3_store):
    report = s3_store.save_report(
        {
            "rodada": 4,
            "report_markdown": "# Resumo da rodada\nTexto.",
            "generated_at": "2026-07-18T12:00:00+00:00",
            "model": "openai:test",
        }
    )

    assert s3_store.load_report(report["report_id"]) == report
    assert s3_store.list_reports(rodada=4)[0]["report_id"] == report["report_id"]
    assert s3_store.list_report_rounds() == [4]

    deleted = s3_store.delete_report(report["report_id"])

    assert deleted == report
    assert s3_store.load_report(report["report_id"]) is None
    assert s3_store.list_reports() == []


def test_same_round_is_isolated_between_seasons(s3_store):
    for season_year in [2025, 2026]:
        s3_store.save_round_memory(
            season_year,
            1,
            {
                "schema_version": 1,
                "generated_at": f"{season_year}-04-01T12:00:00+00:00",
                "headline": f"Temporada {season_year}",
            },
        )

    assert s3_store.list_seasons() == [2026, 2025]
    assert s3_store.load_round_memory(2025, 1)["headline"] == "Temporada 2025"
    assert s3_store.load_round_memory(2026, 1)["headline"] == "Temporada 2026"


def test_migrate_legacy_layout_infers_year_from_report_timestamp(s3_store):
    report_id = "legacy-round-19"
    legacy_key = f"reports/rounds/19/{report_id}.json"
    s3_store._put_json(
        legacy_key,
        {
            "report_id": report_id,
            "rodada": 19,
            "report_markdown": "# Resumo",
            "generated_at": "2026-07-04T18:34:40+00:00",
            "model": "openai:test",
        },
    )
    s3_store._put_json(
        "reports/index.json",
        [
            {
                "report_id": report_id,
                "rodada": 19,
                "generated_at": "2026-07-04T18:34:40+00:00",
                "key": legacy_key,
            }
        ],
    )

    result = s3_store.migrate_legacy_layout()

    assert result["reports"] == [report_id]
    assert s3_store.list_seasons() == [2026]
    assert s3_store.load_report(report_id)["season_year"] == 2026
    assert (
        "cartolapy-test",
        "dicas/seasons/2026/reports/rounds/19/legacy-round-19.json",
    ) in s3_store.client.objects
    assert ("cartolapy-test", "dicas/reports/index.json") not in (
        s3_store.client.objects
    )


def test_build_round_source_snapshot_keeps_actuals_and_pre_round_report(s3_store):
    redis_store = FakeRedisStore()
    archived = s3_store.save_report(
        {
            "rodada": 3,
            "report_markdown": "# Dicas\nPrefiro o jogador Dois.",
            "generated_at": "2026-07-17T12:00:00+00:00",
            "model": "openai:test",
        }
    )

    snapshot = build_round_source_snapshot(
        redis_store, s3_store, season_year=2026, rodada=3
    )

    assert snapshot["season_year"] == 2026
    assert snapshot["rodada"] == 3
    assert snapshot["actuals"]["players_with_points"] == 3
    assert snapshot["actuals"]["top_performers"][0]["apelido"] == "Dois"
    assert snapshot["actuals"]["top_performers"][0]["pontuacao"] == 14.0
    assert snapshot["actuals"]["top_performers"][0]["clube"] == "Time A"
    assert snapshot["actuals"]["top_performers"][0]["matchup"] == {
        "partida_id": 103,
        "adversario_id": 20,
        "adversario": "Time B",
        "is_mandante": True,
        "mando": "mandante",
        "placar_clube": 2,
        "placar_adversario": 1,
        "confronto": "Time A 2 x 1 Time B",
    }
    assert snapshot["actuals"]["matchups"][0]["confronto"] == "Time A 2 x 1 Time B"
    assert snapshot["actuals"]["matchups"][0]["top_performers"][0]["scouts"] == {"G": 1}
    assert snapshot["pre_round_report"]["report_id"] == archived["report_id"]


@pytest.mark.anyio
async def test_daily_refresh_backfills_recent_rounds_once(s3_store, monkeypatch):
    redis_store = FakeRedisStore()
    monkeypatch.setenv("DICAS_MEMORY_LOOKBACK_ROUNDS", "2")

    async def fake_synthesis(source):
        return {
            "headline": f"Resumo da rodada {source['rodada']}",
            "best_insights": ["Insight factual"],
            "position_lessons": [],
            "matchup_lessons": [
                {
                    "matchup": "Time A x Time B",
                    "lesson": "O confronto importou.",
                    "evidence": "Um, do Time A, marcou contra o Time B.",
                }
            ],
            "prediction_lessons": [],
            "signals_to_reuse": ["Forma recente"],
            "risks_to_watch": ["Amostra curta"],
        }

    monkeypatch.setattr(dicas_memory, "synthesize_round_memory", fake_synthesis)

    first = await refresh_round_memories(redis_store, s3_store)
    second = await refresh_round_memories(redis_store, s3_store)

    assert first["created"] == [2, 3]
    assert first["skipped"] == []
    assert second["created"] == []
    assert second["skipped"] == [2, 3]
    assert s3_store.load_round_source(2026, 3)["rodada"] == 3
    assert s3_store.load_round_memory(2026, 3)["headline"] == "Resumo da rodada 3"


@pytest.mark.anyio
async def test_forced_refresh_regenerates_existing_memories(s3_store, monkeypatch):
    redis_store = FakeRedisStore()
    monkeypatch.setenv("DICAS_MEMORY_LOOKBACK_ROUNDS", "1")

    async def fake_synthesis(source):
        return {
            "headline": f"Matchups da rodada {source['rodada']}",
            "best_insights": [],
            "position_lessons": [],
            "matchup_lessons": [
                {
                    "matchup": "Time A x Time B",
                    "lesson": "O confronto importou.",
                    "evidence": "Dois, do Time A, marcou contra o Time B.",
                }
            ],
            "prediction_lessons": [],
            "signals_to_reuse": [],
            "risks_to_watch": [],
        }

    monkeypatch.setattr(dicas_memory, "synthesize_round_memory", fake_synthesis)
    await refresh_round_memories(redis_store, s3_store)
    result = await refresh_round_memories(redis_store, s3_store, force=True)

    assert result["created"] == []
    assert result["regenerated"] == [3]
    assert s3_store.load_round_memory(2026, 3)["schema_version"] == 2


def test_prediction_memory_only_returns_rounds_before_target(s3_store):
    for rodada in [2, 3, 4]:
        s3_store.save_round_memory(
            2026,
            rodada,
            {
                "schema_version": 1,
                "generated_at": f"2026-07-{rodada:02d}T12:00:00+00:00",
                "headline": f"Rodada {rodada}",
                "best_insights": [],
                "position_lessons": [],
                "prediction_lessons": [],
                "signals_to_reuse": [],
                "risks_to_watch": [],
            },
        )

    result = load_memories_for_prediction(
        rodada=4,
        season_year=2026,
        limit=2,
        memory_store=s3_store,
    )

    assert result["available"] is True
    assert result["rounds"] == [3, 2]
    assert all(memory["rodada"] < 4 for memory in result["memories"])


def test_s3_payloads_are_json_files(s3_store):
    s3_store.save_round_memory(
        2026,
        3,
        {
            "schema_version": 1,
            "generated_at": "2026-07-18T12:00:00+00:00",
            "headline": "Memória",
        },
    )

    body = s3_store.client.objects[
        (
            "cartolapy-test",
            "dicas/seasons/2026/memories/rounds/rodada-3.json",
        )
    ]

    assert json.loads(body)["headline"] == "Memória"
