from datetime import datetime, timezone
from unittest.mock import AsyncMock

import pandas as pd
import pytest
from fastapi.testclient import TestClient
from src.dependencies import get_redis_store
from src.services.dicas_da_rodada import (
    CartolaPyApiClient,
    DicasEventSink,
    DicasReportCache,
    DEFAULT_DICAS_MODEL,
    _match_snippets_from_text,
    build_matchup_insights_from_store,
    build_position_recommendation_board_from_store,
    evaluate_matchup_strategy_from_store,
    list_archived_reports,
    load_archived_report,
    report_key,
    run_dicas_report_generation,
    search_known_odds_sources_for_matches,
)
from src.services.enums import Scout
from src.tkq import broker


class FakeStore:
    def __init__(self):
        self.json = {}
        self.lists = {}
        self.frames = {}
        self.deleted = []
        self.enqueued = []
        self.rodada_id = 15

    def load_rodada_id(self):
        return self.rodada_id

    def save_json(self, key, data, ttl_seconds=None):
        self.json[key] = data

    def load_json(self, key):
        return self.json.get(key)

    def append_json(self, key, data, ttl_seconds=None):
        self.lists.setdefault(key, []).append(data)
        return len(self.lists[key])

    def load_json_list(self, key):
        return list(self.lists.get(key, []))

    def delete(self, key):
        self.deleted.append(key)
        self.json.pop(key, None)
        return 1

    def expire(self, key, ttl_seconds):
        return True

    def load_dataframe(self, key):
        return self.frames.get(key)


@pytest.fixture
def fake_store():
    store = FakeStore()
    broker.state.redis_store = store
    get_redis_store.cache_clear()
    return store


@pytest.fixture
def client(fastapi_app, fake_store):
    return TestClient(fastapi_app)


def sample_report(rodada=16):
    return {
        "rodada": rodada,
        "report_markdown": "# Resumo da rodada\nTexto.",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "model": DEFAULT_DICAS_MODEL,
        "reasoning_effort": "medium",
        "recommended_spans": [5, 10],
        "sources": [],
    }


def test_get_dicas_returns_cached_report(client, fake_store):
    fake_store.save_json(report_key(16), sample_report())

    response = client.get("/api/dicas-da-rodada")

    assert response.status_code == 200
    payload = response.json()
    assert payload["rodada"] == 16
    assert payload["report"]["report_markdown"].startswith("# Resumo")
    assert payload["active_run"] is None


def test_generate_creates_run_metadata_and_enqueues(client, fake_store, monkeypatch):
    from src.tasks import generate_dicas_da_rodada_task

    async def fake_kiq(*, run_id, rodada):
        fake_store.enqueued.append({"run_id": run_id, "rodada": rodada})

    monkeypatch.setattr(generate_dicas_da_rodada_task, "kiq", fake_kiq)

    response = client.post("/api/dicas-da-rodada/generate")

    assert response.status_code == 200
    payload = response.json()
    assert payload["started"] is True
    assert payload["run"]["status"] == "queued"
    assert fake_store.enqueued == [{"run_id": payload["run"]["run_id"], "rodada": 16}]
    assert fake_store.load_json("dicas:active:16")["run_id"] == payload["run"]["run_id"]


def test_generate_uses_cached_report_without_enqueue(client, fake_store, monkeypatch):
    from src.tasks import generate_dicas_da_rodada_task

    fake_store.save_json(report_key(16), sample_report())
    fake_kiq = AsyncMock()
    monkeypatch.setattr(generate_dicas_da_rodada_task, "kiq", fake_kiq)

    response = client.post("/api/dicas-da-rodada/generate")

    assert response.status_code == 200
    payload = response.json()
    assert payload["started"] is False
    assert payload["report"]["rodada"] == 16
    fake_kiq.assert_not_called()


def test_regenerate_preserves_old_report_until_new_run_finishes(
    client, fake_store, monkeypatch
):
    from src.tasks import generate_dicas_da_rodada_task

    fake_store.save_json(report_key(16), sample_report())

    async def fake_kiq(*, run_id, rodada):
        fake_store.enqueued.append({"run_id": run_id, "rodada": rodada})

    monkeypatch.setattr(generate_dicas_da_rodada_task, "kiq", fake_kiq)

    response = client.post("/api/dicas-da-rodada/regenerate")

    assert response.status_code == 200
    payload = response.json()
    assert payload["started"] is True
    assert payload["report"]["report_markdown"] == "# Resumo da rodada\nTexto."
    assert fake_store.load_json(report_key(16))["report_markdown"] == (
        "# Resumo da rodada\nTexto."
    )


def test_complete_run_archives_report_as_local_json(fake_store, monkeypatch, tmp_path):
    monkeypatch.setenv("DICAS_REPORTS_DIR", str(tmp_path))
    cache = DicasReportCache(fake_store)
    run = cache.create_run(16)

    cache.complete_run(run["run_id"], sample_report())

    cached_report = fake_store.load_json(report_key(16))
    assert cached_report["report_id"]
    archived_report = load_archived_report(cached_report["report_id"])
    assert archived_report["report_markdown"] == "# Resumo da rodada\nTexto."
    assert list_archived_reports()[0]["report_id"] == cached_report["report_id"]


def test_history_endpoints_list_and_load_reports(
    client, fake_store, monkeypatch, tmp_path
):
    monkeypatch.setenv("DICAS_REPORTS_DIR", str(tmp_path))
    cache = DicasReportCache(fake_store)
    run = cache.create_run(16)
    cache.complete_run(run["run_id"], sample_report())
    previous_run = cache.create_run(15)
    cache.complete_run(previous_run["run_id"], sample_report(rodada=15))
    report_id = fake_store.load_json(report_key(16))["report_id"]

    list_response = client.get("/api/dicas-da-rodada/history?rodada=16")
    detail_response = client.get(f"/api/dicas-da-rodada/history/{report_id}")

    assert list_response.status_code == 200
    history_payload = list_response.json()
    assert history_payload["reports"][0]["report_id"] == report_id
    assert all(item["rodada"] == 16 for item in history_payload["reports"])
    assert history_payload["rodadas"] == [16, 15]
    assert detail_response.status_code == 200
    assert detail_response.json()["report_id"] == report_id


def test_delete_history_report_removes_archive_and_current_cache(
    client, fake_store, monkeypatch, tmp_path
):
    monkeypatch.setenv("DICAS_REPORTS_DIR", str(tmp_path))
    cache = DicasReportCache(fake_store)
    run = cache.create_run(16)
    cache.complete_run(run["run_id"], sample_report())
    report_id = fake_store.load_json(report_key(16))["report_id"]

    response = client.delete(f"/api/dicas-da-rodada/history/{report_id}")

    assert response.status_code == 200
    payload = response.json()
    assert payload["deleted"] is True
    assert payload["cleared_current"] is True
    assert load_archived_report(report_id) is None
    assert fake_store.load_json(report_key(16)) is None


def test_delete_history_report_returns_404_for_missing(client):
    response = client.delete("/api/dicas-da-rodada/history/not-here")

    assert response.status_code == 404


def test_stream_replays_existing_events_and_finishes(client, fake_store):
    cache = DicasReportCache(fake_store)
    run = cache.create_run(16)
    cache.append_event(run["run_id"], "progress", "Coletando dados.")
    cache.update_run(run["run_id"], "completed")

    response = client.get(f"/api/dicas-da-rodada/runs/{run['run_id']}/stream")

    assert response.status_code == 200
    assert "event: status" in response.text
    assert "event: progress" in response.text


@pytest.mark.anyio
async def test_missing_openai_key_marks_run_failed(fake_store, monkeypatch):
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    cache = DicasReportCache(fake_store)
    run = cache.create_run(16)

    result = await run_dicas_report_generation(fake_store, run["run_id"], 16)

    assert result["status"] == "failed"
    assert "OPENAI_API_KEY" in result["error"]
    events = fake_store.load_json_list(f"dicas:events:{run['run_id']}")
    assert any(event["type"] == "error" for event in events)


def test_cartolapy_api_client_calls_expected_url(fake_store, monkeypatch):
    captured = []
    cache = DicasReportCache(fake_store)
    run = cache.create_run(16)
    sink = DicasEventSink(cache, run["run_id"])

    class FakeResponse:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def read(self):
            return b'{"ok": true}'

    def fake_urlopen(request, timeout):
        captured.append(request.full_url)
        return FakeResponse()

    monkeypatch.setattr("src.services.dicas_da_rodada.urlopen", fake_urlopen)

    client = CartolaPyApiClient("http://backend:8000", sink)
    payload = client.get("/api/tables/status", {"span": 5})

    assert payload == {"ok": True}
    assert captured == ["http://backend:8000/api/tables/status?span=5"]


def test_matchup_insights_join_player_opponent_position_and_scouts(fake_store):
    scout_values = {scout: [0] for scout in Scout.as_list()}
    scout_values["DS"] = [5]
    fake_store.frames["atletas"] = pd.DataFrame(
        {
            "atleta_id": ["1"],
            "rodada_id": [15],
            "clube_id": [10],
            "posicao_id": [5],
            "status_id": [7],
            "preco_num": [12.0],
            "apelido": ["Atacante DS"],
        }
    )
    fake_store.frames["pontuacoes"] = pd.DataFrame(
        {
            "atleta_id": [1],
            "posicao_id": [5],
            "clube_id": [10],
            "rodada_id": [15],
            "pontuacao": [9.0],
            "pontuacao_basica": [7.0],
            **scout_values,
        }
    )
    fake_store.frames["confrontos"] = pd.DataFrame(
        {
            "clube_id": [10, 20],
            "opponent_clube_id": [20, 10],
            "is_mandante": [True, False],
            "rodada_id": [15, 15],
            "partida_id": [100, 100],
        }
    )
    ceded_scouts = {scout: [0] for scout in Scout.as_list()}
    ceded_scouts["DS"] = [8]
    fake_store.frames["pontos_cedidos"] = pd.DataFrame(
        {
            "clube_id": [20],
            "posicao_id": [5],
            "is_mandante": [False],
            "rodada_id": [15],
            "partida_id": [100],
            "pontuacao": [11.0],
            "pontuacao_basica": [8.0],
            **ceded_scouts,
        }
    )
    fake_store.json["clubes"] = {
        "10": {"nome": "Mandante"},
        "20": {"nome": "Visitante"},
    }
    fake_store.json["posicoes"] = {
        "5": {"id": 5, "nome": "Atacante", "abreviacao": "ata"}
    }
    fake_store.json["status"] = {"7": {"id": 7, "nome": "Provável"}}
    fake_store.json["partidas:16"] = [
        {"mandante_id": 10, "visitante_id": 20, "partida_id": 200}
    ]

    result = build_matchup_insights_from_store(fake_store, rodada=16, span=5)

    assert result["matchups"]
    insight = result["matchups"][0]
    assert insight["apelido"] == "Atacante DS"
    assert insight["adversario_id"] == 20
    assert insight["mando"] == "mandante"
    assert insight["adversario_cede"]["media_cedida"] == 11.0
    assert insight["overlap_scouts"][0]["scout"] == "DS"
    assert insight["confidence"] == "Baixa"
    assert "Amostra curta" in insight["risk_flags"]
    assert insight["edge_summary"]
    assert result["recommendation_board"]["primary_picks"] == []
    assert result["recommendation_board"]["risk_watch"][0]["apelido"] == "Atacante DS"


def test_historical_strategy_eval_grades_picks_against_actual_round(fake_store):
    scout_rows = {scout: [0, 0, 0, 0, 0, 0] for scout in Scout.as_list()}
    scout_rows["DS"] = [5, 2, 5, 1, 4, 1]
    scout_rows["V"] = [1, 0, 1, 0, 1, 0]
    fake_store.frames["atletas"] = pd.DataFrame(
        {
            "atleta_id": [1, 2],
            "rodada_id": [15, 15],
            "clube_id": [10, 20],
            "posicao_id": [5, 5],
            "status_id": [7, 7],
            "preco_num": [10.0, 7.0],
            "apelido": ["Atacante Forte", "Atacante Fraco"],
        }
    )
    fake_store.frames["pontuacoes"] = pd.DataFrame(
        {
            "atleta_id": [1, 2, 1, 2, 1, 2],
            "posicao_id": [5, 5, 5, 5, 5, 5],
            "clube_id": [10, 20, 10, 20, 10, 20],
            "rodada_id": [13, 13, 14, 14, 15, 15],
            "pontuacao": [8.0, 2.0, 9.0, 3.0, 10.0, 1.0],
            "pontuacao_basica": [7.0, 2.0, 8.0, 3.0, 9.0, 1.0],
            **scout_rows,
        }
    )
    fake_store.frames["confrontos"] = pd.DataFrame(
        [
            {
                "clube_id": clube_id,
                "opponent_clube_id": opponent_id,
                "is_mandante": is_mandante,
                "rodada_id": rodada,
                "partida_id": rodada * 100,
            }
            for rodada in [13, 14, 15]
            for clube_id, opponent_id, is_mandante in [(10, 20, True), (20, 10, False)]
        ]
    )
    ceded_scouts = {scout: [0, 0] for scout in Scout.as_list()}
    ceded_scouts["DS"] = [8, 7]
    fake_store.frames["pontos_cedidos"] = pd.DataFrame(
        {
            "clube_id": [20, 20],
            "posicao_id": [5, 5],
            "is_mandante": [False, False],
            "rodada_id": [13, 14],
            "partida_id": [1300, 1400],
            "pontuacao": [11.0, 10.0],
            "pontuacao_basica": [8.0, 8.0],
            **ceded_scouts,
        }
    )
    fake_store.json["clubes"] = {
        "10": {"nome": "Mandante"},
        "20": {"nome": "Visitante"},
    }
    fake_store.json["posicoes"] = {
        "5": {"id": 5, "nome": "Atacante", "abreviacao": "ata"}
    }
    fake_store.json["status"] = {"7": {"id": 7, "nome": "Provável"}}

    result = evaluate_matchup_strategy_from_store(
        fake_store, span=2, lookback_rounds=1, limit_per_round=6
    )

    assert result["available"] is True
    assert result["target_rounds"] == [15]
    assert result["overall"]["played"] >= 1
    assert result["overall"]["hit_rate"] == 1.0
    assert result["overall"]["top_quartile_rate"] == 1.0
    assert result["overall"]["team_win_rate"] == 1.0


def test_position_recommendation_board_returns_position_groups(fake_store):
    scout_values = {scout: [0, 0] for scout in Scout.as_list()}
    scout_values["DS"] = [5, 3]
    fake_store.frames["atletas"] = pd.DataFrame(
        {
            "atleta_id": [1, 2],
            "rodada_id": [15, 15],
            "clube_id": [10, 10],
            "posicao_id": [5, 5],
            "status_id": [7, 7],
            "preco_num": [12.0, 8.0],
            "apelido": ["Atacante A", "Atacante B"],
        }
    )
    fake_store.frames["pontuacoes"] = pd.DataFrame(
        {
            "atleta_id": [1, 2],
            "posicao_id": [5, 5],
            "clube_id": [10, 10],
            "rodada_id": [15, 15],
            "pontuacao": [9.0, 6.0],
            "pontuacao_basica": [7.0, 5.0],
            **scout_values,
        }
    )
    fake_store.frames["confrontos"] = pd.DataFrame(
        {
            "clube_id": [10, 20],
            "opponent_clube_id": [20, 10],
            "is_mandante": [True, False],
            "rodada_id": [15, 15],
            "partida_id": [100, 100],
        }
    )
    ceded_scouts = {scout: [0] for scout in Scout.as_list()}
    ceded_scouts["DS"] = [8]
    fake_store.frames["pontos_cedidos"] = pd.DataFrame(
        {
            "clube_id": [20],
            "posicao_id": [5],
            "is_mandante": [False],
            "rodada_id": [15],
            "partida_id": [100],
            "pontuacao": [11.0],
            "pontuacao_basica": [8.0],
            **ceded_scouts,
        }
    )
    fake_store.json["clubes"] = {"10": {"nome": "Mandante"}, "20": {"nome": "Fora"}}
    fake_store.json["posicoes"] = {
        "5": {"id": 5, "nome": "Atacante", "abreviacao": "ata"}
    }
    fake_store.json["status"] = {"7": {"id": 7, "nome": "Provável"}}
    fake_store.json["partidas:16"] = [{"mandante_id": 10, "visitante_id": 20}]

    board = build_position_recommendation_board_from_store(
        fake_store, rodada=16, spans=(5,), picks_per_position=7
    )

    assert board["position_picks"]["5"] == []
    assert len(board["low_confidence_watch"]["5"]) == 2
    assert board["low_confidence_watch"]["5"][0]["apelido"] == "Atacante A"
    assert "resumo_humano" in board["low_confidence_watch"]["5"][0]


def test_known_odds_sources_cross_reference_match_snippets(monkeypatch):
    class FakeResponse:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def read(self):
            return (
                b"<html><body>Corinthians vs Remo 1 X 2 1.60 3.40 6.20 "
                b"Chapecoense vs Flamengo 7.50 4.50 1.50</body></html>"
            )

    def fake_urlopen(request, timeout, context=None):
        return FakeResponse()

    monkeypatch.setattr("src.services.dicas_da_rodada.urlopen", fake_urlopen)

    result = search_known_odds_sources_for_matches(
        [{"mandante_nome": "Corinthians", "visitante_nome": "Remo"}]
    )

    assert result["available"] is True
    assert result["matches"]
    assert result["matches"][0]["mandante_nome"] == "Corinthians"


def test_match_snippets_require_single_vs_clause():
    text = (
        "São Paulo 2.13 vs 1.63 Bahia 2.05 3.30 3.50 "
        "Cruzeiro 1.60 vs 0.90 Atlético Mineiro 1.85 3.40 4.40 "
        "Athletico PR 2.30 vs 0.44 Grêmio"
    )

    assert _match_snippets_from_text(text, "São Paulo", "Athlético-PR") == []
