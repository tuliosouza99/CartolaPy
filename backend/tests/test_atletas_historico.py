from unittest.mock import AsyncMock, MagicMock

import pandas as pd
import pytest
from fastapi.testclient import TestClient
from src.dependencies import get_redis_store
from src.services.enums import Scout


class TestAtletasHistoricoEndpoint:
    @pytest.fixture
    def fastapi_app_with_mock_data(self, fastapi_app):
        atletas_df = pd.DataFrame(
            {
                "atleta_id": [1, 2, 3],
                "rodada_id": [15, 15, 15],
                "clube_id": [10, 20, 10],
                "posicao_id": [1, 2, 1],
                "status_id": [7, 7, 7],
                "preco_num": [100.0, 50.0, 75.0],
                "apelido": ["Player A", "Player B", "Player C"],
            }
        )

        pontuacoes_df = pd.DataFrame(
            {
                "atleta_id": [1, 1, 1, 2],
                "posicao_id": [1, 1, 1, 2],
                "clube_id": [10, 10, 10, 20],
                "rodada_id": [1, 2, 3, 1],
                "partida_id": [100, 101, 102, 103],
                "pontuacao": [10.5, 8.0, 12.0, 6.5],
                "pontuacao_basica": [10.0, 7.5, 11.0, 6.0],
                **{scout: [1, 0, 2, 0] for scout in Scout.as_list()},
            }
        )

        confrontos_df = pd.DataFrame(
            {
                "clube_id": [10, 10, 10, 20],
                "opponent_clube_id": [20, 30, 40, 10],
                "is_mandante": [True, True, False, False],
                "rodada_id": [1, 2, 3, 1],
                "partida_id": [100, 101, 102, 103],
            }
        )

        clubes_cache = {
            "10": {
                "id": 10,
                "nome": "Team A",
                "escudos": {"60x60": "https://escudo/10.png"},
            },
            "20": {
                "id": 20,
                "nome": "Team B",
                "escudos": {"60x60": "https://escudo/20.png"},
            },
            "30": {
                "id": 30,
                "nome": "Team C",
                "escudos": {"60x60": "https://escudo/30.png"},
            },
            "40": {
                "id": 40,
                "nome": "Team D",
                "escudos": {"60x60": "https://escudo/40.png"},
            },
        }

        posicoes_cache = {
            "1": {"id": 1, "nome": "Atacante", "abreviacao": "ATA"},
            "2": {"id": 2, "nome": "Meia", "abreviacao": "MEI"},
        }

        status_cache = {
            "7": {"id": 7, "nome": "Provável"},
        }

        mock_atletas = MagicMock()
        mock_atletas.rodada_id = 15
        mock_atletas.df = atletas_df
        mock_atletas.fill_atletas = AsyncMock(return_value=None)

        mock_pontuacoes = MagicMock()
        mock_pontuacoes.df = pontuacoes_df
        mock_pontuacoes.fill_pontuacoes = AsyncMock(return_value=None)

        mock_confrontos = MagicMock()
        mock_confrontos.df = confrontos_df
        mock_confrontos.fill_confrontos = AsyncMock(return_value=None)

        mock_request_handler = MagicMock()
        mock_request_handler.make_get_request = AsyncMock(return_value={"partidas": []})

        mock_data_loader = MagicMock()
        mock_data_loader.atletas = mock_atletas
        mock_data_loader.pontuacoes = mock_pontuacoes
        mock_data_loader.confrontos = mock_confrontos
        mock_data_loader.pontos_cedidos = MagicMock(df=pd.DataFrame())
        mock_data_loader.request_handler = mock_request_handler

        mock_redis_store = MagicMock()

        def load_json_side_effect(key):
            if key.startswith("partidas:"):
                return None
            return {
                "clubes": clubes_cache,
                "posicoes": posicoes_cache,
                "status": status_cache,
            }.get(key)

        mock_redis_store.load_json = MagicMock(side_effect=load_json_side_effect)
        mock_redis_store.load_dataframe = MagicMock(
            side_effect=lambda key: {
                "pontuacoes": pontuacoes_df,
                "confrontos": confrontos_df,
            }.get(key, pd.DataFrame())
        )
        mock_redis_store.exists = MagicMock(return_value=True)

        fastapi_app.state.data_loader = mock_data_loader
        fastapi_app.state.redis_store = mock_redis_store
        fastapi_app.dependency_overrides[get_redis_store] = lambda: mock_redis_store

        return fastapi_app

    @pytest.fixture
    def client(self, fastapi_app_with_mock_data):
        return TestClient(fastapi_app_with_mock_data)

    def test_returns_correct_structure(self, client):
        response = client.get(
            "/api/tables/atletas/1/historico?rodada_min=1&rodada_max=3"
        )
        assert response.status_code == 200
        data = response.json()
        assert "atleta_id" in data
        assert "historico" in data
        assert isinstance(data["historico"], list)

    def test_returns_historico_for_atleta(self, client):
        response = client.get(
            "/api/tables/atletas/1/historico?rodada_min=1&rodada_max=3"
        )
        assert response.status_code == 200
        data = response.json()
        assert data["atleta_id"] == 1
        assert len(data["historico"]) == 3

    def test_historico_contains_rodada_id(self, client):
        response = client.get(
            "/api/tables/atletas/1/historico?rodada_min=1&rodada_max=3"
        )
        assert response.status_code == 200
        data = response.json()
        rodadas = [h["rodada_id"] for h in data["historico"]]
        assert 1 in rodadas
        assert 2 in rodadas
        assert 3 in rodadas

    def test_historico_contains_partida_id(self, client):
        response = client.get(
            "/api/tables/atletas/1/historico?rodada_min=1&rodada_max=3"
        )
        assert response.status_code == 200
        data = response.json()
        for h in data["historico"]:
            assert "partida_id" in h
            assert h["partida_id"] > 0

    def test_historico_contains_pontuacao(self, client):
        response = client.get(
            "/api/tables/atletas/1/historico?rodada_min=1&rodada_max=3"
        )
        assert response.status_code == 200
        data = response.json()
        for h in data["historico"]:
            assert "pontuacao" in h
            assert "pontuacao_basica" in h

    def test_historico_contains_is_mandante(self, client):
        response = client.get(
            "/api/tables/atletas/1/historico?rodada_min=1&rodada_max=3"
        )
        assert response.status_code == 200
        data = response.json()
        for h in data["historico"]:
            assert "is_mandante" in h

    def test_historico_contains_opponent_info(self, client):
        response = client.get(
            "/api/tables/atletas/1/historico?rodada_min=1&rodada_max=3"
        )
        assert response.status_code == 200
        data = response.json()
        for h in data["historico"]:
            assert "opponent_clube_id" in h
            assert "opponent_nome" in h
            assert "opponent_escudo" in h

    def test_historico_contains_scouts(self, client):
        response = client.get(
            "/api/tables/atletas/1/historico?rodada_min=1&rodada_max=3"
        )
        assert response.status_code == 200
        data = response.json()
        for h in data["historico"]:
            assert "scouts" in h
            assert isinstance(h["scouts"], dict)

    def test_filters_by_rodada_range(self, client):
        response = client.get(
            "/api/tables/atletas/1/historico?rodada_min=2&rodada_max=2"
        )
        assert response.status_code == 200
        data = response.json()
        assert len(data["historico"]) == 1
        assert data["historico"][0]["rodada_id"] == 2

    def test_filters_by_is_mandante(self, client):
        response = client.get(
            "/api/tables/atletas/1/historico?rodada_min=1&rodada_max=3&is_mandante=mandante"
        )
        assert response.status_code == 200
        data = response.json()
        for h in data["historico"]:
            assert h["is_mandante"] is True

    def test_filters_by_is_mandante_visitante(self, client):
        response = client.get(
            "/api/tables/atletas/1/historico?rodada_min=1&rodada_max=3&is_mandante=visitante"
        )
        assert response.status_code == 200
        data = response.json()
        for h in data["historico"]:
            assert h["is_mandante"] is False

    def test_returns_empty_historico_for_unknown_atleta(self, client):
        response = client.get(
            "/api/tables/atletas/999/historico?rodada_min=1&rodada_max=3"
        )
        assert response.status_code == 200
        data = response.json()
        assert data["atleta_id"] == 999
        assert data["historico"] == []

    def test_historico_sorted_by_rodada_desc(self, client):
        response = client.get(
            "/api/tables/atletas/1/historico?rodada_min=1&rodada_max=3"
        )
        assert response.status_code == 200
        data = response.json()
        rodadas = [h["rodada_id"] for h in data["historico"]]
        assert rodadas == sorted(rodadas, reverse=True)
