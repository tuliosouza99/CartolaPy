from unittest.mock import AsyncMock, MagicMock

import pandas as pd
import pytest
from fastapi.testclient import TestClient

from backend.dependencies import get_redis_store
from backend.services.enums import Scout


class TestAtletasUnifiedEndpoint:
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
                "atleta_id": [1, 2],
                "posicao_id": [1, 2],
                "clube_id": [10, 20],
                "rodada_id": [1, 2],
                "pontuacao": [10.5, 8.0],
                "pontuacao_basica": [10, 8],
                **{scout: [1, 0] for scout in Scout.as_list()},
            }
        )

        confrontos_df = pd.DataFrame(
            {
                "atleta_id": [1, 2],
                "clube_id": [10, 20],
                "opponent_clube_id": [20, 10],
                "is_mandante": [True, False],
                "rodada_id": [1, 2],
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
        mock_redis_store.exists = MagicMock(return_value=True)

        fastapi_app.state.data_loader = mock_data_loader
        fastapi_app.state.redis_store = mock_redis_store
        fastapi_app.dependency_overrides[get_redis_store] = lambda: mock_redis_store

        return fastapi_app

    @pytest.fixture
    def client(self, fastapi_app_with_mock_data):
        return TestClient(fastapi_app_with_mock_data)

    def test_returns_correct_structure(self, client):
        response = client.get("/api/tables/atletas-unified")
        assert response.status_code == 200
        data = response.json()
        assert "total" in data
        assert "page" in data
        assert "page_size" in data
        assert "data" in data
        assert "sort_by" in data
        assert "sort_direction" in data

    def test_returns_all_atletas(self, client):
        response = client.get("/api/tables/atletas-unified")
        assert response.status_code == 200
        data = response.json()
        assert data["total"] == 3

    def test_calculates_media_for_interval(self, client):
        response = client.get("/api/tables/atletas-unified?rodada_min=1&rodada_max=2")
        assert response.status_code == 200
        data = response.json()
        player_a = next(r for r in data["data"] if r["apelido"] == "Player A")
        assert player_a["media"] == 10.5
        assert player_a["total_jogos"] == 1

    def test_calculates_media_for_player_with_multiple_games(self, client):
        response = client.get("/api/tables/atletas-unified?rodada_min=1&rodada_max=3")
        assert response.status_code == 200
        data = response.json()
        player_a = next(r for r in data["data"] if r["apelido"] == "Player A")
        assert player_a["media"] == 10.5
        assert player_a["total_jogos"] == 1

    def test_player_without_games_returns_zero(self, client):
        response = client.get("/api/tables/atletas-unified?rodada_min=1&rodada_max=2")
        assert response.status_code == 200
        data = response.json()
        player_c = next(r for r in data["data"] if r["apelido"] == "Player C")
        assert player_c["media"] == 0
        assert player_c["media_basica"] == 0
        assert player_c["total_jogos"] == 0

    def test_maps_clube_escudo(self, client):
        response = client.get("/api/tables/atletas-unified")
        assert response.status_code == 200
        data = response.json()
        player_a = next(r for r in data["data"] if r["apelido"] == "Player A")
        assert player_a["clube_escudo"] == "https://escudo/10.png"

    def test_maps_posicao_abreviacao_uppercase(self, client):
        response = client.get("/api/tables/atletas-unified")
        assert response.status_code == 200
        data = response.json()
        player_a = next(r for r in data["data"] if r["apelido"] == "Player A")
        assert player_a["posicao_abreviacao"] == "ATA"

    def test_maps_status_nome_and_cor(self, client):
        response = client.get("/api/tables/atletas-unified")
        assert response.status_code == 200
        data = response.json()
        player_a = next(r for r in data["data"] if r["apelido"] == "Player A")
        assert player_a["status_nome"] == "Provável"
        assert player_a["status_cor"] == "green"

    def test_pagination_works(self, client):
        response = client.get("/api/tables/atletas-unified?page=1&page_size=2")
        assert response.status_code == 200
        data = response.json()
        assert data["total"] == 3
        assert len(data["data"]) == 2
        assert data["page"] == 1

    def test_pagination_page_2(self, client):
        response = client.get("/api/tables/atletas-unified?page=2&page_size=2")
        assert response.status_code == 200
        data = response.json()
        assert data["total"] == 3
        assert len(data["data"]) == 1

    def test_sorting_works(self, client):
        response = client.get(
            "/api/tables/atletas-unified?sort_by=apelido&sort_direction=asc"
        )
        assert response.status_code == 200
        data = response.json()
        assert data["data"][0]["apelido"] == "Player A"
        assert data["sort_by"] == "apelido"
        assert data["sort_direction"] == "asc"

    def test_sorting_descending(self, client):
        response = client.get(
            "/api/tables/atletas-unified?sort_by=apelido&sort_direction=desc"
        )
        assert response.status_code == 200
        data = response.json()
        assert data["data"][0]["apelido"] == "Player C"

    def test_rodada_min_filter(self, client):
        response = client.get("/api/tables/atletas-unified?rodada_min=2&rodada_max=2")
        assert response.status_code == 200
        data = response.json()
        player_b = next(r for r in data["data"] if r["apelido"] == "Player B")
        assert player_b["media"] == 8.0
        assert player_b["total_jogos"] == 1

    def test_rodada_max_filter(self, client):
        response = client.get("/api/tables/atletas-unified?rodada_max=1")
        assert response.status_code == 200
        data = response.json()
        player_a = next(r for r in data["data"] if r["apelido"] == "Player A")
        assert player_a["media"] == 10.5


class TestMandanteFilter:
    @pytest.fixture
    def fastapi_app_with_mandante_data(self, fastapi_app):
        atletas_df = pd.DataFrame(
            {
                "atleta_id": [1, 2],
                "rodada_id": [15, 15],
                "clube_id": [10, 20],
                "posicao_id": [1, 1],
                "status_id": [7, 7],
                "preco_num": [100.0, 50.0],
                "apelido": ["Player A", "Player B"],
            }
        )

        pontuacoes_df = pd.DataFrame(
            {
                "atleta_id": [1, 1, 2],
                "posicao_id": [1, 1, 1],
                "clube_id": [10, 10, 20],
                "rodada_id": [1, 2, 1],
                "pontuacao": [10.0, 5.0, 8.0],
                "pontuacao_basica": [10, 5, 8],
                **{scout: [0, 0, 0] for scout in Scout.as_list()},
            }
        )

        confrontos_df = pd.DataFrame(
            {
                "atleta_id": [1, 1, 2],
                "clube_id": [10, 10, 20],
                "opponent_clube_id": [20, 30, 10],
                "is_mandante": [True, True, False],
                "rodada_id": [1, 2, 1],
            }
        )

        mock_atletas = MagicMock()
        mock_atletas.rodada_id = 15
        mock_atletas.df = atletas_df

        mock_pontuacoes = MagicMock()
        mock_pontuacoes.df = pontuacoes_df

        mock_confrontos = MagicMock()
        mock_confrontos.df = confrontos_df

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
                "clubes": {
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
                },
                "posicoes": {
                    "1": {"id": 1, "nome": "Atacante", "abreviacao": "ATA"},
                },
                "status": {
                    "7": {"id": 7, "nome": "Provável"},
                },
            }.get(key)

        mock_redis_store.load_json = MagicMock(side_effect=load_json_side_effect)
        mock_redis_store.exists = MagicMock(return_value=True)

        fastapi_app.state.data_loader = mock_data_loader
        fastapi_app.state.redis_store = mock_redis_store
        fastapi_app.dependency_overrides[get_redis_store] = lambda: mock_redis_store

        return fastapi_app

    @pytest.fixture
    def client(self, fastapi_app_with_mandante_data):
        return TestClient(fastapi_app_with_mandante_data)

    def test_geral_returns_all_matches(self, client):
        response = client.get(
            "/api/tables/atletas-unified?is_mandante=geral&rodada_min=1&rodada_max=2"
        )
        assert response.status_code == 200
        data = response.json()
        player_a = next(r for r in data["data"] if r["apelido"] == "Player A")
        assert player_a["media"] == 7.5

    def test_mandante_only_returns_mandante_games(self, client):
        response = client.get(
            "/api/tables/atletas-unified?is_mandante=mandante&rodada_min=1&rodada_max=2"
        )
        assert response.status_code == 200
        data = response.json()
        player_a = next(r for r in data["data"] if r["apelido"] == "Player A")
        assert player_a["media"] == 7.5

    def test_visitante_only_returns_visitante_games(self, client):
        response = client.get(
            "/api/tables/atletas-unified?is_mandante=visitante&rodada_min=1&rodada_max=2"
        )
        assert response.status_code == 200
        data = response.json()
        player_a = next(r for r in data["data"] if r["apelido"] == "Player A")
        assert player_a["media"] == 0


class TestProximoJogoEndpoint:
    @pytest.fixture
    def fastapi_app_with_proximo_jogo(self, fastapi_app):
        atletas_df = pd.DataFrame(
            {
                "atleta_id": [1],
                "rodada_id": [15],
                "clube_id": [10],
                "posicao_id": [1],
                "status_id": [7],
                "preco_num": [100.0],
                "apelido": ["Player A"],
            }
        )

        confrontos_df = pd.DataFrame(
            {
                "clube_id": [10],
                "opponent_clube_id": [30],
                "is_mandante": [True],
                "rodada_id": [16],
            }
        )

        mock_atletas = MagicMock()
        mock_atletas.rodada_id = 15
        mock_atletas.df = atletas_df

        mock_request_handler = MagicMock()
        mock_request_handler.make_get_request = AsyncMock(
            return_value={
                "clubes": {
                    "10": {"id": 10, "escudos": {"60x60": "https://escudo/10.png"}},
                    "30": {"id": 30, "escudos": {"60x60": "https://escudo/30.png"}},
                },
                "partidas": [
                    {
                        "clube_casa_id": 10,
                        "clube_visitante_id": 30,
                        "valida": True,
                    }
                ],
                "rodada": 16,
            }
        )

        mock_data_loader = MagicMock()
        mock_data_loader.atletas = mock_atletas
        mock_data_loader.pontuacoes = MagicMock(df=pd.DataFrame())
        mock_data_loader.confrontos = MagicMock()
        mock_data_loader.confrontos.df = confrontos_df
        mock_data_loader.pontos_cedidos = MagicMock(df=pd.DataFrame())
        mock_data_loader.request_handler = mock_request_handler

        mock_redis_store = MagicMock()

        def load_json_side_effect(key):
            if key == "partidas:16":
                return None
            return {
                "clubes": {
                    "10": {"id": 10, "escudos": {"60x60": "https://escudo/10.png"}},
                    "30": {"id": 30, "escudos": {"60x60": "https://escudo/30.png"}},
                },
                "posicoes": {"1": {"abreviacao": "ATA"}},
                "status": {"7": {"nome": "Provável"}},
            }.get(key)

        mock_redis_store.load_json = MagicMock(side_effect=load_json_side_effect)
        mock_redis_store.exists = MagicMock(return_value=True)

        fastapi_app.state.data_loader = mock_data_loader
        fastapi_app.state.redis_store = mock_redis_store
        fastapi_app.dependency_overrides[get_redis_store] = lambda: mock_redis_store

        return fastapi_app

    @pytest.fixture
    def client(self, fastapi_app_with_proximo_jogo):
        return TestClient(fastapi_app_with_proximo_jogo)

    def test_proximo_jogo_returns_correct_structure(self, client):
        response = client.get("/api/proximo-jogo/10")
        assert response.status_code == 200
        data = response.json()
        assert "mandante_escudo" in data
        assert "visitante_escudo" in data
        assert "mandante_id" in data
        assert "visitante_id" in data
        assert "rodada" in data


class TestValidation:
    @pytest.fixture
    def fastapi_app_with_mock_data(self, fastapi_app):
        atletas_df = pd.DataFrame(
            {
                "atleta_id": [1],
                "rodada_id": [15],
                "clube_id": [10],
                "posicao_id": [1],
                "status_id": [7],
                "preco_num": [100.0],
                "apelido": ["Player A"],
            }
        )

        mock_atletas = MagicMock()
        mock_atletas.rodada_id = 15
        mock_atletas.df = atletas_df

        mock_request_handler = MagicMock()
        mock_request_handler.make_get_request = AsyncMock(return_value={"partidas": []})

        mock_data_loader = MagicMock()
        mock_data_loader.atletas = mock_atletas
        mock_data_loader.pontuacoes = MagicMock(df=pd.DataFrame())
        mock_data_loader.confrontos = MagicMock(df=pd.DataFrame())
        mock_data_loader.pontos_cedidos = MagicMock(df=pd.DataFrame())
        mock_data_loader.request_handler = mock_request_handler

        mock_redis_store = MagicMock()

        def load_json_side_effect(key):
            if key.startswith("partidas:"):
                return None
            return None

        mock_redis_store.load_json = MagicMock(side_effect=load_json_side_effect)
        mock_redis_store.exists = MagicMock(return_value=True)

        fastapi_app.state.data_loader = mock_data_loader
        fastapi_app.state.redis_store = mock_redis_store
        fastapi_app.dependency_overrides[get_redis_store] = lambda: mock_redis_store

        return fastapi_app

    @pytest.fixture
    def client(self, fastapi_app_with_mock_data):
        return TestClient(fastapi_app_with_mock_data)

    def test_invalid_sort_direction_returns_422(self, client):
        response = client.get("/api/tables/atletas-unified?sort_direction=invalid")
        assert response.status_code == 422

    def test_negative_page_returns_422(self, client):
        response = client.get("/api/tables/atletas-unified?page=0")
        assert response.status_code == 422

    def test_invalid_is_mandante_returns_422(self, client):
        response = client.get("/api/tables/atletas-unified?is_mandante=invalid")
        assert response.status_code == 422
