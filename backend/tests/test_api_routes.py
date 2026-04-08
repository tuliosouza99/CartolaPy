from unittest.mock import AsyncMock, MagicMock

import pandas as pd
import pytest
from fastapi.testclient import TestClient
from src.services.enums import Scout


class TestTableResponseStructure:
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
                "rodada_id": [1, 1],
                "pontuacao": [10.5, 8.0],
                "pontuacao_basica": [10, 8],
                **{scout: [1, 0] for scout in Scout.as_list()},
            }
        )

        confrontos_df = pd.DataFrame(
            {
                "clube_id": [10, 20],
                "opponent_clube_id": [20, 10],
                "is_mandante": [True, False],
                "rodada_id": [1, 1],
            }
        )

        pontos_cedidos_df = pd.DataFrame(
            {
                "clube_id": [10, 20],
                "posicao_id": [1, 2],
                "is_mandante": [True, False],
                "rodada_id": [1, 1],
                "pontuacao": [5.0, 3.0],
                "pontuacao_basica": [5, 3],
                **{scout: [1, 0] for scout in Scout.as_list()},
            }
        )

        mock_atletas = MagicMock()
        mock_atletas.rodada_id = 15
        mock_atletas.df = atletas_df
        mock_atletas.fill_atletas = AsyncMock(return_value=None)

        mock_confrontos = MagicMock()
        mock_confrontos.df = confrontos_df
        mock_confrontos.fill_confrontos = AsyncMock(return_value=None)

        mock_pontuacoes = MagicMock()
        mock_pontuacoes.df = pontuacoes_df
        mock_pontuacoes.fill_pontuacoes = AsyncMock(return_value=None)

        mock_pontos_cedidos = MagicMock()
        mock_pontos_cedidos.df = pontos_cedidos_df
        mock_pontos_cedidos.fill_pontos_cedidos = MagicMock()

        mock_data_loader = MagicMock()
        mock_data_loader.atletas = mock_atletas
        mock_data_loader.confrontos = mock_confrontos
        mock_data_loader.pontuacoes = mock_pontuacoes
        mock_data_loader.pontos_cedidos = mock_pontos_cedidos

        fastapi_app.state.data_loader = mock_data_loader

        return fastapi_app

    @pytest.fixture
    def client(self, fastapi_app_with_mock_data):
        return TestClient(fastapi_app_with_mock_data)

    def test_atletas_returns_correct_structure(self, client):
        response = client.get("/api/tables/atletas")
        assert response.status_code == 200
        data = response.json()
        assert "total" in data
        assert "page" in data
        assert "page_size" in data
        assert "data" in data
        assert "sort_by" in data
        assert "sort_direction" in data

    def test_pontuacoes_returns_correct_structure(self, client):
        response = client.get("/api/tables/pontuacoes")
        assert response.status_code == 200
        data = response.json()
        assert "total" in data
        assert "page" in data
        assert "page_size" in data
        assert "data" in data

    def test_confrontos_returns_correct_structure(self, client):
        response = client.get("/api/tables/confrontos")
        assert response.status_code == 200
        data = response.json()
        assert "total" in data
        assert "page" in data
        assert "page_size" in data
        assert "data" in data

    def test_pontos_cedidos_returns_correct_structure(self, client):
        response = client.get("/api/tables/pontos-cedidos")
        assert response.status_code == 200
        data = response.json()
        assert "total" in data
        assert "page" in data
        assert "page_size" in data
        assert "data" in data


class TestPagination:
    @pytest.fixture
    def fastapi_app_with_mock_data(self, fastapi_app):
        _ = fastapi_app
        atletas_df = pd.DataFrame(
            {
                "atleta_id": range(1, 101),
                "rodada_id": [15] * 100,
                "clube_id": [10] * 100,
                "posicao_id": [1] * 100,
                "status_id": [7] * 100,
                "preco_num": [100.0] * 100,
                "apelido": [f"Player {i}" for i in range(1, 101)],
            }
        )

        pontuacoes_df = pd.DataFrame(
            {
                "atleta_id": [1, 2],
                "posicao_id": [1, 2],
                "clube_id": [10, 20],
                "rodada_id": [1, 1],
                "pontuacao": [10.5, 8.0],
                "pontuacao_basica": [10, 8],
            }
        )

        confrontos_df = pd.DataFrame(
            {
                "clube_id": [10, 20],
                "opponent_clube_id": [20, 10],
                "is_mandante": [True, False],
                "rodada_id": [1, 1],
            }
        )

        pontos_cedidos_df = pd.DataFrame(
            {
                "clube_id": [10, 20],
                "posicao_id": [1, 2],
                "is_mandante": [True, False],
                "rodada_id": [1, 1],
                "pontuacao": [5.0, 3.0],
                "pontuacao_basica": [5, 3],
            }
        )

        mock_redis_store = MagicMock()
        mock_redis_store.load_dataframe = MagicMock(
            side_effect=lambda key: {
                "atletas": atletas_df,
                "pontuacoes": pontuacoes_df,
                "confrontos": confrontos_df,
                "pontos_cedidos": pontos_cedidos_df,
            }.get(key, pd.DataFrame())
        )

        fastapi_app.state.redis_store = mock_redis_store

        from src.tkq import broker

        broker.state.redis_store = mock_redis_store

        return fastapi_app

    @pytest.fixture
    def client(self, fastapi_app_with_mock_data):
        return TestClient(fastapi_app_with_mock_data)

    def test_pagination_defaults(self, client):
        response = client.get("/api/tables/atletas")
        data = response.json()
        assert data["total"] == 100
        assert data["page"] == 1
        assert data["page_size"] == 20
        assert len(data["data"]) == 20

    def test_pagination_page_2(self, client):
        response = client.get("/api/tables/atletas?page=2&page_size=20")
        data = response.json()
        assert data["total"] == 100
        assert data["page"] == 2
        assert data["page_size"] == 20
        assert len(data["data"]) == 20

    def test_pagination_custom_page_size(self, client):
        response = client.get("/api/tables/atletas?page=1&page_size=10")
        data = response.json()
        assert data["total"] == 100
        assert data["page"] == 1
        assert data["page_size"] == 10
        assert len(data["data"]) == 10

    def test_pagination_last_page(self, client):
        response = client.get("/api/tables/atletas?page=5&page_size=20")
        data = response.json()
        assert data["page"] == 5
        assert len(data["data"]) == 20

    def test_pagination_exceeds_data(self, client):
        response = client.get("/api/tables/atletas?page=100&page_size=20")
        data = response.json()
        assert data["page"] == 100
        assert len(data["data"]) == 0


class TestSorting:
    @pytest.fixture
    def fastapi_app_with_mock_data(self, fastapi_app):
        atletas_df = pd.DataFrame(
            {
                "atleta_id": [1, 2, 3],
                "rodada_id": [15, 15, 15],
                "clube_id": [20, 10, 30],
                "posicao_id": [1, 2, 1],
                "status_id": [7, 7, 7],
                "preco_num": [50.0, 100.0, 75.0],
                "apelido": ["Player C", "Player A", "Player B"],
            }
        )

        pontuacoes_df = pd.DataFrame(
            {
                "atleta_id": [1, 2],
                "posicao_id": [1, 2],
                "clube_id": [10, 20],
                "rodada_id": [1, 1],
                "pontuacao": [10.5, 8.0],
                "pontuacao_basica": [10, 8],
            }
        )

        confrontos_df = pd.DataFrame(
            {
                "clube_id": [10, 20],
                "opponent_clube_id": [20, 10],
                "is_mandante": [True, False],
                "rodada_id": [1, 1],
            }
        )

        pontos_cedidos_df = pd.DataFrame(
            {
                "clube_id": [10, 20],
                "posicao_id": [1, 2],
                "is_mandante": [True, False],
                "rodada_id": [1, 1],
                "pontuacao": [5.0, 3.0],
                "pontuacao_basica": [5, 3],
            }
        )

        mock_redis_store = MagicMock()
        mock_redis_store.load_dataframe = MagicMock(
            side_effect=lambda key: {
                "atletas": atletas_df,
                "pontuacoes": pontuacoes_df,
                "confrontos": confrontos_df,
                "pontos_cedidos": pontos_cedidos_df,
            }.get(key, pd.DataFrame())
        )

        fastapi_app.state.redis_store = mock_redis_store

        from src.tkq import broker

        broker.state.redis_store = mock_redis_store

        return fastapi_app

    @pytest.fixture
    def client(self, fastapi_app_with_mock_data):
        return TestClient(fastapi_app_with_mock_data)

    def test_sort_ascending(self, client):
        response = client.get("/api/tables/atletas?sort_by=apelido&sort_direction=asc")
        data = response.json()
        assert data["sort_by"] == "apelido"
        assert data["sort_direction"] == "asc"
        assert data["data"][0]["apelido"] == "Player A"
        assert data["data"][1]["apelido"] == "Player B"
        assert data["data"][2]["apelido"] == "Player C"

    def test_sort_descending(self, client):
        response = client.get("/api/tables/atletas?sort_by=apelido&sort_direction=desc")
        data = response.json()
        assert data["sort_by"] == "apelido"
        assert data["sort_direction"] == "desc"
        assert data["data"][0]["apelido"] == "Player C"
        assert data["data"][1]["apelido"] == "Player B"
        assert data["data"][2]["apelido"] == "Player A"

    def test_sort_by_numeric_column(self, client):
        response = client.get(
            "/api/tables/atletas?sort_by=preco_num&sort_direction=asc"
        )
        data = response.json()
        assert data["data"][0]["preco_num"] == 50.0
        assert data["data"][-1]["preco_num"] == 100.0

    def test_sort_by_clube_id(self, client):
        response = client.get("/api/tables/atletas?sort_by=clube_id&sort_direction=asc")
        data = response.json()
        assert data["data"][0]["clube_id"] == 10
        assert data["data"][1]["clube_id"] == 20
        assert data["data"][2]["clube_id"] == 30


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

        pontuacoes_df = pd.DataFrame(
            {
                "atleta_id": [1],
                "posicao_id": [1],
                "clube_id": [10],
                "rodada_id": [1],
                "pontuacao": [10.5],
                "pontuacao_basica": [10],
            }
        )

        confrontos_df = pd.DataFrame(
            {
                "clube_id": [10],
                "opponent_clube_id": [20],
                "is_mandante": [True],
                "rodada_id": [1],
            }
        )

        pontos_cedidos_df = pd.DataFrame(
            {
                "clube_id": [10],
                "posicao_id": [1],
                "is_mandante": [True],
                "rodada_id": [1],
                "pontuacao": [5.0],
                "pontuacao_basica": [5],
            }
        )

        mock_redis_store = MagicMock()
        mock_redis_store.load_dataframe = MagicMock(
            side_effect=lambda key: {
                "atletas": atletas_df,
                "pontuacoes": pontuacoes_df,
                "confrontos": confrontos_df,
                "pontos_cedidos": pontos_cedidos_df,
            }.get(key, pd.DataFrame())
        )

        fastapi_app.state.redis_store = mock_redis_store

        from src.tkq import broker

        broker.state.redis_store = mock_redis_store

        return fastapi_app

    @pytest.fixture
    def client(self, fastapi_app_with_mock_data):
        return TestClient(fastapi_app_with_mock_data)

    def test_invalid_sort_direction_returns_422(self, client):
        response = client.get("/api/tables/atletas?sort_direction=invalid")
        assert response.status_code == 422

    def test_invalid_sort_direction_case_sensitive(self, client):
        response = client.get("/api/tables/atletas?sort_direction=ASC")
        assert response.status_code == 422

    def test_invalid_sort_by_column_returns_422(self, client):
        response = client.get("/api/tables/atletas?sort_by=invalid_column")
        assert response.status_code == 422

    def test_negative_page_returns_422(self, client):
        response = client.get("/api/tables/atletas?page=0")
        assert response.status_code == 422

    def test_negative_page_size_returns_422(self, client):
        response = client.get("/api/tables/atletas?page_size=-1")
        assert response.status_code == 422


class TestConfrontosEndpoint:
    @pytest.fixture
    def fastapi_app_with_mock_data(self, fastapi_app):
        atletas_df = pd.DataFrame(
            {
                "atleta_id": [1, 2, 3, 4],
                "rodada_id": [10, 10, 10, 10],
                "clube_id": [10, 10, 20, 20],
                "posicao_id": [1, 2, 2, 1],
                "status_id": [7, 7, 7, 7],
                "preco_num": [100.0, 50.0, 75.0, 80.0],
                "apelido": ["Goalkeeper A", "Forward A", "Forward B", "Goalkeeper B"],
            }
        )

        pontuacoes_df = pd.DataFrame(
            {
                "atleta_id": [1, 2, 3, 4],
                "posicao_id": [1, 2, 2, 1],
                "clube_id": [10, 10, 20, 20],
                "rodada_id": [10, 10, 10, 10],
                "pontuacao": [6.5, 8.0, 5.5, 4.0],
                "pontuacao_basica": [6, 7, 5, 4],
                "G": [0, 1, 0, 0],
                "A": [0, 1, 0, 0],
                "FD": [0, 2, 1, 0],
                "FF": [0, 1, 0, 0],
                "FS": [0, 0, 1, 0],
                "DS": [2, 0, 0, 1],
                "SG": [1, 0, 0, 0],
                "CA": [0, 0, 1, 0],
            }
        )

        mock_atletas = MagicMock()
        mock_atletas.rodada_id = 15
        mock_atletas.df = atletas_df
        mock_atletas.fill_atletas = AsyncMock(return_value=None)

        mock_pontuacoes = MagicMock()
        mock_pontuacoes.df = pontuacoes_df
        mock_pontuacoes.fill_pontuacoes = AsyncMock(return_value=None)

        mock_request_handler = MagicMock()
        mock_request_handler.make_get_request = AsyncMock(
            return_value={
                "clubes": {
                    "10": {
                        "nome": "Clube Mandante",
                        "escudos": {"60x60": "http://escudo/mandante.png"},
                    },
                    "20": {
                        "nome": "Clube Visitante",
                        "escudos": {"60x60": "http://escudo/visitante.png"},
                    },
                },
                "partidas": [
                    {
                        "partida_id": 1001,
                        "clube_casa_id": 10,
                        "clube_visitante_id": 20,
                        "valida": True,
                        "placar_oficial_mandante": 1,
                        "placar_oficial_visitante": 1,
                    },
                ],
            }
        )

        mock_data_loader = MagicMock()
        mock_data_loader.atletas = mock_atletas
        mock_data_loader.pontuacoes = mock_pontuacoes
        mock_data_loader.request_handler = mock_request_handler

        mock_redis_store = MagicMock()
        mock_redis_store.load_json = MagicMock(return_value=None)
        mock_redis_store.save_json = MagicMock()
        mock_redis_store.save_last_updated = MagicMock()
        mock_redis_store.load_dataframe = MagicMock(
            side_effect=lambda key: {
                "atletas": atletas_df,
                "pontuacoes": pontuacoes_df,
            }.get(key, pd.DataFrame())
        )

        fastapi_app.state.data_loader = mock_data_loader
        fastapi_app.state.redis_store = mock_redis_store

        from src.tkq import broker

        broker.state.redis_store = mock_redis_store

        return fastapi_app

    @pytest.fixture
    def client(self, fastapi_app_with_mock_data):
        return TestClient(fastapi_app_with_mock_data)

    def test_returns_correct_structure(self, client, fastapi_app_with_mock_data):
        from src.tkq import broker

        cached_data = [
            {
                "mandante_id": 10,
                "visitante_id": 20,
                "mandante_escudo": "http://escudo/mandante.png",
                "visitante_escudo": "http://escudo/visitante.png",
                "mandante_nome": "Clube Mandante",
                "visitante_nome": "Clube Visitante",
                "placar_oficial_mandante": 2,
                "placar_oficial_visitante": 1,
                "local": "Estadio X",
                "partida_data": "2024-10-01T20:00:00",
            }
        ]

        def load_json_side_effect(key):
            if key == "partidas:10":
                return cached_data
            if key == "posicoes":
                return {
                    "1": {"id": 1, "nome": "Goleiro", "abreviacao": "GOL"},
                    "2": {"id": 2, "nome": "Atacante", "abreviacao": "ATA"},
                }
            return None

        broker.state.redis_store.load_json = MagicMock(
            side_effect=load_json_side_effect
        )

        response = client.get("/api/confrontos/10")
        assert response.status_code == 200
        data = response.json()

        mandante_players = data["matches"][0]["mandante_players"]
        assert len(mandante_players) == 2
        assert mandante_players[0]["apelido"] == "Goalkeeper A"
        assert mandante_players[1]["apelido"] == "Forward A"

    def test_player_structure(self, client, fastapi_app_with_mock_data):
        from src.tkq import broker

        cached_data = [
            {
                "mandante_id": 10,
                "visitante_id": 20,
                "mandante_escudo": "http://escudo/mandante.png",
                "visitante_escudo": "http://escudo/visitante.png",
                "mandante_nome": "Clube Mandante",
                "visitante_nome": "Clube Visitante",
                "placar_oficial_mandante": 1,
                "placar_oficial_visitante": 0,
                "local": None,
                "partida_data": None,
            }
        ]

        def load_json_side_effect(key):
            if key == "partidas:10":
                return cached_data
            if key == "posicoes":
                return {
                    "1": {"id": 1, "nome": "Goleiro", "abreviacao": "GOL"},
                    "2": {"id": 2, "nome": "Atacante", "abreviacao": "ATA"},
                }
            return None

        broker.state.redis_store.load_json = MagicMock(
            side_effect=load_json_side_effect
        )

        response = client.get("/api/confrontos/10")
        assert response.status_code == 200
        data = response.json()

        player = data["matches"][0]["mandante_players"][0]
        assert "atleta_id" in player
        assert "apelido" in player
        assert "posicao_abreviacao" in player
        assert "pontuacao" in player
        assert "pontuacao_basica" in player
        assert "scouts" in player
        assert player["apelido"] == "Goalkeeper A"
        assert player["pontuacao"] == 6.5
        assert player["pontuacao_basica"] == 6.0
        assert "SG" in player["scouts"]

    def test_returns_matches_for_rodada(self, client, fastapi_app_with_mock_data):
        response = client.get("/api/confrontos/10")
        assert response.status_code == 200
        data = response.json()

        assert "rodada" in data
        assert "matches" in data
        assert data["rodada"] == 10
        assert isinstance(data["matches"], list)

    def test_fetches_from_api_on_cache_miss(self, client, fastapi_app_with_mock_data):
        from src.tkq import broker

        def load_json_side_effect(key):
            if key == "partidas:10":
                return None
            if key == "posicoes":
                return {
                    "1": {"id": 1, "nome": "Goleiro", "abreviacao": "GOL"},
                    "2": {"id": 2, "nome": "Atacante", "abreviacao": "ATA"},
                }
            return None

        broker.state.redis_store.load_json = MagicMock(
            side_effect=load_json_side_effect
        )

        fastapi_app_with_mock_data.state.data_loader.request_handler.make_get_request = AsyncMock(
            return_value={
                "clubes": {
                    "10": {
                        "nome": "Clube Mandante",
                        "escudos": {"60x60": "http://escudo/mandante.png"},
                    },
                    "20": {
                        "nome": "Clube Visitante",
                        "escudos": {"60x60": "http://escudo/visitante.png"},
                    },
                },
                "partidas": [
                    {
                        "partida_id": 1001,
                        "clube_casa_id": 10,
                        "clube_visitante_id": 20,
                        "valida": True,
                        "placar_oficial_mandante": 1,
                        "placar_oficial_visitante": 1,
                        "local": "Estadio Z",
                        "partida_data": "2024-10-02T21:00:00",
                    }
                ],
            }
        )

        response = client.get("/api/confrontos/10")
        assert response.status_code == 200
        data = response.json()
        assert len(data["matches"]) == 1
        assert data["matches"][0]["mandante_id"] == 10
        assert data["matches"][0]["visitante_id"] == 20

    def test_match_includes_partida_id(self, client, fastapi_app_with_mock_data):
        from src.tkq import broker

        cached_data = [
            {
                "partida_id": 1001,
                "mandante_id": 10,
                "visitante_id": 20,
                "mandante_escudo": "http://escudo/mandante.png",
                "visitante_escudo": "http://escudo/visitante.png",
                "mandante_nome": "Clube Mandante",
                "visitante_nome": "Clube Visitante",
                "placar_oficial_mandante": 2,
                "placar_oficial_visitante": 1,
                "local": "Estadio X",
                "partida_data": "2024-10-01T20:00:00",
            }
        ]

        def load_json_side_effect(key):
            if key == "partidas:10":
                return cached_data
            if key == "posicoes":
                return {
                    "1": {"id": 1, "nome": "Goleiro", "abreviacao": "GOL"},
                    "2": {"id": 2, "nome": "Atacante", "abreviacao": "ATA"},
                }
            return None

        broker.state.redis_store.load_json = MagicMock(
            side_effect=load_json_side_effect
        )

        response = client.get("/api/confrontos/10")
        assert response.status_code == 200
        data = response.json()
        assert len(data["matches"]) == 1
        assert "partida_id" in data["matches"][0]
        assert data["matches"][0]["partida_id"] == 1001


class TestPontosCedidosUnifiedMatchesEndpoint:
    @pytest.fixture
    def fastapi_app_with_mock_data(self, fastapi_app):
        from src.services.enums import Scout

        pontuacoes_df = pd.DataFrame(
            {
                "atleta_id": [1, 2],
                "posicao_id": [1, 1],
                "clube_id": [10, 20],
                "rodada_id": [5, 5],
                "pontuacao": [5.0, 3.0],
                "pontuacao_basica": [5, 3],
                **{scout: [1, 0] for scout in Scout.as_list()},
            }
        )

        confrontos_df = pd.DataFrame(
            {
                "clube_id": [10, 20],
                "opponent_clube_id": [20, 10],
                "is_mandante": [True, False],
                "rodada_id": [5, 5],
                "partida_id": [1001, 1001],
            }
        )

        pontos_cedidos_df = pd.DataFrame(
            {
                "clube_id": [10, 20],
                "posicao_id": [1, 1],
                "is_mandante": [True, False],
                "rodada_id": [5, 5],
                "partida_id": [1001, 1001],
                "pontuacao": [5.0, 3.0],
                "pontuacao_basica": [5, 3],
                **{scout: [1, 0] for scout in Scout.as_list()},
            }
        )

        mock_atletas = MagicMock()
        mock_atletas.rodada_id = 15
        mock_atletas.df = pd.DataFrame()

        mock_confrontos = MagicMock()
        mock_confrontos.df = confrontos_df

        mock_pontuacoes = MagicMock()
        mock_pontuacoes.df = pontuacoes_df

        mock_pontos_cedidos = MagicMock()
        mock_pontos_cedidos.df = pontos_cedidos_df

        mock_data_loader = MagicMock()
        mock_data_loader.atletas = mock_atletas
        mock_data_loader.confrontos = mock_confrontos
        mock_data_loader.pontuacoes = mock_pontuacoes
        mock_data_loader.pontos_cedidos = mock_pontos_cedidos

        mock_redis_store = MagicMock()
        mock_redis_store.load_json = MagicMock(
            return_value={
                "10": {
                    "id": 10,
                    "nome_fantasia": "Clube Casa",
                    "escudos": {"60x60": "http://escudo/10.png"},
                },
                "20": {
                    "id": 20,
                    "nome_fantasia": "Clube Fora",
                    "escudos": {"60x60": "http://escudo/20.png"},
                },
            }
        )
        mock_redis_store.load_dataframe = MagicMock(
            side_effect=lambda key: {
                "pontos_cedidos": pontos_cedidos_df,
                "confrontos": confrontos_df,
            }.get(key, pd.DataFrame())
        )

        fastapi_app.state.data_loader = mock_data_loader
        fastapi_app.state.redis_store = mock_redis_store

        from src.tkq import broker

        broker.state.redis_store = mock_redis_store

        return fastapi_app

    @pytest.fixture
    def client(self, fastapi_app_with_mock_data):
        return TestClient(fastapi_app_with_mock_data)

    def test_returns_correct_structure(self, client, fastapi_app_with_mock_data):
        response = client.get(
            "/api/tables/pontos-cedidos-unified/10/matches?rodada_min=1&rodada_max=10&posicao_id=1"
        )
        assert response.status_code == 200
        data = response.json()
        assert "matches" in data
        assert isinstance(data["matches"], list)

    def test_match_contains_required_fields(self, client, fastapi_app_with_mock_data):
        response = client.get(
            "/api/tables/pontos-cedidos-unified/10/matches?rodada_min=1&rodada_max=10&posicao_id=1"
        )
        assert response.status_code == 200
        data = response.json()
        assert len(data["matches"]) >= 1
        match = data["matches"][0]
        assert "partida_id" in match
        assert "rodada_id" in match
        assert "opponent_clube_id" in match
        assert "opponent_nome" in match
        assert "opponent_escudo" in match
        assert "is_mandante" in match
        assert "pontuacao" in match
        assert "pontuacao_basica" in match

    def test_is_mandante_indicator_present(self, client, fastapi_app_with_mock_data):
        response = client.get(
            "/api/tables/pontos-cedidos-unified/10/matches?rodada_min=1&rodada_max=10&posicao_id=1"
        )
        assert response.status_code == 200
        data = response.json()
        for match in data["matches"]:
            assert isinstance(match["is_mandante"], bool)
