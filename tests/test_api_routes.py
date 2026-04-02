from unittest.mock import AsyncMock, MagicMock

import pandas as pd
import pytest
from fastapi.testclient import TestClient

from backend.services.enums import Scout


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

        mock_atletas = MagicMock()
        mock_atletas.rodada_id = 15
        mock_atletas.df = atletas_df

        mock_data_loader = MagicMock()
        mock_data_loader.atletas = mock_atletas
        mock_data_loader.pontuacoes = MagicMock(df=pd.DataFrame())
        mock_data_loader.confrontos = MagicMock(df=pd.DataFrame())
        mock_data_loader.pontos_cedidos = MagicMock(df=pd.DataFrame())

        fastapi_app.state.data_loader = mock_data_loader
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

        mock_atletas = MagicMock()
        mock_atletas.rodada_id = 15
        mock_atletas.df = atletas_df

        mock_data_loader = MagicMock()
        mock_data_loader.atletas = mock_atletas
        mock_data_loader.pontuacoes = MagicMock(df=pd.DataFrame())
        mock_data_loader.confrontos = MagicMock(df=pd.DataFrame())
        mock_data_loader.pontos_cedidos = MagicMock(df=pd.DataFrame())

        fastapi_app.state.data_loader = mock_data_loader
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

        mock_atletas = MagicMock()
        mock_atletas.df = atletas_df

        mock_data_loader = MagicMock()
        mock_data_loader.atletas = mock_atletas
        mock_data_loader.pontuacoes = MagicMock(df=pd.DataFrame())
        mock_data_loader.confrontos = MagicMock(df=pd.DataFrame())
        mock_data_loader.pontos_cedidos = MagicMock(df=pd.DataFrame())

        fastapi_app.state.data_loader = mock_data_loader
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
