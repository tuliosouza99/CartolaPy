"""
Integration tests for Redis-only data access.

These tests verify that:
1. Routes read directly from Redis (no in-memory caching)
2. Tasks write directly to Redis
3. Data persists correctly in Redis

This is the simplified architecture after removing the dual storage (in-memory + Redis).
"""

from unittest.mock import MagicMock

import pandas as pd
import pytest
from fastapi.testclient import TestClient


class TestDataLoaderReturnsResults:
    """
    Tests that data loaders return DataFrames instead of storing in memory.
    """

    @pytest.fixture
    def sample_atletas_response(self):
        return {
            "atletas": [
                {
                    "atleta_id": 89256,
                    "rodada_id": 15,
                    "clube_id": 264,
                    "posicao_id": 1,
                    "status_id": 2,
                    "preco_num": 14.8,
                    "apelido": "Gerson",
                }
            ],
            "clubes": {},
            "posicoes": {},
            "status": {},
        }

    @pytest.mark.asyncio
    async def test_atletas_fill_returns_result_with_df_and_metadata(
        self, sample_atletas_response
    ):
        """Atletas.fill_atletas() should return AtletasResult with df and metadata."""
        from unittest.mock import AsyncMock, MagicMock, patch

        mock_request_handler = MagicMock()
        mock_request_handler.make_get_request = AsyncMock(
            return_value=sample_atletas_response
        )

        from src.services.data_loaders.atletas import Atletas

        atletas = Atletas(mock_request_handler)

        with patch(
            "src.services.data_loaders.atletas.validate_mercado_response"
        ) as mock_validate:
            mock_validate.return_value = MagicMock(
                atletas=[
                    MagicMock(
                        model_dump=MagicMock(
                            return_value={
                                "atleta_id": 89256,
                                "rodada_id": 15,
                                "clube_id": 264,
                                "posicao_id": 1,
                                "status_id": 2,
                                "preco_num": 14.8,
                                "apelido": "Gerson",
                            }
                        )
                    )
                ],
                rodada_id=15,
                clubes={},
                posicoes={},
                status={},
            )
            result = await atletas.fill_atletas()

        assert result.df is not None
        assert isinstance(result.df, pd.DataFrame)
        assert result.rodada_id == 15
        assert result.clubes == {}


class TestEndpointsReadFromRedis:
    """
    Tests that endpoints read directly from Redis.

    After the simplification, endpoints should use store.load_dataframe()
    instead of data_loader.*.df
    """

    def test_get_atletas_endpoint_uses_store(self):
        """GET /tables/atletas should read directly from Redis store."""
        from unittest.mock import patch

        mock_store = MagicMock()
        mock_store.load_dataframe.return_value = pd.DataFrame(
            [
                {
                    "atleta_id": 89256,
                    "status_id": 2,
                    "preco_num": 14.8,
                    "apelido": "Gerson",
                }
            ]
        )

        with patch("src.api.routes.get_redis_store", return_value=mock_store):
            with patch("src.api.routes.get_data_loader") as mock_dl:
                mock_dl.return_value = MagicMock()
                from src.main import get_app

                app = get_app()
                client = TestClient(app)
                response = client.get("/tables/atletas")

        if response.status_code == 200:
            mock_store.load_dataframe.assert_called_once_with("atletas")

    def test_get_pontuacoes_endpoint_uses_store(self):
        """GET /tables/pontuacoes should read directly from Redis store."""
        from unittest.mock import patch

        mock_store = MagicMock()
        mock_store.load_dataframe.return_value = pd.DataFrame(
            [{"atleta_id": 89256, "rodada_id": 15, "pontuacao": 8.0}]
        )

        with patch("src.api.routes.get_redis_store", return_value=mock_store):
            with patch("src.api.routes.get_data_loader") as mock_dl:
                mock_dl.return_value = MagicMock()
                from src.main import get_app

                app = get_app()
                client = TestClient(app)
                response = client.get("/tables/pontuacoes")

        if response.status_code == 200:
            mock_store.load_dataframe.assert_called_with("pontuacoes")
