from unittest.mock import AsyncMock, MagicMock

import pandas as pd
import pytest
from src.services.data_loaders.atletas import Atletas, AtletasResult


class TestAtletas:
    @pytest.fixture
    def sample_api_response(self):
        return {
            "atletas": [
                {
                    "atleta_id": 1,
                    "rodada_id": 15,
                    "clube_id": 264,
                    "posicao_id": 1,
                    "status_id": 7,
                    "preco_num": 10.5,
                    "apelido": "Biro",
                },
                {
                    "atleta_id": 2,
                    "rodada_id": 15,
                    "clube_id": 276,
                    "posicao_id": 2,
                    "status_id": 7,
                    "preco_num": 15.2,
                    "apelido": "Casa",
                },
            ],
            "clubes": {
                "264": {"id": 264, "nome": "Flamengo"},
                "276": {"id": 276, "nome": "Fls"},
            },
            "posicoes": {
                "1": {"id": 1, "nome": "Goleiro"},
                "2": {"id": 2, "nome": "Lateral"},
            },
            "status": {
                "7": {"id": 7, "nome": "Provável"},
            },
        }

    @pytest.fixture
    def mock_request_handler(self, sample_api_response):
        handler = MagicMock()
        handler.make_get_request = AsyncMock(return_value=sample_api_response)
        return handler

    @pytest.fixture
    def atletas(self, mock_request_handler):
        return Atletas(mock_request_handler)

    def test_atletas_has_request_handler(self, atletas):
        assert atletas.request_handler is not None

    @pytest.mark.anyio
    async def test_fill_atletas_returns_atletas_result(
        self, atletas, sample_api_response
    ):
        atletas.request_handler.make_get_request = AsyncMock(
            return_value=sample_api_response
        )

        result = await atletas.fill_atletas()

        assert isinstance(result, AtletasResult)
        assert result.df is not None
        assert isinstance(result.df, pd.DataFrame)

    @pytest.mark.anyio
    async def test_fill_atletas_result_contains_rodada_id(
        self, atletas, sample_api_response
    ):
        atletas.request_handler.make_get_request = AsyncMock(
            return_value=sample_api_response
        )

        result = await atletas.fill_atletas()

        assert result.rodada_id == 15

    @pytest.mark.anyio
    async def test_fill_atletas_result_contains_clubes(
        self, atletas, sample_api_response
    ):
        atletas.request_handler.make_get_request = AsyncMock(
            return_value=sample_api_response
        )

        result = await atletas.fill_atletas()

        assert result.clubes is not None
        assert "264" in result.clubes

    @pytest.mark.anyio
    async def test_fill_atletas_result_contains_posicoes(
        self, atletas, sample_api_response
    ):
        atletas.request_handler.make_get_request = AsyncMock(
            return_value=sample_api_response
        )

        result = await atletas.fill_atletas()

        assert result.posicoes is not None
        assert "1" in result.posicoes

    @pytest.mark.anyio
    async def test_fill_atletas_result_contains_status(
        self, atletas, sample_api_response
    ):
        atletas.request_handler.make_get_request = AsyncMock(
            return_value=sample_api_response
        )

        result = await atletas.fill_atletas()

        assert result.status is not None
        assert "7" in result.status

    @pytest.mark.anyio
    async def test_fill_atletas_uses_correct_url(self, atletas, sample_api_response):
        atletas.request_handler.make_get_request = AsyncMock(
            return_value=sample_api_response
        )

        await atletas.fill_atletas()

        atletas.request_handler.make_get_request.assert_called_once_with(
            "https://api.cartola.globo.com/atletas/mercado"
        )

    @pytest.mark.anyio
    async def test_fill_atletas_returns_df_with_correct_columns(
        self, atletas, sample_api_response
    ):
        atletas.request_handler.make_get_request = AsyncMock(
            return_value=sample_api_response
        )

        result = await atletas.fill_atletas()

        assert list(result.df.columns) == [
            "atleta_id",
            "rodada_id",
            "clube_id",
            "posicao_id",
            "status_id",
            "preco_num",
            "apelido",
        ]
