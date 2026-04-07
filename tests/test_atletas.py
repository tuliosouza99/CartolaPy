from unittest.mock import AsyncMock, MagicMock

import pandas as pd
import pytest

from backend.services.data_loaders.atletas import Atletas


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
            ]
        }

    @pytest.fixture
    def mock_request_handler(self, sample_api_response):
        handler = MagicMock()
        handler.make_get_request = AsyncMock(return_value=sample_api_response)
        return handler

    @pytest.fixture
    def atletas(self, mock_request_handler):
        return Atletas(mock_request_handler)

    def test_initial_df_is_empty_with_correct_columns(self, atletas):
        assert isinstance(atletas.df, pd.DataFrame)
        assert atletas.df.empty
        assert list(atletas.df.columns) == [
            "atleta_id",
            "rodada_id",
            "clube_id",
            "posicao_id",
            "status_id",
            "preco_num",
            "apelido",
        ]

    def test_initial_rodada_id_is_none(self, atletas):
        assert atletas.rodada_id is None

    @pytest.mark.anyio
    async def test_fill_atletas_sets_rodada_id(self, atletas, sample_api_response):
        atletas.request_handler.make_get_request = AsyncMock(
            return_value=sample_api_response
        )

        await atletas.fill_atletas()

        assert atletas.rodada_id == 15

    @pytest.mark.anyio
    async def test_fill_atletas_populates_df(self, atletas, sample_api_response):
        atletas.request_handler.make_get_request = AsyncMock(
            return_value=sample_api_response
        )

        await atletas.fill_atletas()

        assert len(atletas.df) == 2
        assert atletas.df["atleta_id"].tolist() == [1, 2]
        assert atletas.df["apelido"].tolist() == ["Biro", "Casa"]

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
    async def test_fill_atletas_selects_only_required_columns(self, atletas):
        extended_response = {
            "atletas": [
                {
                    "atleta_id": 1,
                    "rodada_id": 15,
                    "clube_id": 264,
                    "posicao_id": 1,
                    "status_id": 7,
                    "preco_num": 10.5,
                    "apelido": "Biro",
                    "extra_column": "should_not_appear",
                },
            ]
        }
        atletas.request_handler.make_get_request = AsyncMock(
            return_value=extended_response
        )

        await atletas.fill_atletas()

        assert list(atletas.df.columns) == [
            "atleta_id",
            "rodada_id",
            "clube_id",
            "posicao_id",
            "status_id",
            "preco_num",
            "apelido",
        ]
        assert "extra_column" not in atletas.df.columns

    def test_df_property_returns_dataframe(self, atletas):
        assert isinstance(atletas.df, pd.DataFrame)

    def test_rodada_id_property_returns_int_or_none(self, atletas):
        result = atletas.rodada_id
        assert result is None or isinstance(result, int)
