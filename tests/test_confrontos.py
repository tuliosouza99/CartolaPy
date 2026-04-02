from unittest.mock import AsyncMock, MagicMock

import pandas as pd
import pytest

from backend.services.data_loaders.confrontos import Confrontos


class TestConfrontos:
    @pytest.fixture
    def sample_api_response(self):
        return {
            "partidas": [
                {
                    "partida_id": 1001,
                    "clube_casa_id": 264,
                    "clube_visitante_id": 276,
                    "valida": True,
                },
                {
                    "partida_id": 1002,
                    "clube_casa_id": 287,
                    "clube_visitante_id": 303,
                    "valida": True,
                },
            ]
        }

    @pytest.fixture
    def mock_request_handler(self, sample_api_response):
        handler = MagicMock()
        handler.make_get_request = AsyncMock(return_value=sample_api_response)
        return handler

    @pytest.fixture
    def confrontos(self, mock_request_handler):
        return Confrontos(mock_request_handler)

    def test_initial_df_is_empty_with_correct_columns(self, confrontos):
        assert isinstance(confrontos.df, pd.DataFrame)
        assert confrontos.df.empty
        assert list(confrontos.df.columns) == [
            "clube_id",
            "opponent_clube_id",
            "is_mandante",
            "rodada_id",
        ]

    @pytest.mark.anyio
    async def test_fill_confrontos_rodada_fetches_correct_url(
        self, confrontos, sample_api_response
    ):
        confrontos.request_handler.make_get_request = AsyncMock(
            return_value=sample_api_response
        )

        await confrontos._fill_confrontos_rodada(1)

        confrontos.request_handler.make_get_request.assert_called_once_with(
            "https://api.cartola.globo.com/partidas/1"
        )

    @pytest.mark.anyio
    async def test_fill_confrontos_rodada_expands_home_away(
        self, confrontos, sample_api_response
    ):
        confrontos.request_handler.make_get_request = AsyncMock(
            return_value=sample_api_response
        )

        result = await confrontos._fill_confrontos_rodada(1)

        assert len(result) == 4

    @pytest.mark.anyio
    async def test_fill_confrontos_rodada_sets_is_mandante_correctly(
        self, confrontos, sample_api_response
    ):
        confrontos.request_handler.make_get_request = AsyncMock(
            return_value=sample_api_response
        )

        result = await confrontos._fill_confrontos_rodada(1)

        mandante_rows = result[result["is_mandante"]]
        visitante_rows = result[~result["is_mandante"]]

        assert len(mandante_rows) == 2
        assert len(visitante_rows) == 2

    @pytest.mark.anyio
    async def test_fill_confrontos_rodada_sets_opponent_correctly(
        self, confrontos, sample_api_response
    ):
        confrontos.request_handler.make_get_request = AsyncMock(
            return_value=sample_api_response
        )

        result = await confrontos._fill_confrontos_rodada(1)

        casa_row = result[(result["clube_id"] == 264) & result["is_mandante"]]
        assert casa_row.iloc[0]["opponent_clube_id"] == 276

        visitante_row = result[(result["clube_id"] == 276) & ~result["is_mandante"]]
        assert visitante_row.iloc[0]["opponent_clube_id"] == 264

    @pytest.mark.anyio
    async def test_fill_confrontos_rodada_filters_invalid_matches(self, confrontos):
        response_with_invalid = {
            "partidas": [
                {
                    "partida_id": 1001,
                    "clube_casa_id": 264,
                    "clube_visitante_id": 276,
                    "valida": True,
                },
                {
                    "partida_id": 1002,
                    "clube_casa_id": 287,
                    "clube_visitante_id": 303,
                    "valida": False,
                },
            ]
        }
        confrontos.request_handler.make_get_request = AsyncMock(
            return_value=response_with_invalid
        )

        result = await confrontos._fill_confrontos_rodada(1)

        assert len(result) == 2

    @pytest.mark.anyio
    async def test_fill_confrontos_rodada_sets_rodada_id(
        self, confrontos, sample_api_response
    ):
        confrontos.request_handler.make_get_request = AsyncMock(
            return_value=sample_api_response
        )

        result = await confrontos._fill_confrontos_rodada(7)

        assert (result["rodada_id"] == 7).all()

    @pytest.mark.anyio
    async def test_fill_confrontos_fetches_multiple_rodadas(
        self, confrontos, sample_api_response
    ):
        confrontos.request_handler.make_get_request = AsyncMock(
            return_value=sample_api_response
        )

        await confrontos.fill_confrontos(3)

        assert confrontos.request_handler.make_get_request.call_count == 3

    @pytest.mark.anyio
    async def test_fill_confrontos_concatenates_all_rodadas(
        self, confrontos, sample_api_response
    ):
        confrontos.request_handler.make_get_request = AsyncMock(
            return_value=sample_api_response
        )

        await confrontos.fill_confrontos(2)

        expected_rows = 4 * 2
        assert len(confrontos.df) == expected_rows

    def test_df_property_returns_dataframe(self, confrontos):
        assert isinstance(confrontos.df, pd.DataFrame)
