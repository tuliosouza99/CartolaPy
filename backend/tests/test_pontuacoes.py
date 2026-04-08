from unittest.mock import AsyncMock, MagicMock

import pandas as pd
import pytest
from src.services.data_loaders.pontuacoes import Pontuacoes
from src.services.enums import Scout


class TestPontuacoes:
    @pytest.fixture
    def sample_api_response(self):
        return {
            "atletas": {
                "1": {
                    "posicao_id": 1,
                    "clube_id": 264,
                    "scout": {"FD": 1, "FF": 2},
                    "pontuacao": 3.6,
                    "entrou_em_campo": True,
                },
                "2": {
                    "posicao_id": 2,
                    "clube_id": 276,
                    "scout": {"FS": 3, "FC": 1},
                    "pontuacao": 1.9,
                    "entrou_em_campo": True,
                },
            }
        }

    @pytest.fixture
    def mock_request_handler(self, sample_api_response):
        handler = MagicMock()
        handler.make_get_request = AsyncMock(return_value=sample_api_response)
        return handler

    @pytest.fixture
    def pontuacoes(self, mock_request_handler):
        return Pontuacoes(mock_request_handler)

    def test_pontuacoes_has_request_handler(self, pontuacoes):
        assert pontuacoes.request_handler is not None

    @pytest.mark.anyio
    async def test_fill_pontuacoes_rodada_fetches_correct_url(
        self, pontuacoes, sample_api_response
    ):
        pontuacoes.request_handler.make_get_request = AsyncMock(
            return_value=sample_api_response
        )

        await pontuacoes._fill_pontuacoes_rodada(1)

        pontuacoes.request_handler.make_get_request.assert_called_once_with(
            "https://api.cartola.globo.com/atletas/pontuados/1"
        )

    @pytest.mark.anyio
    async def test_fill_pontuacoes_rodada_filters_entrou_em_campo(self, pontuacoes):
        response_with_bench = {
            "atletas": {
                "1": {
                    "posicao_id": 1,
                    "clube_id": 264,
                    "scout": {},
                    "pontuacao": 0.0,
                    "entrou_em_campo": True,
                },
                "2": {
                    "posicao_id": 2,
                    "clube_id": 276,
                    "scout": {},
                    "pontuacao": 0.0,
                    "entrou_em_campo": False,
                },
            }
        }
        pontuacoes.request_handler.make_get_request = AsyncMock(
            return_value=response_with_bench
        )

        result = await pontuacoes._fill_pontuacoes_rodada(1)

        assert len(result) == 1

    @pytest.mark.anyio
    async def test_fill_pontuacoes_rodada_calculates_pontuacao_basica(self, pontuacoes):
        response_fd = {
            "atletas": {
                "1": {
                    "posicao_id": 1,
                    "clube_id": 264,
                    "scout": {"FD": 1},
                    "pontuacao": 1.2,
                    "entrou_em_campo": True,
                },
            }
        }
        pontuacoes.request_handler.make_get_request = AsyncMock(
            return_value=response_fd
        )

        result = await pontuacoes._fill_pontuacoes_rodada(1)

        fd_value = Scout.FD.value["value"]
        assert result.iloc[0]["pontuacao_basica"] == fd_value

    @pytest.mark.anyio
    async def test_fill_pontuacoes_rodada_calculates_multiple_scouts(self, pontuacoes):
        response_multi = {
            "atletas": {
                "1": {
                    "posicao_id": 1,
                    "clube_id": 264,
                    "scout": {"FD": 1, "FF": 2},
                    "pontuacao": 3.6,
                    "entrou_em_campo": True,
                },
            }
        }
        pontuacoes.request_handler.make_get_request = AsyncMock(
            return_value=response_multi
        )

        result = await pontuacoes._fill_pontuacoes_rodada(1)

        expected = Scout.FD.value["value"] * 1 + Scout.FF.value["value"] * 2
        assert result.iloc[0]["pontuacao_basica"] == expected

    @pytest.mark.anyio
    async def test_fill_pontuacoes_rodada_sets_rodada_id(
        self, pontuacoes, sample_api_response
    ):
        pontuacoes.request_handler.make_get_request = AsyncMock(
            return_value=sample_api_response
        )

        result = await pontuacoes._fill_pontuacoes_rodada(5)

        assert (result["rodada_id"] == 5).all()

    @pytest.mark.anyio
    async def test_fill_pontuacoes_fetches_multiple_rodadas(
        self, pontuacoes, sample_api_response
    ):
        pontuacoes.request_handler.make_get_request = AsyncMock(
            return_value=sample_api_response
        )

        await pontuacoes.fill_pontuacoes(3)

        assert pontuacoes.request_handler.make_get_request.call_count == 3

    @pytest.mark.anyio
    async def test_fill_pontuacoes_returns_dataframe(
        self, pontuacoes, sample_api_response
    ):
        pontuacoes.request_handler.make_get_request = AsyncMock(
            return_value=sample_api_response
        )

        result = await pontuacoes.fill_pontuacoes(3)

        expected_rows = len(sample_api_response["atletas"]) * 3
        assert len(result) == expected_rows
        assert isinstance(result, pd.DataFrame)

    @pytest.mark.anyio
    async def test_fill_pontuacoes_includes_all_scout_columns(
        self, pontuacoes, sample_api_response
    ):
        pontuacoes.request_handler.make_get_request = AsyncMock(
            return_value=sample_api_response
        )

        result = await pontuacoes.fill_pontuacoes(1)

        for scout_name in Scout.as_list():
            assert scout_name in result.columns

    @pytest.mark.anyio
    async def test_fill_pontuacoes_fills_missing_scouts_with_zero(self, pontuacoes):
        response_no_scouts = {
            "atletas": {
                "1": {
                    "posicao_id": 1,
                    "clube_id": 264,
                    "scout": {},
                    "pontuacao": 0.0,
                    "entrou_em_campo": True,
                },
            }
        }
        pontuacoes.request_handler.make_get_request = AsyncMock(
            return_value=response_no_scouts
        )

        result = await pontuacoes._fill_pontuacoes_rodada(1)

        for scout_name in Scout.as_basic_scouts_list():
            assert result.iloc[0][scout_name] == 0
