from unittest.mock import AsyncMock, MagicMock, patch

import pandas as pd
import pytest


@pytest.fixture
def sample_atletas_response():
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
        ],
        "clubes": {"264": {"id": 264, "nome": "Flamengo", "escudos": {"60x60": "url"}}},
        "posicoes": {"1": {"id": 1, "nome": "Goleiro", "abreviacao": "GOL"}},
        "status": {"7": {"id": 7, "nome": "Provável"}},
    }


@pytest.fixture
def sample_pontuacoes_response():
    return {
        "atletas": {
            "1": {
                "posicao_id": 1,
                "clube_id": 264,
                "scout": {"G": 1},
                "pontuacao": 8.0,
                "entrou_em_campo": True,
            },
        }
    }


@pytest.fixture
def sample_confrontos_response():
    return {
        "partidas": [
            {
                "partida_id": 1001,
                "clube_casa_id": 264,
                "clube_visitante_id": 276,
                "valida": True,
            },
        ]
    }


@pytest.fixture
def mock_request_handler(
    sample_atletas_response,
    sample_pontuacoes_response,
    sample_confrontos_response,
):
    def get_response(url: str):
        if "atletas/mercado" in url:
            return sample_atletas_response
        elif "atletas/pontuados" in url:
            return sample_pontuacoes_response
        elif "partidas" in url:
            return sample_confrontos_response
        return {}

    handler = MagicMock()
    handler.make_get_request = AsyncMock(side_effect=get_response)
    return handler


@pytest.fixture
def data_loader(mock_request_handler):
    with patch(
        "src.services.data_loaders.data_loader.RequestHandler",
        return_value=mock_request_handler,
    ):
        loader = MagicMock()
        from src.services.data_loaders.atletas import Atletas
        from src.services.data_loaders.confrontos import Confrontos
        from src.services.data_loaders.pontos_cedidos import PontosCedidos
        from src.services.data_loaders.pontuacoes import Pontuacoes

        loader.atletas = Atletas(mock_request_handler)
        loader.confrontos = Confrontos(mock_request_handler)
        loader.pontuacoes = Pontuacoes(mock_request_handler)
        loader.pontos_cedidos = PontosCedidos()
        return loader


class TestDataLoader:
    def test_data_loader_has_all_sub_loaders(self, data_loader):
        assert hasattr(data_loader, "atletas")
        assert hasattr(data_loader, "confrontos")
        assert hasattr(data_loader, "pontuacoes")
        assert hasattr(data_loader, "pontos_cedidos")

    @pytest.mark.anyio
    async def test_fill_atletas_returns_result(self, data_loader, mock_request_handler):
        result = await data_loader.atletas.fill_atletas()

        first_call_args = mock_request_handler.make_get_request.call_args_list[0]
        assert "atletas/mercado" in first_call_args[0][0]
        assert result.df is not None
        assert result.rodada_id == 15
        assert result.clubes is not None
        assert result.posicoes is not None
        assert result.status is not None

    @pytest.mark.anyio
    async def test_fill_confrontos_returns_dataframe(
        self, data_loader, mock_request_handler
    ):
        result = await data_loader.confrontos.fill_confrontos(15)

        assert mock_request_handler.make_get_request.call_count >= 1
        assert result is not None
        assert isinstance(result, pd.DataFrame)

    @pytest.mark.anyio
    async def test_fill_pontuacoes_returns_dataframe(
        self, data_loader, mock_request_handler
    ):
        result = await data_loader.pontuacoes.fill_pontuacoes(15)

        assert mock_request_handler.make_get_request.call_count >= 1
        assert result is not None
        assert isinstance(result, pd.DataFrame)

    @pytest.mark.anyio
    async def test_fill_pontos_cedidos_returns_dataframe(self, data_loader):
        from src.services.enums import Scout

        pontuacoes_df = pd.DataFrame(
            {
                "atleta_id": [1],
                "posicao_id": [1],
                "clube_id": [264],
                "rodada_id": [15],
                "pontuacao": [5.0],
                "pontuacao_basica": [5.0],
                **{scout: [0] for scout in Scout.as_list()},
            }
        )
        confrontos_df = pd.DataFrame(
            [
                {
                    "clube_id": 264,
                    "opponent_clube_id": 276,
                    "is_mandante": True,
                    "rodada_id": 15,
                    "partida_id": 1001,
                }
            ]
        )

        result = data_loader.pontos_cedidos.fill_pontos_cedidos(
            pontuacoes_df, confrontos_df
        )

        assert result is not None
        assert isinstance(result, pd.DataFrame)
