from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from backend.services.data_loaders.data_loader import DataLoader


class TestDataLoader:
    @pytest.fixture
    def sample_atletas_response(self):
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
            ]
        }

    @pytest.fixture
    def sample_pontuacoes_response(self):
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
    def sample_confrontos_response(self):
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
        self,
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
    def data_loader(self, mock_request_handler):
        with patch(
            "backend.services.data_loaders.data_loader.RequestHandler",
            return_value=mock_request_handler,
        ):
            loader = DataLoader()
            loader.request_handler = mock_request_handler
            return loader

    def test_data_loader_has_all_sub_loaders(self, data_loader):
        assert hasattr(data_loader, "atletas")
        assert hasattr(data_loader, "confrontos")
        assert hasattr(data_loader, "pontuacoes")
        assert hasattr(data_loader, "pontos_cedidos")

    @pytest.mark.anyio
    async def test_fill_data_fills_atletas_first(
        self, data_loader, mock_request_handler
    ):
        await data_loader.fill_data()

        first_call_args = mock_request_handler.make_get_request.call_args_list[0]
        assert "atletas/mercado" in first_call_args[0][0]

    @pytest.mark.anyio
    async def test_fill_data_fills_confrontos_and_pontuacoes(
        self, data_loader, mock_request_handler
    ):
        await data_loader.fill_data()

        assert mock_request_handler.make_get_request.call_count >= 3

    @pytest.mark.anyio
    async def test_fill_data_raises_error_when_rodada_id_missing(
        self, mock_request_handler
    ):
        mock_request_handler.make_get_request = AsyncMock(
            return_value={
                "atletas": [
                    {
                        "atleta_id": 1,
                        "rodada_id": None,
                        "clube_id": 264,
                        "posicao_id": 1,
                        "status_id": 7,
                        "preco_num": 10.5,
                        "apelido": "Test",
                    }
                ]
            }
        )

        with patch(
            "backend.services.data_loaders.data_loader.RequestHandler",
            return_value=mock_request_handler,
        ):
            loader = DataLoader()
            loader.request_handler = mock_request_handler

        with pytest.raises(ValueError, match="Rodada ID not found"):
            await loader.fill_data()

    @pytest.mark.anyio
    async def test_fill_data_populates_atletas_df(self, data_loader):
        await data_loader.fill_data()

        assert not data_loader.atletas.df.empty

    @pytest.mark.anyio
    async def test_fill_data_populates_pontuacoes_df(self, data_loader):
        await data_loader.fill_data()

        assert not data_loader.pontuacoes.df.empty

    @pytest.mark.anyio
    async def test_fill_data_populates_confrontos_df(self, data_loader):
        await data_loader.fill_data()

        assert not data_loader.confrontos.df.empty

    @pytest.mark.anyio
    async def test_fill_data_populates_pontos_cedidos_df(self, data_loader):
        await data_loader.fill_data()

        assert not data_loader.pontos_cedidos.df.empty

    @pytest.mark.anyio
    async def test_fill_data_sets_rodada_id_from_atletas(self, data_loader):
        await data_loader.fill_data()

        assert data_loader.atletas.rodada_id == 15
