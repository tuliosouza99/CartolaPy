from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pandas as pd
import pytest
from src.services.data_loaders.data_loader import DataLoader
from src.services.data_loaders.atletas import Atletas


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
        ]
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
        loader = DataLoader()
        loader.request_handler = mock_request_handler
        return loader


@pytest.fixture
def mock_store():
    return MagicMock()


@pytest.fixture
def fresh_atletas_df():
    return pd.DataFrame(
        [
            {
                "atleta_id": 89256,
                "rodada_id": 15,
                "clube_id": 264,
                "posicao_id": 1,
                "status_id": 2,
                "preco_num": 15.0,
                "apelido": "Gerson",
            }
        ]
    )


@pytest.fixture
def stale_atletas_df():
    return pd.DataFrame(
        [
            {
                "atleta_id": 89256,
                "rodada_id": 15,
                "clube_id": 264,
                "posicao_id": 1,
                "status_id": 7,
                "preco_num": 15.5,
                "apelido": "Gerson",
            }
        ]
    )


class TestDataLoader:
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
            "src.services.data_loaders.data_loader.RequestHandler",
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


class TestReloadAtletasIfStale:
    def test_reload_happens_when_redis_is_newer(
        self, data_loader, mock_store, stale_atletas_df, fresh_atletas_df
    ):
        older_timestamp = datetime(2026, 4, 8, 10, 0, 0, tzinfo=timezone.utc)
        newer_timestamp = datetime(2026, 4, 8, 12, 0, 0, tzinfo=timezone.utc)

        data_loader.atletas._df = stale_atletas_df
        data_loader.atletas._last_updated = older_timestamp

        mock_store.load_last_updated.return_value = newer_timestamp

        fresh_atletas_loader = Atletas.__new__(Atletas)
        fresh_atletas_loader.columns = [
            "atleta_id",
            "rodada_id",
            "clube_id",
            "posicao_id",
            "status_id",
            "preco_num",
            "apelido",
        ]
        fresh_atletas_loader._df = fresh_atletas_df
        fresh_atletas_loader._rodada_id = 15
        fresh_atletas_loader._last_updated = newer_timestamp
        fresh_atletas_loader._clubes = {}
        fresh_atletas_loader._posicoes = {}
        fresh_atletas_loader._status = {}
        fresh_atletas_loader.request_handler = data_loader.request_handler

        with patch.object(
            Atletas, "load_from_redis", return_value=fresh_atletas_loader
        ):
            result = data_loader.reload_atletas_if_stale(mock_store)

        assert result is True
        assert data_loader.atletas._df.iloc[0]["status_id"] == 2
        assert data_loader.atletas.request_handler is data_loader.request_handler

    def test_no_reload_when_redis_is_older(
        self, data_loader, mock_store, stale_atletas_df
    ):
        older_timestamp = datetime(2026, 4, 8, 10, 0, 0, tzinfo=timezone.utc)
        newer_in_memory_timestamp = datetime(2026, 4, 8, 12, 0, 0, tzinfo=timezone.utc)

        data_loader.atletas._df = stale_atletas_df
        data_loader.atletas._last_updated = newer_in_memory_timestamp

        mock_store.load_last_updated.return_value = older_timestamp

        result = data_loader.reload_atletas_if_stale(mock_store)

        assert result is False

    def test_no_reload_when_redis_has_no_timestamp(self, data_loader, mock_store):
        data_loader.atletas._last_updated = datetime.now(timezone.utc)
        mock_store.load_last_updated.return_value = None

        result = data_loader.reload_atletas_if_stale(mock_store)

        assert result is False

    def test_no_reload_when_memory_has_no_timestamp(
        self, data_loader, mock_store, stale_atletas_df
    ):
        data_loader.atletas._df = stale_atletas_df
        data_loader.atletas._last_updated = None

        mock_store.load_last_updated.return_value = datetime.now(timezone.utc)

        result = data_loader.reload_atletas_if_stale(mock_store)

        assert result is False

    def test_no_reload_when_redis_returns_none(self, data_loader, mock_store):
        data_loader.atletas._last_updated = datetime.now(timezone.utc)
        mock_store.load_last_updated.return_value = datetime.now(timezone.utc)

        with patch.object(Atletas, "load_from_redis", return_value=None):
            result = data_loader.reload_atletas_if_stale(mock_store)

        assert result is False
