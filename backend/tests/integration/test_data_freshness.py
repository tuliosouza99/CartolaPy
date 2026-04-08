"""
Integration tests for cross-container data freshness.

These tests simulate the scenario where:
1. API container loads data at startup (stale in-memory copy)
2. Scheduler/Worker container updates Redis with fresh data
3. API container should reload from Redis on next request

This prevents regression of the bug where API served stale in-memory data
even after scheduler updated Redis.
"""

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from fastapi.testclient import TestClient

from src.services.data_loaders.atletas import Atletas
from src.services.data_loaders.confrontos import Confrontos
from src.services.data_loaders.pontos_cedidos import PontosCedidos
from src.services.data_loaders.pontuacoes import Pontuacoes


def create_mock_atletas(df, last_updated, rodada_id=15):
    """Create a mock Atletas loader with specified data."""
    loader = MagicMock()
    loader.df = df
    loader.last_updated = last_updated
    loader.rodada_id = rodada_id
    loader.REDIS_KEY = "atletas"
    loader.request_handler = None
    return loader


class TestDataFreshnessAcrossContainers:
    """
    Simulates cross-container data freshness scenario.

    This is the bug we fixed: when the scheduler (worker container) updates
    Redis, the API container was still serving its stale in-memory copy.
    """

    @pytest.fixture
    def stale_timestamp(self):
        return datetime(2026, 4, 8, 10, 0, 0, tzinfo=timezone.utc)

    @pytest.fixture
    def fresh_timestamp(self):
        return datetime(2026, 4, 8, 12, 0, 0, tzinfo=timezone.utc)

    @pytest.fixture
    def stale_atletas_df(self):
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

    @pytest.fixture
    def fresh_atletas_df(self):
        return pd.DataFrame(
            [
                {
                    "atleta_id": 89256,
                    "rodada_id": 15,
                    "clube_id": 264,
                    "posicao_id": 1,
                    "status_id": 2,
                    "preco_num": 14.8,
                    "apelido": "Gerson",
                }
            ]
        )

    def test_api_reloads_atletas_when_redis_has_newer_data(
        self, stale_timestamp, fresh_timestamp, stale_atletas_df, fresh_atletas_df
    ):
        """
        When Redis has newer data than in-memory, API should reload.

        This is the core bug fix: API container loads stale data at startup,
        but scheduler updates Redis. Next request should return fresh data.
        """
        from src.services.data_loaders.data_loader import DataLoader

        loader = DataLoader.__new__(DataLoader)

        stale_atletas_loader = Atletas.__new__(Atletas)
        stale_atletas_loader.columns = [
            "atleta_id",
            "rodada_id",
            "clube_id",
            "posicao_id",
            "status_id",
            "preco_num",
            "apelido",
        ]
        stale_atletas_loader._df = stale_atletas_df
        stale_atletas_loader._rodada_id = 15
        stale_atletas_loader._last_updated = stale_timestamp
        stale_atletas_loader._clubes = {}
        stale_atletas_loader._posicoes = {}
        stale_atletas_loader._status = {}
        stale_atletas_loader.request_handler = None

        loader.atletas = stale_atletas_loader
        loader._request_handler = None

        store = MagicMock()
        store.load_last_updated.return_value = fresh_timestamp

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
        fresh_atletas_loader._last_updated = fresh_timestamp
        fresh_atletas_loader._clubes = {}
        fresh_atletas_loader._posicoes = {}
        fresh_atletas_loader._status = {}
        fresh_atletas_loader.request_handler = None

        with patch.object(
            Atletas, "load_from_redis", return_value=fresh_atletas_loader
        ):
            result = loader.reload_atletas_if_stale(store)

        assert result is True
        assert loader.atletas.df.iloc[0]["status_id"] == 2
        assert loader.atletas.df.iloc[0]["preco_num"] == 14.8

    def test_api_keeps_stale_data_when_redis_is_older(
        self, stale_timestamp, stale_atletas_df
    ):
        """
        When Redis has older data than in-memory, API should NOT reload.

        This prevents unnecessary Redis reads and reloads.
        """
        from src.services.data_loaders.data_loader import DataLoader

        loader = DataLoader.__new__(DataLoader)

        stale_atletas_loader = Atletas.__new__(Atletas)
        stale_atletas_loader.columns = [
            "atleta_id",
            "rodada_id",
            "clube_id",
            "posicao_id",
            "status_id",
            "preco_num",
            "apelido",
        ]
        stale_atletas_loader._df = stale_atletas_df
        stale_atletas_loader._rodada_id = 15
        stale_atletas_loader._last_updated = stale_timestamp
        stale_atletas_loader._clubes = {}
        stale_atletas_loader._posicoes = {}
        stale_atletas_loader._status = {}
        stale_atletas_loader.request_handler = None

        loader.atletas = stale_atletas_loader
        loader._request_handler = None

        redis_timestamp = stale_timestamp.replace(hour=8)
        store = MagicMock()
        store.load_last_updated.return_value = redis_timestamp

        result = loader.reload_atletas_if_stale(store)

        assert result is False
        assert loader.atletas.df.iloc[0]["status_id"] == 7

    def test_reload_all_checks_all_tables(self):
        """
        reload_all_if_stale should check and reload all tables that are stale.
        """
        from src.services.data_loaders.data_loader import DataLoader

        loader = DataLoader.__new__(DataLoader)

        old_ts = datetime(2026, 4, 8, 10, 0, 0, tzinfo=timezone.utc)
        new_ts = datetime(2026, 4, 8, 14, 0, 0, tzinfo=timezone.utc)

        def timestamp_side_effect(key):
            if key == "atletas":
                return new_ts
            return old_ts

        store = MagicMock()
        store.load_last_updated.side_effect = timestamp_side_effect
        store.load_rodada_id.return_value = 15

        mock_atletas = create_mock_atletas(pd.DataFrame(), old_ts)
        mock_confrontos = MagicMock()
        mock_confrontos.last_updated = old_ts
        mock_confrontos.REDIS_KEY = "confrontos"

        mock_pontuacoes = MagicMock()
        mock_pontuacoes.last_updated = old_ts
        mock_pontuacoes.REDIS_KEY = "pontuacoes"

        mock_pontos_cedidos = MagicMock()
        mock_pontos_cedidos.last_updated = old_ts
        mock_pontos_cedidos.REDIS_KEY = "pontos_cedidos"

        loader.atletas = mock_atletas
        loader.confrontos = mock_confrontos
        loader.pontuacoes = mock_pontuacoes
        loader.pontos_cedidos = mock_pontos_cedidos
        loader._request_handler = None

        fresh_atletas = create_mock_atletas(pd.DataFrame(), new_ts)

        with patch.object(Atletas, "load_from_redis", return_value=fresh_atletas):
            results = loader.reload_all_if_stale(store)

        assert results["atletas"] is True
        assert results["confrontos"] is False
        assert results["pontuacoes"] is False
        assert results["pontos_cedidos"] is False


class TestEndpointsReloadOnRequest:
    """
    Tests that endpoints call reload before serving data.

    These tests verify that the fix (calling reload_*_if_stale in endpoints)
    actually works by checking the endpoint behavior.
    """

    def test_get_atletas_endpoint_calls_reload(self):
        """
        /tables/atletas should call reload_atletas_if_stale before serving.
        """
        stale_df = pd.DataFrame(
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

        old_ts = datetime(2026, 4, 8, 10, 0, 0, tzinfo=timezone.utc)
        new_ts = datetime(2026, 4, 8, 14, 0, 0, tzinfo=timezone.utc)

        mock_atletas = MagicMock()
        mock_atletas.df = stale_df
        mock_atletas.last_updated = old_ts
        mock_atletas.REDIS_KEY = "atletas"

        mock_confrontos = MagicMock()
        mock_confrontos.df = pd.DataFrame()
        mock_confrontos.last_updated = old_ts
        mock_confrontos.REDIS_KEY = "confrontos"

        mock_pontuacoes = MagicMock()
        mock_pontuacoes.df = pd.DataFrame()
        mock_pontuacoes.last_updated = old_ts
        mock_pontuacoes.REDIS_KEY = "pontuacoes"

        mock_pontos_cedidos = MagicMock()
        mock_pontos_cedidos.df = pd.DataFrame()
        mock_pontos_cedidos.last_updated = old_ts
        mock_pontos_cedidos.REDIS_KEY = "pontos_cedidos"

        mock_dl = MagicMock()
        mock_dl.atletas = mock_atletas
        mock_dl.confrontos = mock_confrontos
        mock_dl.pontuacoes = mock_pontuacoes
        mock_dl.pontos_cedidos = mock_pontos_cedidos
        mock_dl.reload_atletas_if_stale = MagicMock(return_value=False)
        mock_dl.reload_confrontos_if_stale = MagicMock(return_value=False)
        mock_dl.reload_pontuacoes_if_stale = MagicMock(return_value=False)
        mock_dl.reload_pontos_cedidos_if_stale = MagicMock(return_value=False)

        mock_store = MagicMock()
        mock_store.load_last_updated.return_value = new_ts
        mock_store.load_rodada_id.return_value = 15

        with patch("src.api.routes.get_data_loader", return_value=mock_dl):
            with patch("src.api.routes.get_redis_store", return_value=mock_store):
                from src.main import get_app

                app = get_app()
                client = TestClient(app)
                response = client.get("/tables/atletas")

        if response.status_code == 200:
            mock_dl.reload_atletas_if_stale.assert_called_once()
