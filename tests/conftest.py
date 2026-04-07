from unittest.mock import AsyncMock, MagicMock

import pytest
import taskiq_fastapi
from taskiq import InMemoryBroker

from backend.tkq import broker


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.fixture
def fastapi_app():
    from backend.main import get_app

    return get_app()


@pytest.fixture(autouse=True)
def init_taskiq_deps(fastapi_app):
    if isinstance(broker, InMemoryBroker):
        taskiq_fastapi.populate_dependency_context(broker, fastapi_app)
        broker.state.fastapi_app = fastapi_app

        from backend.dependencies import get_redis_store

        get_redis_store.cache_clear()

        mock_atletas = MagicMock()
        mock_atletas.rodada_id = 15
        mock_atletas.fill_atletas = AsyncMock(return_value=None)

        mock_confrontos = MagicMock()
        mock_confrontos.df = MagicMock()
        mock_confrontos.fill_confrontos = AsyncMock(return_value=None)

        mock_pontuacoes = MagicMock()
        mock_pontuacoes.df = MagicMock()
        mock_pontuacoes.fill_pontuacoes = AsyncMock(return_value=None)

        mock_pontos_cedidos = MagicMock()
        mock_pontos_cedidos.df = MagicMock()
        mock_pontos_cedidos.fill_pontos_cedidos = MagicMock()

        mock_data_loader = MagicMock()
        mock_data_loader.atletas = mock_atletas
        mock_data_loader.confrontos = mock_confrontos
        mock_data_loader.pontuacoes = mock_pontuacoes
        mock_data_loader.pontos_cedidos = mock_pontos_cedidos
        mock_data_loader.fill_data = AsyncMock(return_value=None)
        mock_data_loader._update_expensive_tables = AsyncMock(return_value=None)

        mock_rodada_id_state = {"current": 15, "previous": 15}
        mock_redis_store = MagicMock()

        fastapi_app.state.data_loader = mock_data_loader
        fastapi_app.state.rodada_id_state = mock_rodada_id_state
        broker.state.rodada_id_state = mock_rodada_id_state
        broker.state.redis_store = mock_redis_store
    yield
    broker.custom_dependency_context = {}
