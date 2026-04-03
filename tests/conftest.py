from unittest.mock import AsyncMock, MagicMock

import pytest
import taskiq_fastapi
from taskiq import InMemoryBroker

from backend.tkq import broker

_mock_store = MagicMock()
_mock_store.load_rodada_id = MagicMock(return_value=15)


@pytest.fixture(scope="session", autouse=True)
def patch_redis_store():
    from backend import tasks
    from backend.services.redis_store import RedisDataFrameStore

    original_new = RedisDataFrameStore.__new__

    def mock_new(cls, *args, **kwargs):
        if cls is RedisDataFrameStore:
            return _mock_store
        return original_new(cls, *args, **kwargs)

    RedisDataFrameStore.__new__ = staticmethod(mock_new)
    tasks.get_redis_store = lambda: _mock_store

    yield

    RedisDataFrameStore.__new__ = original_new


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.fixture
def mock_store():
    return _mock_store


@pytest.fixture
def fastapi_app(mock_store):
    from backend.main import get_app

    app = get_app()
    if isinstance(broker, InMemoryBroker):
        taskiq_fastapi.populate_dependency_context(broker, app)
        broker.state.fastapi_app = app

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

        app.state.data_loader = mock_data_loader
        app.state.rodada_id_state = mock_rodada_id_state
        broker.state.rodada_id_state = mock_rodada_id_state
    yield app
    if isinstance(broker, InMemoryBroker):
        pass
    broker.custom_dependency_context = {}
