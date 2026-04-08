from unittest.mock import AsyncMock, MagicMock

import pandas as pd
import pytest
import taskiq_fastapi
from src.tkq import broker
from taskiq import InMemoryBroker


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.fixture
def fastapi_app():
    from src.main import get_app

    return get_app()


@pytest.fixture
def mock_redis_data():
    return {
        "atletas": pd.DataFrame(
            [
                {
                    "atleta_id": 1,
                    "rodada_id": 15,
                    "clube_id": 264,
                    "posicao_id": 1,
                    "status_id": 7,
                    "preco_num": 10.5,
                    "apelido": "Biro",
                }
            ]
        ),
        "pontuacoes": pd.DataFrame(
            [
                {
                    "atleta_id": 1,
                    "posicao_id": 1,
                    "clube_id": 264,
                    "rodada_id": 15,
                    "pontuacao": 8.0,
                    "pontuacao_basica": 8.0,
                }
            ]
        ),
        "confrontos": pd.DataFrame(
            [
                {
                    "clube_id": 264,
                    "opponent_clube_id": 276,
                    "is_mandante": True,
                    "rodada_id": 15,
                    "partida_id": 1001,
                }
            ]
        ),
        "pontos_cedidos": pd.DataFrame(
            [
                {
                    "clube_id": 264,
                    "posicao_id": 1,
                    "is_mandante": True,
                    "rodada_id": 15,
                    "partida_id": 1001,
                    "pontuacao": 5.0,
                    "pontuacao_basica": 5.0,
                }
            ]
        ),
        "json": {
            "1": {"id": 1, "nome": "Goleiro", "abreviacao": "GOL"},
            "7": {"id": 7, "nome": "Provável", "cor": "green"},
            "264": {"id": 264, "nome": "Flamengo", "escudos": {"60x60": "url"}},
        },
    }


@pytest.fixture(autouse=True)
def init_taskiq_deps(fastapi_app, mock_redis_data):
    if isinstance(broker, InMemoryBroker):
        taskiq_fastapi.populate_dependency_context(broker, fastapi_app)
        broker.state.fastapi_app = fastapi_app

        from src.dependencies import get_redis_store

        get_redis_store.cache_clear()

        mock_atletas = MagicMock()
        mock_atletas.rodada_id = 15
        mock_atletas.fill_atletas = AsyncMock(
            return_value=MagicMock(
                df=mock_redis_data["atletas"],
                rodada_id=15,
                clubes={},
                posicoes={},
                status={},
            )
        )

        mock_confrontos = MagicMock()
        mock_confrontos.df = pd.DataFrame()
        mock_confrontos.fill_confrontos = AsyncMock(return_value=pd.DataFrame())

        mock_pontuacoes = MagicMock()
        mock_pontuacoes.df = pd.DataFrame()
        mock_pontuacoes.fill_pontuacoes = AsyncMock(return_value=pd.DataFrame())

        mock_pontos_cedidos = MagicMock()
        mock_pontos_cedidos.df = pd.DataFrame()
        mock_pontos_cedidos.fill_pontos_cedidos = MagicMock(return_value=pd.DataFrame())

        mock_data_loader = MagicMock()
        mock_data_loader.atletas = mock_atletas
        mock_data_loader.confrontos = mock_confrontos
        mock_data_loader.pontuacoes = mock_pontuacoes
        mock_data_loader.pontos_cedidos = mock_pontos_cedidos
        mock_data_loader.fill_data = AsyncMock(return_value=None)
        mock_data_loader._update_expensive_tables = AsyncMock(return_value=None)

        mock_rodada_id_state = {"current": 15, "previous": 15}

        mock_redis_store = MagicMock()
        mock_redis_store.load_dataframe = MagicMock(
            side_effect=lambda key: mock_redis_data.get(key, pd.DataFrame())
        )
        mock_redis_store.load_json = MagicMock(
            side_effect=lambda key: mock_redis_data.get("json", {}).get(key, {})
        )
        mock_redis_store.load_rodada_id = MagicMock(return_value=15)
        mock_redis_store.load_last_updated = MagicMock(return_value=None)
        mock_redis_store.save_dataframe = MagicMock()
        mock_redis_store.save_json = MagicMock()
        mock_redis_store.save_rodada_id = MagicMock()
        mock_redis_store.save_last_updated = MagicMock()

        fastapi_app.state.data_loader = mock_data_loader
        fastapi_app.state.redis_store = mock_redis_store
        fastapi_app.state.rodada_id_state = mock_rodada_id_state
        broker.state.rodada_id_state = mock_rodada_id_state
        broker.state.redis_store = mock_redis_store
    yield
    broker.custom_dependency_context = {}
