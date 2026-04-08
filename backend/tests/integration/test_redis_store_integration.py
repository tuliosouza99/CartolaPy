"""
Integration tests for Redis store operations.

These tests require a running Redis instance.

Usage:
    # For local Redis (no password):
    docker-compose up -d redis
    pytest tests/integration/test_redis_store_integration.py -v

    # For Docker Redis with password:
    REDIS_URL="redis://default:cartolapy_secret_password@localhost:6379" \
      pytest tests/integration/test_redis_store_integration.py -v

Note: REDIS_URL defaults to redis://localhost:6379 via pyproject.toml pytest.env config.
"""

import os

import pandas as pd
import pytest


class TestRedisStoreIntegration:
    """Tests that Redis store operations work correctly with a real Redis instance."""

    @pytest.fixture
    def store(self):
        from src.services.redis_store import RedisDataFrameStore

        redis_url = os.environ.get("REDIS_URL", "redis://localhost:6379")
        return RedisDataFrameStore(redis_url=redis_url)

    @pytest.fixture
    def sample_df(self):
        return pd.DataFrame(
            [
                {
                    "atleta_id": 89256,
                    "status_id": 2,
                    "preco_num": 14.8,
                    "apelido": "Gerson",
                }
            ]
        )

    def test_save_and_load_dataframe(self, store, sample_df):
        """DataFrames should be saved to and loaded from Redis correctly."""
        test_key = "test_atletas_integration"

        store.save_dataframe(test_key, sample_df)

        loaded_df = store.load_dataframe(test_key)
        assert loaded_df is not None
        assert len(loaded_df) == 1
        assert loaded_df.iloc[0]["atleta_id"] == 89256
        assert loaded_df.iloc[0]["apelido"] == "Gerson"

        store.redis.delete(store._key(test_key))

    def test_load_returns_none_when_missing(self, store):
        """Loading a missing key should return None."""
        result = store.load_dataframe("nonexistent_key_12345")
        assert result is None

    def test_save_and_load_confrontos(self, store):
        """Test saving and loading confrontos DataFrame."""
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
        test_key = "test_confrontos_integration"

        store.save_dataframe(test_key, confrontos_df)

        loaded_df = store.load_dataframe(test_key)
        assert loaded_df is not None
        assert len(loaded_df) == 1
        assert loaded_df.iloc[0]["clube_id"] == 264
        assert bool(loaded_df.iloc[0]["is_mandante"]) is True

        store.redis.delete(store._key(test_key))

    def test_overwrite_existing_data(self, store, sample_df):
        """Saving to an existing key should overwrite the data."""
        test_key = "test_overwrite_integration"

        store.save_dataframe(test_key, sample_df)
        original_loaded = store.load_dataframe(test_key)
        assert len(original_loaded) == 1

        new_df = pd.DataFrame(
            [
                {
                    "atleta_id": 99999,
                    "status_id": 7,
                    "preco_num": 5.5,
                    "apelido": "New Player",
                }
            ]
        )
        store.save_dataframe(test_key, new_df)
        new_loaded = store.load_dataframe(test_key)

        assert len(new_loaded) == 1
        assert new_loaded.iloc[0]["atleta_id"] == 99999
        assert new_loaded.iloc[0]["apelido"] == "New Player"

        store.redis.delete(store._key(test_key))
