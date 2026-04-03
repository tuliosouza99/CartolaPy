import io
import json
from datetime import datetime, timezone
from typing import Any

import pandas as pd
import redis


class RedisDataFrameStore:
    PREFIX = "cartolapy"

    def __init__(self, redis_url: str = "redis://localhost:6379"):
        self.redis = redis.from_url(redis_url, decode_responses=False)
        self._verify_connection()

    def _verify_connection(self):
        self.redis.ping()

    def _key(self, name: str) -> str:
        return f"{self.PREFIX}:{name}"

    def save_dataframe(self, key: str, df: pd.DataFrame) -> None:
        buffer = io.BytesIO()
        df.to_parquet(buffer)
        self.redis.set(self._key(key), buffer.getvalue())

    def load_dataframe(self, key: str) -> pd.DataFrame | None:
        data = self.redis.get(self._key(key))
        if data is None:
            return None
        return pd.read_parquet(io.BytesIO(data))

    def save_metadata(self, metadata: dict[str, Any]) -> None:
        serialized = json.dumps(metadata, default=str)
        self.redis.set(self._key("metadata"), serialized.encode())

    def load_metadata(self) -> dict[str, Any]:
        data = self.redis.get(self._key("metadata"))
        if data is None:
            return {}
        if isinstance(data, bytes):
            data = data.decode()
        return json.loads(data)

    def save_rodada_id(self, rodada_id: int | None) -> None:
        if rodada_id is None:
            self.redis.delete(self._key("rodada_id"))
        else:
            self.redis.set(self._key("rodada_id"), str(rodada_id))

    def load_rodada_id(self) -> int | None:
        data = self.redis.get(self._key("rodada_id"))
        if data is None:
            return None
        if isinstance(data, bytes):
            data = data.decode()
        return int(data)

    def save_last_updated(self, table: str, timestamp: datetime | None) -> None:
        metadata = self.load_metadata()
        if timestamp is None:
            metadata.pop(f"{table}_updated", None)
        else:
            metadata[f"{table}_updated"] = timestamp.isoformat()
        self.save_metadata(metadata)

    def load_last_updated(self, table: str) -> datetime | None:
        metadata = self.load_metadata()
        ts = metadata.get(f"{table}_updated")
        if ts is None:
            return None
        return datetime.fromisoformat(ts).replace(tzinfo=timezone.utc)

    def exists(self, key: str) -> bool:
        return self.redis.exists(self._key(key)) > 0

    def has_any_data(self) -> bool:
        keys = [
            "atletas",
            "confrontos",
            "pontuacoes",
            "pontos_cedidos",
        ]
        return any(self.exists(k) for k in keys)
