from datetime import datetime, timezone

import pandas as pd

from ..cartola_models import validate_mercado_response
from ..request_handler import RequestHandler
from ..redis_store import RedisDataFrameStore


class Atletas:
    REDIS_KEY = "atletas"

    def __init__(self, request_handler: RequestHandler):
        self.columns = [
            "atleta_id",
            "rodada_id",
            "clube_id",
            "posicao_id",
            "status_id",
            "preco_num",
            "apelido",
        ]
        self.request_handler = request_handler
        self._df = pd.DataFrame(columns=self.columns)
        self._rodada_id: int | None = None
        self._last_updated: datetime | None = None
        self._clubes: dict | None = None
        self._posicoes: dict | None = None
        self._status: dict | None = None

    @property
    def rodada_id(self):
        return self._rodada_id

    @property
    def df(self):
        return self._df

    @property
    def last_updated(self) -> datetime | None:
        return self._last_updated

    async def fill_atletas(self):
        page_json = await self.request_handler.make_get_request(
            "https://api.cartola.globo.com/atletas/mercado"
        )
        validated = validate_mercado_response(page_json)
        self._rodada_id = (
            validated.rodada_id or validated.atletas[0].rodada_id
            if validated.atletas
            else None
        )
        self._df = pd.DataFrame([a.model_dump() for a in validated.atletas])[
            self.columns
        ]
        self._clubes = {k: v.model_dump() for k, v in validated.clubes.items()}
        self._posicoes = {k: v.model_dump() for k, v in validated.posicoes.items()}
        self._status = {k: v.model_dump() for k, v in validated.status.items()}
        self._last_updated = datetime.now(timezone.utc)

    def save_to_redis(self, store: RedisDataFrameStore) -> None:
        store.save_dataframe(self.REDIS_KEY, self._df)
        store.save_rodada_id(self._rodada_id)
        store.save_last_updated(self.REDIS_KEY, self._last_updated)
        if self._clubes:
            store.save_json("clubes", self._clubes)
        if self._posicoes:
            store.save_json("posicoes", self._posicoes)
        if self._status:
            store.save_json("status", self._status)

    @classmethod
    def load_from_redis(cls, store: RedisDataFrameStore) -> "Atletas | None":
        df = store.load_dataframe(cls.REDIS_KEY)
        if df is None:
            return None
        atletas = object.__new__(cls)
        atletas.columns = [
            "atleta_id",
            "rodada_id",
            "clube_id",
            "posicao_id",
            "status_id",
            "preco_num",
            "apelido",
        ]
        atletas.request_handler = None
        atletas._df = df
        atletas._rodada_id = store.load_rodada_id()
        atletas._last_updated = store.load_last_updated(cls.REDIS_KEY)
        atletas._clubes = store.load_json("clubes")
        atletas._posicoes = store.load_json("posicoes")
        atletas._status = store.load_json("status")
        return atletas
