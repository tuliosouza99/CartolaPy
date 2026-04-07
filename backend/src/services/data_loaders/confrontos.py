import asyncio
from datetime import datetime, timezone

import pandas as pd

from ..cartola_models import validate_partidas_response
from ..request_handler import RequestHandler
from ..redis_store import RedisDataFrameStore


class Confrontos:
    REDIS_KEY = "confrontos"

    def __init__(self, request_handler: RequestHandler):
        self.columns = [
            "clube_id",
            "opponent_clube_id",
            "is_mandante",
            "rodada_id",
            "partida_id",
        ]
        self.request_handler = request_handler
        self._df = pd.DataFrame(columns=self.columns)
        self._last_updated: datetime | None = None

    @property
    def df(self):
        return self._df

    @property
    def last_updated(self) -> datetime | None:
        return self._last_updated

    async def fill_confrontos(self, rodada_atual: int):
        async with asyncio.TaskGroup() as tg:
            tasks = [
                tg.create_task(self._fill_confrontos_rodada(rodada))
                for rodada in range(1, rodada_atual + 1)
            ]

        rodadas_dfs = [task.result() for task in tasks]
        self._df = pd.concat([self._df, *rodadas_dfs], ignore_index=True)
        self._last_updated = datetime.now(timezone.utc)

    def save_to_redis(self, store: RedisDataFrameStore) -> None:
        store.save_dataframe(self.REDIS_KEY, self._df)
        store.save_last_updated(self.REDIS_KEY, self._last_updated)

    @classmethod
    def load_from_redis(cls, store: RedisDataFrameStore) -> "Confrontos | None":
        df = store.load_dataframe(cls.REDIS_KEY)
        if df is None:
            return None
        confrontos = object.__new__(cls)
        confrontos.columns = [
            "clube_id",
            "opponent_clube_id",
            "is_mandante",
            "rodada_id",
            "partida_id",
        ]
        confrontos.request_handler = None
        confrontos._df = df
        confrontos._last_updated = store.load_last_updated(cls.REDIS_KEY)
        return confrontos

    async def _fill_confrontos_rodada(self, rodada: int) -> pd.DataFrame:
        page_json = await self.request_handler.make_get_request(
            f"https://api.cartola.globo.com/partidas/{rodada}"
        )
        validated = validate_partidas_response(page_json)

        if not validated.partidas:
            return pd.DataFrame(columns=self.columns)

        api_df = pd.DataFrame([p.model_dump() for p in validated.partidas])
        api_df = api_df.loc[api_df["valida"]]

        return pd.concat(
            [
                (
                    api_df.rename(
                        columns={
                            "clube_casa_id": "clube_id",
                            "clube_visitante_id": "opponent_clube_id",
                        }
                    )
                    .assign(rodada_id=rodada, is_mandante=True)
                    .loc[:, self.columns]
                ),
                (
                    api_df.rename(
                        columns={
                            "clube_visitante_id": "clube_id",
                            "clube_casa_id": "opponent_clube_id",
                        }
                    )
                    .assign(rodada_id=rodada, is_mandante=False)
                    .loc[:, self.columns]
                ),
            ],
            ignore_index=True,
        )
