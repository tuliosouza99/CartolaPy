import asyncio
from datetime import datetime, timezone

import pandas as pd

from ..enums import Scout
from ..redis_store import RedisDataFrameStore
from ..request_handler import RequestHandler


class Pontuacoes:
    REDIS_KEY = "pontuacoes"

    def __init__(self, request_handler: RequestHandler):
        self.columns = [
            "atleta_id",
            "posicao_id",
            "clube_id",
            "rodada_id",
            "pontuacao",
            "pontuacao_basica",
            *Scout.as_list(),
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

    async def fill_pontuacoes(self, rodada_atual: int):
        async with asyncio.TaskGroup() as tg:
            tasks = [
                tg.create_task(self._fill_pontuacoes_rodada(rodada))
                for rodada in range(1, rodada_atual + 1)
            ]

        rodadas_dfs = [task.result() for task in tasks]
        self._df = pd.concat([self._df, *rodadas_dfs], ignore_index=True)
        self._last_updated = datetime.now(timezone.utc)

    def save_to_redis(self, store: RedisDataFrameStore) -> None:
        store.save_dataframe(self.REDIS_KEY, self._df)
        store.save_last_updated(self.REDIS_KEY, self._last_updated)

    @classmethod
    def load_from_redis(cls, store: RedisDataFrameStore) -> "Pontuacoes | None":
        df = store.load_dataframe(cls.REDIS_KEY)
        if df is None:
            return None
        pontuacoes = object.__new__(cls)
        pontuacoes.columns = [
            "atleta_id",
            "posicao_id",
            "clube_id",
            "rodada_id",
            "pontuacao",
            "pontuacao_basica",
            *Scout.as_list(),
        ]
        pontuacoes.request_handler = None
        pontuacoes._df = df
        pontuacoes._last_updated = store.load_last_updated(cls.REDIS_KEY)
        return pontuacoes

    async def _fill_pontuacoes_rodada(self, rodada: int) -> pd.DataFrame:
        page_json = await self.request_handler.make_get_request(
            f"https://api.cartola.globo.com/atletas/pontuados/{rodada}"
        )
        rodada_df = (
            pd.DataFrame(page_json["atletas"])
            .T.reset_index(names="atleta_id")
            .loc[lambda df_: df_["entrou_em_campo"]]
        )
        normalized_df = rodada_df.join(pd.json_normalize(rodada_df["scout"])).fillna(0)

        return (
            normalized_df.assign(
                **{k: 0 for k in Scout.as_list() if k not in normalized_df.columns}
            )
            .astype({k: "int64" for k in Scout.as_list()})
            .assign(
                rodada_id=rodada,
                pontuacao_basica=lambda df_: (
                    df_[Scout.as_basic_scouts_list()]
                    .mul(
                        {
                            k: getattr(Scout, k).value["value"]
                            for k in Scout.as_basic_scouts_list()
                        }
                    )
                    .sum(axis=1)
                ),
            )
            .loc[:, self.columns]
        )
