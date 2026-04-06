from datetime import datetime, timezone

import pandas as pd

from ..enums import Scout
from ..redis_store import RedisDataFrameStore


class PontosCedidos:
    REDIS_KEY = "pontos_cedidos"

    def __init__(self):
        self.columns = [
            "clube_id",
            "posicao_id",
            "is_mandante",
            "rodada_id",
            "pontuacao",
            "pontuacao_basica",
            *Scout.as_list(),
        ]
        self._df = pd.DataFrame(columns=self.columns)
        self._last_updated: datetime | None = None

    @property
    def df(self):
        return self._df

    @property
    def last_updated(self) -> datetime | None:
        return self._last_updated

    def fill_pontos_cedidos(
        self, pontuacoes_df: pd.DataFrame, confrontos_df: pd.DataFrame
    ):
        pontuacoes_agg = pontuacoes_df.groupby(
            ["clube_id", "posicao_id", "rodada_id"], as_index=False
        ).agg(
            {col: "mean" for col in ["pontuacao", "pontuacao_basica", *Scout.as_list()]}
        )

        positions_df = pontuacoes_df.loc[:, ["posicao_id"]].drop_duplicates()

        self._df = (
            confrontos_df.merge(positions_df, how="cross")
            .merge(
                pontuacoes_agg.rename(columns={"clube_id": "opponent_clube_id"}),
                on=["opponent_clube_id", "posicao_id", "rodada_id"],
            )
            .drop(columns=["opponent_clube_id"])
        )
        self._last_updated = datetime.now(timezone.utc)

    def save_to_redis(self, store: RedisDataFrameStore) -> None:
        store.save_dataframe(self.REDIS_KEY, self._df)
        store.save_last_updated(self.REDIS_KEY, self._last_updated)

    @classmethod
    def load_from_redis(cls, store: RedisDataFrameStore) -> "PontosCedidos | None":
        df = store.load_dataframe(cls.REDIS_KEY)
        if df is None:
            return None
        pontos_cedidos = object.__new__(cls)
        pontos_cedidos.columns = [
            "clube_id",
            "posicao_id",
            "is_mandante",
            "rodada_id",
            "pontuacao",
            "pontuacao_basica",
            *Scout.as_list(),
        ]
        pontos_cedidos._df = df
        pontos_cedidos._last_updated = store.load_last_updated(cls.REDIS_KEY)
        return pontos_cedidos
