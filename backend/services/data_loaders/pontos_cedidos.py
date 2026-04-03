from datetime import datetime, timezone

import pandas as pd

from ..enums import Scout


class PontosCedidos:
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
            {col: "sum" for col in ["pontuacao", "pontuacao_basica", *Scout.as_list()]}
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
