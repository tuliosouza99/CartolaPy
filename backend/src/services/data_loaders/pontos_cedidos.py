import pandas as pd

from ..enums import Scout


class PontosCedidos:
    def __init__(self) -> None:
        self.columns = [
            "clube_id",
            "posicao_id",
            "is_mandante",
            "rodada_id",
            "partida_id",
            "pontuacao",
            "pontuacao_basica",
            *Scout.as_list(),
        ]

    def fill_pontos_cedidos(
        self, pontuacoes_df: pd.DataFrame, confrontos_df: pd.DataFrame
    ) -> pd.DataFrame:

        if pontuacoes_df.empty or confrontos_df.empty:
            return pd.DataFrame(columns=self.columns)

        pontuacoes_agg = pontuacoes_df.groupby(
            ["clube_id", "posicao_id", "rodada_id"], as_index=False
        ).agg(
            {col: "mean" for col in ["pontuacao", "pontuacao_basica", *Scout.as_list()]}
        )

        positions_df = pontuacoes_df.loc[:, ["posicao_id"]].drop_duplicates()

        return (
            confrontos_df.merge(positions_df, how="cross")
            .merge(
                pontuacoes_agg.rename(columns={"clube_id": "opponent_clube_id"}),
                on=["opponent_clube_id", "posicao_id", "rodada_id"],
            )
            .drop(columns=["opponent_clube_id"])
        )
