import asyncio
import os
from typing import Iterable

import numpy as np
import pandas as pd
from stqdm import stqdm

from src.utils import get_page_json


def create_df(atletas: pd.DataFrame) -> pd.DataFrame:
    rodadas = list(range(1, 39))
    df = pd.DataFrame(
        [(atleta, rodada) for atleta in atletas["atleta_id"] for rodada in rodadas],
        columns=["atleta_id", "rodada"],
    )
    df["pontuacao"] = np.nan
    return df


async def update_pontuacoes_and_scouts_rodada(
    rodada: int, pontuacoes_df: pd.DataFrame, scouts_df: pd.DataFrame
):
    json = await get_page_json(
        f"https://api.cartola.globo.com/atletas/pontuados/{rodada}"
    )

    rodada_df = (
        pd.DataFrame(json["atletas"])
        .T.reset_index()
        .astype({"index": "int64"})
        .sort_values(by=["index"])
        .loc[lambda _df: _df["index"].isin(pontuacoes_df["atleta_id"].unique())]
    )

    mask = pontuacoes_df["atleta_id"].isin(rodada_df["index"].to_list()) & (
        pontuacoes_df["rodada"] == rodada
    )
    pontuacoes_df.loc[mask, "pontuacao"] = (
        pontuacoes_df.loc[mask, "atleta_id"]
        .map(dict(zip(rodada_df["index"], rodada_df["pontuacao"])))
        .values
    )

    mask_scouts = scouts_df["atleta_id"].isin(rodada_df["index"].to_list()) & (
        scouts_df["rodada"] == rodada
    )
    scouts_df.loc[mask_scouts, "scout"] = (
        scouts_df.loc[mask_scouts, "atleta_id"]
        .map(dict(zip(rodada_df["index"], rodada_df["scout"])))
        .values
    )


async def update_pontuacoes_and_scouts(
    rodadas: int | Iterable[int] | None = None, first_round: bool = False
):
    atletas_df = pd.read_csv("data/csv/atletas.csv", index_col=0)
    rodada_atual = int(atletas_df.at[0, "rodada_id"])

    if rodadas is None:
        rodadas = range(1, rodada_atual + 1)
    elif isinstance(rodadas, int):
        rodadas = [rodadas]

    pontuacoes_df = create_df(atletas_df)
    scouts_df = create_df(atletas_df)

    if not first_round:
        await asyncio.gather(
            *[
                update_pontuacoes_and_scouts_rodada(rodada, pontuacoes_df, scouts_df)
                for rodada in stqdm(
                    rodadas,
                    desc="Atualizando as pontuações dos atletas...",
                    backend=True,
                )
            ]
        )

    pontuacoes_df.to_csv("data/csv/pontuacoes.csv", index=False)
    os.makedirs("data/parquet", exist_ok=True)
    scouts_df.to_parquet("data/parquet/scouts.parquet", index=False)
