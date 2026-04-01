import asyncio
import os

import pandas as pd
from stqdm import stqdm

from src.utils import get_page_json


def create_pontuacoes_df(atletas_df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for atleta_id in atletas_df["atleta_id"]:
        for rodada in range(1, 39):
            rows.append({"atleta_id": atleta_id, "rodada": rodada})
    return pd.DataFrame(rows)


def create_scouts_df(atletas_df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for atleta_id in atletas_df["atleta_id"]:
        for rodada in range(1, 39):
            rows.append({"atleta_id": atleta_id, "rodada": rodada})
    return pd.DataFrame(rows)


async def update_pontuacoes_and_scouts_rodada(
    rodada: int,
    pontuacoes_df: pd.DataFrame,
    scouts_df: pd.DataFrame,
    atletas_df: pd.DataFrame,
):
    json = await get_page_json(
        f"https://api.cartola.globo.com/atletas/pontuados/{rodada}"
    )

    rodada_df = (
        pd.DataFrame(json["atletas"])
        .T.reset_index()
        .astype({"index": "int64"})
        .sort_values(by=["index"])
        .loc[lambda _df: _df["index"].isin(atletas_df["atleta_id"].to_list())]
    )

    valid_atletas = rodada_df.loc[
        rodada_df["entrou_em_campo"] == True, "index"
    ].to_list()

    mask = pontuacoes_df["atleta_id"].isin(valid_atletas) & (
        pontuacoes_df["rodada"] == rodada
    )
    pontuacoes_df.loc[mask, "pontuacao"] = (
        rodada_df.set_index("index")["pontuacao"].reindex(valid_atletas).values
    )

    mask = scouts_df["atleta_id"].isin(valid_atletas) & (scouts_df["rodada"] == rodada)
    scouts_df.loc[mask, "scout"] = (
        rodada_df.set_index("index")["scout"].reindex(valid_atletas).values
    )


async def update_pontuacoes_and_scouts(first_round=False):
    atletas_df = pd.read_csv("data/csv/atletas.csv", index_col=0)
    rodada_atual = int(atletas_df.at[0, "rodada_id"])
    pontuacoes_df = create_pontuacoes_df(atletas_df)
    scouts_df = create_scouts_df(atletas_df)

    if not first_round:
        await asyncio.gather(
            *[
                update_pontuacoes_and_scouts_rodada(
                    rodada, pontuacoes_df, scouts_df, atletas_df
                )
                for rodada in stqdm(
                    range(1, rodada_atual + 1),
                    desc="Atualizando as pontuações dos atletas...",
                    backend=True,
                )
            ]
        )

    pontuacoes_df.dropna(subset=["pontuacao"]).to_csv(
        "data/csv/pontuacoes.csv", index=False
    )
    os.makedirs("data/parquet", exist_ok=True)
    scouts_df.dropna(subset=["scout"]).to_parquet("data/parquet/scouts.parquet")
