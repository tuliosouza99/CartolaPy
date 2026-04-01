import asyncio
import os

import numpy as np
import pandas as pd

from src.utils import get_page_json


async def create_clubes_and_posicoes():
    os.makedirs("data/csv", exist_ok=True)
    json = await get_page_json("https://api.cartola.globo.com/atletas/mercado")

    pd.DataFrame(json["clubes"]).T.reset_index(drop=True).to_csv("data/csv/clubes.csv")
    pd.DataFrame(json["posicoes"]).T.reset_index(drop=True).to_csv(
        "data/csv/posicoes.csv"
    )


def create_confrontos_or_mandos(table_name: str):
    clubes_df = pd.read_csv("data/csv/clubes.csv", index_col=0)

    empty_df = pd.DataFrame(
        [
            (clube_id, rodada)
            for clube_id in clubes_df["id"].astype(int)
            for rodada in range(1, 39)
        ],
        columns=["clube_id", "rodada"],
    )
    if table_name == "mandos":
        empty_df["mando"] = np.nan
    else:
        empty_df["adversario"] = np.nan

    empty_df.to_csv(f"data/csv/{table_name}.csv", index=False)


def create_pontos_cedidos_posicao(clubes_df: pd.DataFrame, posicao: int):
    empty_df = pd.DataFrame(
        [
            (posicao, clube_id, rodada)
            for clube_id in clubes_df["id"].astype(int)
            for rodada in range(1, 39)
        ],
        columns=["posicao", "clube_id", "rodada"],
    )
    empty_df["pontos_cedidos"] = np.nan

    return empty_df


def create_pontos_cedidos_dfs():
    posicoes_df = pd.read_csv("data/csv/posicoes.csv", index_col=0)
    clubes_df = pd.read_csv("data/csv/clubes.csv", index_col=0)

    dfs = [
        create_pontos_cedidos_posicao(clubes_df, posicao)
        for posicao in posicoes_df["id"]
    ]
    consolidated_df = pd.concat(dfs, ignore_index=True)
    consolidated_df.to_csv("data/csv/pontos_cedidos.csv", index=False)
