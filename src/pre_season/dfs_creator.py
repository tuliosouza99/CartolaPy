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
    rodadas = list(range(1, 39))

    df = pd.DataFrame(
        [
            (clube, rodada)
            for clube in clubes_df["id"].astype(int)
            for rodada in rodadas
        ],
        columns=["clube_id", "rodada"],
    )

    if table_name == "mandos":
        df["mando"] = np.nan
    else:
        df["adversario_id"] = np.nan

    df.to_csv(f"data/csv/{table_name}.csv", index=False)


async def create_pontos_cedidos_dfs():
    posicoes_df = pd.read_csv("data/csv/posicoes.csv", index_col=0)
    clubes_df = pd.read_csv("data/csv/clubes.csv", index_col=0)
    rodadas = list(range(1, 39))

    df = pd.DataFrame(
        [
            (clube, posicao, rodada)
            for clube in clubes_df["id"].astype(int)
            for posicao in posicoes_df["id"]
            for rodada in rodadas
        ],
        columns=["clube_id", "posicao_id", "rodada"],
    )
    df["pontos_cedidos"] = np.nan

    df.to_csv("data/csv/pontos_cedidos.csv", index=False)
