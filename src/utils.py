import json
from collections.abc import Mapping

import aiohttp
import aiofiles
import numpy as np
import pandas as pd
from aiolimiter import AsyncLimiter

from src.enums import Scout

rate_limiter = AsyncLimiter(10, 1)


async def get_page_json(url: str) -> dict:
    async with rate_limiter:
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                page_json = await response.json()

    return page_json


async def load_dict_async(name: str) -> dict[int, str]:
    async with aiofiles.open(f"data/json/{name}.json", "r") as f:
        json_str = await f.read()

    json_dict = json.loads(json_str)
    return {int(k): v for k, v in json_dict.items()}


def load_dict(name: str) -> dict[int, str]:
    with open(f"data/json/{name}.json", "r") as f:
        json_str = f.read()

    json_dict = json.loads(json_str)
    return {int(k): v for k, v in json_dict.items()}


def create_mando_dict(mandos_df: pd.DataFrame, mando_flag: int) -> dict[int, list[int]]:
    filtered = mandos_df.loc[mandos_df["mando"] == mando_flag]
    grouped = filtered.groupby("clube_id")["rodada"].apply(list)
    return grouped.to_dict()


def get_pontuacoes_mando(
    df: pd.DataFrame,
    partidas_mando_dict: dict[int, list[int]],
    clube_id: int,
    atleta_id: int,
    pontuacoes_df: pd.DataFrame,
):
    rodadas = partidas_mando_dict.get(clube_id, [])
    if not rodadas:
        return df

    mask = (pontuacoes_df["atleta_id"] == atleta_id) & (
        pontuacoes_df["rodada"].isin(rodadas)
    )
    pontuacoes = pontuacoes_df.loc[mask, "pontuacao"].dropna().tolist()

    if len(pontuacoes) > 0:
        df.at[atleta_id, "Média"] = np.mean(pontuacoes)
        df.at[atleta_id, "Desvio Padrão"] = np.std(pontuacoes)
        df.at[atleta_id, "Jogos"] = len(pontuacoes)

        pontuacoes_basicas = (
            pontuacoes_df.loc[mask, "pontuacao_basica"].dropna().tolist()
        )
        if len(pontuacoes_basicas) > 0:
            df.at[atleta_id, "Média Básica"] = np.mean(pontuacoes_basicas)

    return df


def atletas_clean_and_filter(
    atletas_df: pd.DataFrame,
    clubes: list[str],
    posicoes: list[str],
    status: list[str],
    min_jogos: int,
    precos: tuple[int, int],
):
    clubes_dict, status_dict, posicoes_dict = (
        load_dict("clubes"),
        load_dict("status"),
        load_dict("posicoes"),
    )

    query = f"(Preço >= {precos[0]}) & (Preço <= {precos[1]}) & (Jogos >= {min_jogos})"
    if len(clubes) > 0:
        query += f" & (Clube in {clubes})"
    if len(posicoes) > 0:
        query += f" & (Posição in {posicoes})"
    if len(status) > 0:
        query += f" & (Status in {status})"

    return (
        atletas_df.assign(
            **{
                "clube_id": atletas_df["clube_id"].map(clubes_dict),
                "status_id": atletas_df["status_id"].map(status_dict),
                "posicao_id": atletas_df["posicao_id"].map(posicoes_dict),
            }
        )
        .loc[
            :,
            [
                "apelido",
                "clube_id",
                "posicao_id",
                "status_id",
                "preco_num",
                "Média",
                "Média Básica",
                "Desvio Padrão",
                "Jogos",
            ],
        ]
        .rename(
            columns={
                "apelido": "Nome",
                "clube_id": "Clube",
                "posicao_id": "Posição",
                "status_id": "Status",
                "preco_num": "Preço",
            },
        )
        .query(query)
    )


def color_status(status: str):
    if status == "Provável":
        color = "limegreen"
    elif status == "Dúvida":
        color = "gold"
    else:
        color = "indianred"

    return f"color: {color}"


def get_basic_points(scouts: dict | float | None):
    if not isinstance(scouts, Mapping) and (scouts is None or np.isnan(scouts)):
        return np.nan

    valid_scouts = {
        k: v for k, v in scouts.items() if k in Scout.as_basic_scouts_list()
    }

    return sum(
        [
            v * getattr(Scout, k).value["value"] if v is not None else 0
            for k, v in valid_scouts.items()
        ]
    )
