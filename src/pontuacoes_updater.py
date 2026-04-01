import asyncio

import pandas as pd
from stqdm import stqdm

from src.enums import Scout
from src.utils import get_page_json


SCOUT_COLUMNS = [scout.name for scout in Scout]
BASIC_SCOUTS = Scout.as_basic_scouts_list()


def create_base_df(atletas_df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for atleta_id in atletas_df["atleta_id"]:
        for rodada in range(1, 39):
            rows.append({"atleta_id": atleta_id, "rodada": rodada})
    return pd.DataFrame(rows)


async def update_pontuacoes_and_scouts_rodada(
    rodada: int,
    pontuacoes_df: pd.DataFrame,
    atletas_df: pd.DataFrame,
):
    json_data = await get_page_json(
        f"https://api.cartola.globo.com/atletas/pontuados/{rodada}"
    )

    rodada_df = (
        pd.DataFrame(json_data["atletas"])
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

    for scout_name in SCOUT_COLUMNS:
        pontuacoes_df.loc[mask, scout_name] = (
            rodada_df.set_index("index")["scout"]
            .reindex(valid_atletas)
            .apply(lambda x: x.get(scout_name, 0) if isinstance(x, dict) else 0)
            .values
        )


def _compute_pontuacao_basica(row: pd.Series) -> float:
    total = 0.0
    for scout_name in BASIC_SCOUTS:
        value = row.get(scout_name, 0)
        if value is not None and not pd.isna(value):
            scout_enum = getattr(Scout, scout_name)
            total += value * scout_enum.value["value"]
    return total


async def update_pontuacoes_and_scouts(first_round=False):
    atletas_df = pd.read_csv("data/csv/atletas.csv", index_col=0)
    rodada_atual = int(atletas_df.at[0, "rodada_id"])
    pontuacoes_df = create_base_df(atletas_df)

    for scout_name in SCOUT_COLUMNS:
        pontuacoes_df[scout_name] = 0

    if not first_round:
        await asyncio.gather(
            *[
                update_pontuacoes_and_scouts_rodada(rodada, pontuacoes_df, atletas_df)
                for rodada in stqdm(
                    range(1, rodada_atual + 1),
                    desc="Atualizando as pontuações dos atletas...",
                    backend=True,
                )
            ]
        )

    final_df = pontuacoes_df.dropna(subset=["pontuacao"])
    final_df["pontuacao_basica"] = final_df.apply(_compute_pontuacao_basica, axis=1)
    final_df.to_csv("data/csv/pontuacoes_and_scouts.csv", index=False)
