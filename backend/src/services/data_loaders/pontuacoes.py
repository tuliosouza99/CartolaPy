import asyncio

import pandas as pd

from ..cartola_models import validate_pontuados_response
from ..enums import Scout
from ..request_handler import RequestHandler


class Pontuacoes:
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

    async def fill_pontuacoes(self, rodada_atual: int) -> pd.DataFrame:
        async with asyncio.TaskGroup() as tg:
            tasks = [
                tg.create_task(self._fill_pontuacoes_rodada(rodada))
                for rodada in range(1, rodada_atual + 1)
            ]

        rodadas_dfs = [task.result() for task in tasks]
        return pd.concat(
            [pd.DataFrame(columns=self.columns), *rodadas_dfs], ignore_index=True
        )

    async def _fill_pontuacoes_rodada(self, rodada: int) -> pd.DataFrame:
        page_json = await self.request_handler.make_get_request(
            f"https://api.cartola.globo.com/atletas/pontuados/{rodada}"
        )
        validate_pontuados_response(page_json)

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
