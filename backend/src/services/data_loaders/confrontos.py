import asyncio

import pandas as pd

from ..cartola_models import validate_partidas_response
from ..request_handler import RequestHandler


class Confrontos:
    def __init__(self, request_handler: RequestHandler):
        self.request_handler = request_handler
        self.columns = [
            "clube_id",
            "opponent_clube_id",
            "is_mandante",
            "rodada_id",
            "partida_id",
        ]

    async def fill_confrontos(self, rodada_atual: int) -> pd.DataFrame:
        async with asyncio.TaskGroup() as tg:
            tasks = [
                tg.create_task(self._fill_confrontos_rodada(rodada))
                for rodada in range(1, rodada_atual + 1)
            ]

        rodadas_dfs = [task.result() for task in tasks]
        return pd.concat(
            [pd.DataFrame(columns=self.columns), *rodadas_dfs], ignore_index=True
        )

    async def _fill_confrontos_rodada(self, rodada: int) -> pd.DataFrame:
        page_json = await self.request_handler.make_get_request(
            f"https://api.cartola.globo.com/partidas/{rodada}"
        )
        validated = validate_partidas_response(page_json)

        if not validated.partidas:
            return pd.DataFrame()

        api_df = pd.DataFrame([p.model_dump() for p in validated.partidas])
        api_df = api_df.loc[api_df["valida"]]

        return pd.concat(
            [
                (
                    api_df.rename(
                        columns={
                            "clube_casa_id": "clube_id",
                            "clube_visitante_id": "opponent_clube_id",
                        }
                    )
                    .assign(rodada_id=rodada, is_mandante=True)
                    .loc[:, self.columns]
                ),
                (
                    api_df.rename(
                        columns={
                            "clube_visitante_id": "clube_id",
                            "clube_casa_id": "opponent_clube_id",
                        }
                    )
                    .assign(rodada_id=rodada, is_mandante=False)
                    .loc[:, self.columns]
                ),
            ],
            ignore_index=True,
        )
