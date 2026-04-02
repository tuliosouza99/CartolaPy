import asyncio

import pandas as pd

from ..request_handler import RequestHandler


class Confrontos:
    def __init__(self, request_handler: RequestHandler):
        self.columns = ["clube_id", "opponent_clube_id", "is_mandante", "rodada_id"]
        self.request_handler = request_handler
        self._df = pd.DataFrame(columns=self.columns)

    @property
    def df(self):
        return self._df

    async def fill_confrontos(self, rodada_atual: int):
        async with asyncio.TaskGroup() as tg:
            tasks = [
                tg.create_task(self._fill_confrontos_rodada(rodada))
                for rodada in range(1, rodada_atual + 1)
            ]

        rodadas_dfs = [task.result() for task in tasks]
        self._df = pd.concat([self._df, *rodadas_dfs], ignore_index=True)

    async def _fill_confrontos_rodada(self, rodada: int) -> pd.DataFrame:
        page_json = await self.request_handler.make_get_request(
            f"https://api.cartola.globo.com/partidas/{rodada}"
        )
        api_df = pd.DataFrame(page_json["partidas"]).loc[lambda df_: df_["valida"]]

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
