from datetime import datetime, timezone

import pandas as pd

from ..request_handler import RequestHandler


class Atletas:
    def __init__(self, request_handler: RequestHandler):
        self.columns = [
            "atleta_id",
            "rodada_id",
            "clube_id",
            "posicao_id",
            "status_id",
            "preco_num",
            "apelido",
        ]
        self.request_handler = request_handler
        self._df = pd.DataFrame(columns=self.columns)
        self._rodada_id: int | None = None
        self._last_updated: datetime | None = None

    @property
    def rodada_id(self):
        return self._rodada_id

    @property
    def df(self):
        return self._df

    @property
    def last_updated(self) -> datetime | None:
        return self._last_updated

    async def fill_atletas(self):
        page_json = await self.request_handler.make_get_request(
            "https://api.cartola.globo.com/atletas/mercado"
        )
        self._rodada_id = page_json["atletas"][0]["rodada_id"]
        self._df = pd.DataFrame(page_json["atletas"]).loc[:, self.columns]
        self._last_updated = datetime.now(timezone.utc)
