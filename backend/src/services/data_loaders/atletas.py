import pandas as pd

from ..cartola_models import validate_mercado_response
from ..request_handler import RequestHandler


class AtletasResult:
    def __init__(
        self,
        df: pd.DataFrame,
        rodada_id: int | None,
        clubes: dict,
        posicoes: dict,
        status: dict,
    ):
        self.df = df
        self.rodada_id = rodada_id
        self.clubes = clubes
        self.posicoes = posicoes
        self.status = status


class Atletas:
    def __init__(self, request_handler: RequestHandler):
        self.request_handler = request_handler
        self.columns = [
            "atleta_id",
            "rodada_id",
            "clube_id",
            "posicao_id",
            "status_id",
            "preco_num",
            "apelido",
        ]

    async def fill_atletas(self) -> AtletasResult:
        page_json = await self.request_handler.make_get_request(
            "https://api.cartola.globo.com/atletas/mercado"
        )
        validated = validate_mercado_response(page_json)
        rodada_id = (
            validated.rodada_id or validated.atletas[0].rodada_id
            if validated.atletas
            else None
        )

        df = pd.DataFrame([a.model_dump() for a in validated.atletas])[self.columns]
        clubes = {k: v.model_dump() for k, v in validated.clubes.items()}
        posicoes = {k: v.model_dump() for k, v in validated.posicoes.items()}
        status = {k: v.model_dump() for k, v in validated.status.items()}
        return AtletasResult(df, rodada_id, clubes, posicoes, status)
