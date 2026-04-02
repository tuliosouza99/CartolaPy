import asyncio

from ..request_handler import RequestHandler
from .atletas import Atletas
from .confrontos import Confrontos
from .pontos_cedidos import PontosCedidos
from .pontuacoes import Pontuacoes


class DataLoader:
    def __init__(self):
        self.request_handler = RequestHandler()
        self.atletas = Atletas(self.request_handler)
        self.confrontos = Confrontos(self.request_handler)
        self.pontuacoes = Pontuacoes(self.request_handler)
        self.pontos_cedidos = PontosCedidos()

    async def fill_data(self):
        await self.atletas.fill_atletas()
        if self.atletas.rodada_id is None:
            raise ValueError("Rodada ID not found in atletas data")

        await self._update_expensive_tables()

    async def _update_expensive_tables(self):
        async with asyncio.TaskGroup() as tg:
            tg.create_task(self.confrontos.fill_confrontos(self.atletas.rodada_id))
            tg.create_task(self.pontuacoes.fill_pontuacoes(self.atletas.rodada_id))

        await asyncio.to_thread(
            self.pontos_cedidos.fill_pontos_cedidos,
            self.pontuacoes.df,
            self.confrontos.df,
        )
