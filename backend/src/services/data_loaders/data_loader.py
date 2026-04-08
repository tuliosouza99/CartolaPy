import asyncio

from ..redis_store import RedisDataFrameStore
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

    @property
    def request_handler(self):
        return self._request_handler

    @request_handler.setter
    def request_handler(self, value):
        self._request_handler = value

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

    def save_all_to_redis(self, store: RedisDataFrameStore) -> None:
        self.atletas.save_to_redis(store)
        self.confrontos.save_to_redis(store)
        self.pontuacoes.save_to_redis(store)
        self.pontos_cedidos.save_to_redis(store)

    def load_all_from_redis(self, store: RedisDataFrameStore) -> bool:
        atletas = Atletas.load_from_redis(store)
        if atletas is None:
            return False

        confrontos = Confrontos.load_from_redis(store)
        if confrontos is None:
            return False

        pontuacoes = Pontuacoes.load_from_redis(store)
        if pontuacoes is None:
            return False

        pontos_cedidos = PontosCedidos.load_from_redis(store)
        if pontos_cedidos is None:
            return False

        self.atletas = atletas
        self.atletas.request_handler = self.request_handler

        self.confrontos = confrontos
        self.confrontos.request_handler = self.request_handler

        self.pontuacoes = pontuacoes
        self.pontuacoes.request_handler = self.request_handler

        self.pontos_cedidos = pontos_cedidos

        return True

    def reload_atletas_if_stale(self, store: RedisDataFrameStore) -> bool:
        redis_updated = store.load_last_updated(Atletas.REDIS_KEY)
        if redis_updated is None:
            return False
        if self.atletas.last_updated is None:
            return False
        if redis_updated > self.atletas.last_updated:
            atletas = Atletas.load_from_redis(store)
            if atletas is None:
                return False
            atletas.request_handler = self.request_handler
            self.atletas = atletas
            return True
        return False

    def reload_confrontos_if_stale(self, store: RedisDataFrameStore) -> bool:
        redis_updated = store.load_last_updated(Confrontos.REDIS_KEY)
        if redis_updated is None:
            return False
        if self.confrontos.last_updated is None:
            return False
        if redis_updated > self.confrontos.last_updated:
            confrontos = Confrontos.load_from_redis(store)
            if confrontos is None:
                return False
            confrontos.request_handler = self.request_handler
            self.confrontos = confrontos
            return True
        return False

    def reload_pontuacoes_if_stale(self, store: RedisDataFrameStore) -> bool:
        redis_updated = store.load_last_updated(Pontuacoes.REDIS_KEY)
        if redis_updated is None:
            return False
        if self.pontuacoes.last_updated is None:
            return False
        if redis_updated > self.pontuacoes.last_updated:
            pontuacoes = Pontuacoes.load_from_redis(store)
            if pontuacoes is None:
                return False
            pontuacoes.request_handler = self.request_handler
            self.pontuacoes = pontuacoes
            return True
        return False

    def reload_pontos_cedidos_if_stale(self, store: RedisDataFrameStore) -> bool:
        redis_updated = store.load_last_updated(PontosCedidos.REDIS_KEY)
        if redis_updated is None:
            return False
        if self.pontos_cedidos.last_updated is None:
            return False
        if redis_updated > self.pontos_cedidos.last_updated:
            pontos_cedidos = PontosCedidos.load_from_redis(store)
            if pontos_cedidos is None:
                return False
            self.pontos_cedidos = pontos_cedidos
            return True
        return False

    def reload_all_if_stale(self, store: RedisDataFrameStore) -> dict[str, bool]:
        return {
            "atletas": self.reload_atletas_if_stale(store),
            "confrontos": self.reload_confrontos_if_stale(store),
            "pontuacoes": self.reload_pontuacoes_if_stale(store),
            "pontos_cedidos": self.reload_pontos_cedidos_if_stale(store),
        }
