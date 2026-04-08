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
