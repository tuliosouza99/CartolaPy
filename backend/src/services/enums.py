from enum import Enum
from typing import ClassVar


class Scout(Enum):
    G: ClassVar[dict[str, str | float]] = {"name": "Gol", "value": 8}
    A: ClassVar[dict[str, str | float]] = {"name": "Assistência", "value": 5}
    FT: ClassVar[dict[str, str | float]] = {
        "name": "Finalização na trave",
        "value": 3,
    }
    FD: ClassVar[dict[str, str | float]] = {
        "name": "Finalização defendida",
        "value": 1.2,
    }
    FF: ClassVar[dict[str, str | float]] = {
        "name": "Finalização pra fora",
        "value": 0.8,
    }
    FS: ClassVar[dict[str, str | float]] = {
        "name": "Falta sofrida",
        "value": 0.5,
    }
    PS: ClassVar[dict[str, str | float]] = {
        "name": "Pênalti sofrido",
        "value": 1,
    }
    V: ClassVar[dict[str, str | float]] = {"name": "Vitória", "value": 1}
    I: ClassVar[dict[str, str | float]] = {"name": "Impedimento", "value": -0.1}
    PP: ClassVar[dict[str, str | float]] = {
        "name": "Pênalti perdido",
        "value": -4,
    }
    DS: ClassVar[dict[str, str | float]] = {"name": "Desarme", "value": 1.5}
    SG: ClassVar[dict[str, str | float]] = {
        "name": "Jogo sem sofrer gol",
        "value": 5,
    }
    DE: ClassVar[dict[str, str | float]] = {"name": "Defesa", "value": 1.3}
    DP: ClassVar[dict[str, str | float]] = {
        "name": "Defesa de pênalti",
        "value": 7,
    }
    CV: ClassVar[dict[str, str | float]] = {
        "name": "Cartão vermelho",
        "value": -3,
    }
    CA: ClassVar[dict[str, str | float]] = {
        "name": "Cartão amarelo",
        "value": -1,
    }
    FC: ClassVar[dict[str, str | float]] = {
        "name": "Falta cometida",
        "value": -0.3,
    }
    GC: ClassVar[dict[str, str | float]] = {"name": "Gol contra", "value": -3}
    GS: ClassVar[dict[str, str | float]] = {"name": "Gol sofrido", "value": -1}
    PC: ClassVar[dict[str, str | float]] = {
        "name": "Pênalti cometido",
        "value": -1,
    }

    @classmethod
    def as_basic_scouts_list(cls):
        return [
            scout.name
            for scout in cls
            if scout.name not in ("G", "A", "FT", "PP", "DP", "SG", "CV", "GC")
        ]

    @classmethod
    def as_list(cls):
        return [scout.name for scout in cls]

    @classmethod
    def get_value(cls, scout_name: str) -> float:
        return cls[scout_name].value["value"]
