from datetime import datetime
from enum import Enum
from typing import Generic, TypeVar

from pydantic import BaseModel, Field, field_validator

T = TypeVar("T")


class SortDirection(str, Enum):
    ASC = "asc"
    DESC = "desc"


class IsMandante(str, Enum):
    GERAL = "geral"
    MANDANTE = "mandante"
    VISITANTE = "visitante"


class PaginationParams(BaseModel):
    page: int = Field(default=1, ge=1)
    page_size: int = Field(default=20, ge=1, le=1000)


class SortParams(BaseModel):
    sort_by: str
    sort_direction: SortDirection = SortDirection.ASC


class TableResponse(BaseModel, Generic[T]):
    total: int
    page: int
    page_size: int
    data: list[T]
    sort_by: str | None = None
    sort_direction: str | None = None


class SortParamsDep(BaseModel):
    sort_by: str | None = None
    sort_direction: SortDirection = SortDirection.ASC

    @field_validator("sort_direction", mode="before")
    @classmethod
    def validate_sort_direction(cls, v):
        if isinstance(v, str) and v not in ("asc", "desc"):
            raise ValueError("sort_direction must be 'asc' or 'desc'")
        return v


class TableStatus(BaseModel):
    atletas: datetime | None = None
    confrontos: datetime | None = None
    pontuacoes: datetime | None = None
    pontos_cedidos: datetime | None = None
    rodada_atual: int = 1


class UpdateResponse(BaseModel):
    success: bool
    message: str
    updated_at: datetime | None = None


class ProximoJogoResponse(BaseModel):
    mandante_escudo: str
    visitante_escudo: str
    mandante_id: int
    visitante_id: int
    rodada: int


class PlayerConfrontoResponse(BaseModel):
    atleta_id: int
    apelido: str
    posicao_abreviacao: str
    pontuacao: float
    pontuacao_basica: float
    scouts: dict[str, int]


class ConfrontoMatchResponse(BaseModel):
    partida_id: int | None
    mandante_id: int
    mandante_nome: str
    mandante_escudo: str
    visitante_id: int
    visitante_nome: str
    visitante_escudo: str
    placar_mandante: int | None
    placar_visitante: int | None
    local: str | None
    partida_data: str | None
    mandante_players: list[PlayerConfrontoResponse]
    visitante_players: list[PlayerConfrontoResponse]


class ConfrontosResponse(BaseModel):
    rodada: int
    matches: list[ConfrontoMatchResponse]


class MatchPontosCedidosResponse(BaseModel):
    partida_id: int
    rodada_id: int
    opponent_clube_id: int
    opponent_nome: str
    opponent_escudo: str
    is_mandante: bool
    pontuacao: float
    pontuacao_basica: float


class MatchPontosCedidosListResponse(BaseModel):
    matches: list[MatchPontosCedidosResponse]


class AtletaHistoricoItem(BaseModel):
    rodada_id: int
    partida_id: int
    pontuacao: float
    pontuacao_basica: float
    is_mandante: bool
    opponent_clube_id: int
    opponent_nome: str
    opponent_escudo: str
    scouts: dict[str, int]


class AtletaHistoricoResponse(BaseModel):
    atleta_id: int
    historico: list[AtletaHistoricoItem]
