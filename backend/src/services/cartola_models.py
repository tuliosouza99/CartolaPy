import logging
from typing import Any

from pydantic import BaseModel, Field, field_validator


logger = logging.getLogger(__name__)


class AtletaItem(BaseModel):
    atleta_id: int
    rodada_id: int | None = None
    clube_id: int
    posicao_id: int
    status_id: int
    preco_num: float
    apelido: str

    @field_validator("atleta_id", "rodada_id", "clube_id", "posicao_id", "status_id")
    @classmethod
    def must_be_positive(cls, v: int | None) -> int | None:
        if v is None:
            return None
        if v < 0:
            raise ValueError(f"Field must be non-negative, got {v}")
        return v

    @field_validator("preco_num")
    @classmethod
    def preco_must_be_non_negative(cls, v: float) -> float:
        if v < 0:
            raise ValueError(f"preco_num must be non-negative, got {v}")
        return v


class ClubeData(BaseModel):
    id: int | None = None
    nome: str = ""
    nome_fantasia: str = ""
    abreviacao: str = ""
    apelido: str = ""
    slug: str = ""
    escudos: dict[str, str] = Field(default_factory=dict)


class PosicaoData(BaseModel):
    id: int
    nome: str
    abreviacao: str = ""


class StatusData(BaseModel):
    id: int
    nome: str


class MercadoResponse(BaseModel):
    atletas: list[AtletaItem]
    clubes: dict[str, ClubeData] = Field(default_factory=dict)
    posicoes: dict[str, PosicaoData] = Field(default_factory=dict)
    status: dict[str, StatusData] = Field(default_factory=dict)
    rodada_id: int | None = None


class PartidaItem(BaseModel):
    partida_id: int
    clube_casa_id: int
    clube_visitante_id: int
    placar_oficial_mandante: int | None = None
    placar_oficial_visitante: int | None = None
    local: str | None = None
    partida_data: str | None = None
    valida: bool = True


class PartidasResponse(BaseModel):
    clubes: dict[str, ClubeData] = Field(default_factory=dict)
    partidas: list[PartidaItem]


class PontuadosResponse(BaseModel):
    atletas: dict[str, dict[str, Any]]

    @field_validator("atletas")
    @classmethod
    def validate_atletas_dict(
        cls, v: dict[str, dict[str, Any]]
    ) -> dict[str, dict[str, Any]]:
        for key, data in v.items():
            if not isinstance(data, dict):
                raise ValueError(f"atleta {key} should be a dict")
            if "clube_id" not in data:
                raise ValueError(f"atleta {key} missing clube_id")
        return v


def validate_mercado_response(page_json: dict[str, Any]) -> MercadoResponse:
    try:
        return MercadoResponse.model_validate(page_json)
    except Exception as e:
        logger.error(f"Invalid mercado response: {e}")
        raise


def validate_partidas_response(page_json: dict[str, Any]) -> PartidasResponse:
    try:
        return PartidasResponse.model_validate(page_json)
    except Exception as e:
        logger.error(f"Invalid partidas response: {e}")
        raise


def validate_pontuados_response(page_json: dict[str, Any]) -> PontuadosResponse:
    try:
        return PontuadosResponse.model_validate(page_json)
    except Exception as e:
        logger.error(f"Invalid pontuados response: {e}")
        raise
