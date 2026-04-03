from datetime import datetime
from enum import Enum
from typing import Generic, TypeVar

from pydantic import BaseModel, Field, field_validator

T = TypeVar("T")


class SortDirection(str, Enum):
    ASC = "asc"
    DESC = "desc"


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


class UpdateResponse(BaseModel):
    success: bool
    message: str
    updated_at: datetime | None = None
