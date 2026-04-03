from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query, Request

from ..dependencies import get_data_loader, get_redis_store
from ..services import DataLoader
from ..services.redis_store import RedisDataFrameStore
from .models import SortDirection, TableResponse, TableStatus, UpdateResponse

router = APIRouter()


@router.get("/tables/atletas", response_model=TableResponse)
async def get_atletas(
    request: Request,
    data_loader: Annotated[DataLoader, Depends(get_data_loader)],
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=20, ge=1),
    sort_by: str | None = None,
    sort_direction: SortDirection = Query(default=SortDirection.ASC),
):
    df = data_loader.atletas.df.copy()

    if sort_by is not None:
        if sort_by not in df.columns:
            raise HTTPException(
                status_code=422, detail=f"Invalid sort_by column: {sort_by}"
            )
        df = df.sort_values(by=sort_by, ascending=sort_direction == SortDirection.ASC)

    total = len(df)
    offset = (page - 1) * page_size
    paginated_df = df.iloc[offset : offset + page_size]

    return TableResponse(
        total=total,
        page=page,
        page_size=page_size,
        data=paginated_df.to_dict(orient="records"),
        sort_by=sort_by,
        sort_direction=sort_direction.value,
    )


@router.get("/tables/pontuacoes", response_model=TableResponse)
async def get_pontuacoes(
    request: Request,
    data_loader: Annotated[DataLoader, Depends(get_data_loader)],
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=20, ge=1),
    sort_by: str | None = None,
    sort_direction: SortDirection = Query(default=SortDirection.ASC),
):
    df = data_loader.pontuacoes.df.copy()

    if sort_by is not None:
        if sort_by not in df.columns:
            raise HTTPException(
                status_code=422, detail=f"Invalid sort_by column: {sort_by}"
            )
        df = df.sort_values(by=sort_by, ascending=sort_direction == SortDirection.ASC)

    total = len(df)
    offset = (page - 1) * page_size
    paginated_df = df.iloc[offset : offset + page_size]

    return TableResponse(
        total=total,
        page=page,
        page_size=page_size,
        data=paginated_df.to_dict(orient="records"),
        sort_by=sort_by,
        sort_direction=sort_direction.value,
    )


@router.get("/tables/confrontos", response_model=TableResponse)
async def get_confrontos(
    request: Request,
    data_loader: Annotated[DataLoader, Depends(get_data_loader)],
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=20, ge=1),
    sort_by: str | None = None,
    sort_direction: SortDirection = Query(default=SortDirection.ASC),
):
    df = data_loader.confrontos.df.copy()

    if sort_by is not None:
        if sort_by not in df.columns:
            raise HTTPException(
                status_code=422, detail=f"Invalid sort_by column: {sort_by}"
            )
        df = df.sort_values(by=sort_by, ascending=sort_direction == SortDirection.ASC)

    total = len(df)
    offset = (page - 1) * page_size
    paginated_df = df.iloc[offset : offset + page_size]

    return TableResponse(
        total=total,
        page=page,
        page_size=page_size,
        data=paginated_df.to_dict(orient="records"),
        sort_by=sort_by,
        sort_direction=sort_direction.value,
    )


@router.get("/tables/pontos-cedidos", response_model=TableResponse)
async def get_pontos_cedidos(
    request: Request,
    data_loader: Annotated[DataLoader, Depends(get_data_loader)],
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=20, ge=1),
    sort_by: str | None = None,
    sort_direction: SortDirection = Query(default=SortDirection.ASC),
):
    df = data_loader.pontos_cedidos.df.copy()

    if sort_by is not None:
        if sort_by not in df.columns:
            raise HTTPException(
                status_code=422, detail=f"Invalid sort_by column: {sort_by}"
            )
        df = df.sort_values(by=sort_by, ascending=sort_direction == SortDirection.ASC)

    total = len(df)
    offset = (page - 1) * page_size
    paginated_df = df.iloc[offset : offset + page_size]

    return TableResponse(
        total=total,
        page=page,
        page_size=page_size,
        data=paginated_df.to_dict(orient="records"),
        sort_by=sort_by,
        sort_direction=sort_direction.value,
    )


@router.get("/tables/status", response_model=TableStatus)
async def get_table_status(
    store: Annotated[RedisDataFrameStore, Depends(get_redis_store)],
):
    return TableStatus(
        atletas=store.load_last_updated("atletas"),
        confrontos=store.load_last_updated("confrontos"),
        pontuacoes=store.load_last_updated("pontuacoes"),
        pontos_cedidos=store.load_last_updated("pontos_cedidos"),
    )


@router.post("/update/atletas", response_model=UpdateResponse)
async def update_atletas(
    data_loader: Annotated[DataLoader, Depends(get_data_loader)],
    store: Annotated[RedisDataFrameStore, Depends(get_redis_store)],
):
    try:
        await data_loader.atletas.fill_atletas()
        data_loader.atletas.save_to_redis(store)
        return UpdateResponse(
            success=True,
            message="Atletas updated successfully",
            updated_at=data_loader.atletas.last_updated,
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
