from datetime import datetime, timezone
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query, Request

from ..dependencies import get_data_loader, get_redis_store
from ..services import DataLoader
from ..services.atletas_unified import compute_atletas_unified
from ..services.pontos_cedidos_unified import compute_pontos_cedidos_unified
from ..services.redis_store import RedisDataFrameStore
from .models import (
    IsMandante,
    SortDirection,
    TableResponse,
    TableStatus,
    UpdateResponse,
    ProximoJogoResponse,
)

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
    data_loader: Annotated[DataLoader, Depends(get_data_loader)],
    store: Annotated[RedisDataFrameStore, Depends(get_redis_store)],
):
    return TableStatus(
        atletas=store.load_last_updated("atletas"),
        confrontos=store.load_last_updated("confrontos"),
        pontuacoes=store.load_last_updated("pontuacoes"),
        pontos_cedidos=store.load_last_updated("pontos_cedidos"),
        rodada_atual=store.load_rodada_id() or 1,
    )


async def fetch_partidas_from_cartola(request_handler, rodada: int) -> list[dict]:
    page_json = await request_handler.make_get_request(
        f"https://api.cartola.globo.com/partidas/{rodada}"
    )

    clubes = page_json.get("clubes", {})

    return [
        {
            "mandante_id": p["clube_casa_id"],
            "visitante_id": p["clube_visitante_id"],
            "mandante_escudo": clubes.get(str(p["clube_casa_id"]), {})
            .get("escudos", {})
            .get("60x60", ""),
            "visitante_escudo": clubes.get(str(p["clube_visitante_id"]), {})
            .get("escudos", {})
            .get("60x60", ""),
        }
        for p in page_json.get("partidas", [])
        if p.get("valida", False)
    ]


@router.get("/partidas/{rodada}", response_model=list[dict])
async def get_partidas(
    rodada: int,
    data_loader: Annotated[DataLoader, Depends(get_data_loader)],
    store: Annotated[RedisDataFrameStore, Depends(get_redis_store)],
):
    cache_key = f"partidas:{rodada}"
    cached = store.load_json(cache_key)

    if cached:
        return cached

    partidas = await fetch_partidas_from_cartola(data_loader.request_handler, rodada)
    store.save_json(cache_key, partidas)
    store.save_last_updated(cache_key, datetime.now(timezone.utc))

    return partidas


@router.get("/tables/filter-options")
async def get_filter_options(
    store: Annotated[RedisDataFrameStore, Depends(get_redis_store)],
):
    clubes_cache = store.load_json("clubes")
    posicoes_cache = store.load_json("posicoes")
    status_cache = store.load_json("status")

    clubes_list = (
        [{"id": int(k), **v} for k, v in clubes_cache.items()] if clubes_cache else []
    )

    posicoes_list = (
        [
            {"id": v["id"], "nome": v["nome"], "abreviacao": v.get("abreviacao", "")}
            for v in posicoes_cache.values()
        ]
        if posicoes_cache
        else []
    )

    status_list = (
        [{"id": v["id"], "nome": v["nome"]} for v in status_cache.values()]
        if status_cache
        else []
    )

    return {
        "clubes": clubes_list,
        "posicoes": posicoes_list,
        "status": status_list,
    }


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


@router.get("/tables/atletas-unified", response_model=TableResponse)
async def get_atletas_unified(
    request: Request,
    data_loader: Annotated[DataLoader, Depends(get_data_loader)],
    store: Annotated[RedisDataFrameStore, Depends(get_redis_store)],
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=20, ge=1),
    sort_by: str | None = None,
    sort_direction: SortDirection = Query(default=SortDirection.ASC),
    rodada_min: int = Query(default=1, ge=1),
    rodada_max: int | None = None,
    is_mandante: IsMandante = Query(default=IsMandante.GERAL),
    search: str | None = Query(default=None),
    clube_ids: str | None = Query(default=None),
    posicao_ids: str | None = Query(default=None),
    status_ids: str | None = Query(default=None),
    preco_min: int | None = Query(default=None),
    preco_max: int | None = Query(default=None),
):
    rodada_atual = store.load_rodada_id() or 1
    if rodada_max is None:
        rodada_max = rodada_atual

    parsed_clube_ids = (
        [int(x) for x in clube_ids.split(",") if x.isdigit()] if clube_ids else None
    )
    parsed_posicao_ids = (
        [int(x) for x in posicao_ids.split(",") if x.isdigit()] if posicao_ids else None
    )
    parsed_status_ids = (
        [int(x) for x in status_ids.split(",") if x.isdigit()] if status_ids else None
    )

    clubes_cache = store.load_json("clubes")
    posicoes_cache = store.load_json("posicoes")
    status_cache = store.load_json("status")

    next_rodada = rodada_atual + 1
    proximo_jogo_cache = store.load_json(f"partidas:{next_rodada}")

    if not proximo_jogo_cache:
        proximo_jogo_cache = await fetch_partidas_from_cartola(
            data_loader.request_handler, next_rodada
        )
        store.save_json(f"partidas:{next_rodada}", proximo_jogo_cache)
        store.save_last_updated(f"partidas:{next_rodada}", datetime.now(timezone.utc))

    df = compute_atletas_unified(
        atletas_df=data_loader.atletas.df,
        pontuacoes_df=data_loader.pontuacoes.df,
        confrontos_df=data_loader.confrontos.df,
        rodada_min=rodada_min,
        rodada_max=rodada_max,
        is_mandante=is_mandante,
        rodada_atual=rodada_atual,
        clubes_cache=clubes_cache,
        posicoes_cache=posicoes_cache,
        status_cache=status_cache,
        proximo_jogo_cache=proximo_jogo_cache,
        search=search,
        clube_ids=parsed_clube_ids,
        posicao_ids=parsed_posicao_ids,
        status_ids=parsed_status_ids,
        preco_min=preco_min,
        preco_max=preco_max,
    )

    output_cols = [
        "atleta_id",
        "apelido",
        "clube_id",
        "clube_escudo",
        "posicao_id",
        "posicao_abreviacao",
        "status_id",
        "status_nome",
        "status_cor",
        "preco",
        "media",
        "media_basica",
        "total_jogos",
        "scouts",
        "proximo_jogo",
    ]

    df = df.loc[:, [c for c in output_cols if c in df.columns]]

    if sort_by is not None:
        if sort_by not in df.columns:
            raise HTTPException(
                status_code=422, detail=f"Invalid sort_by column: {sort_by}"
            )
        df = df.sort_values(by=sort_by, ascending=sort_direction == SortDirection.ASC)

    total = len(df)
    offset = (page - 1) * page_size
    paginated_df = df.iloc[offset : offset + page_size]

    data = paginated_df.to_dict(orient="records")

    return TableResponse(
        total=total,
        page=page,
        page_size=page_size,
        data=data,
        sort_by=sort_by,
        sort_direction=sort_direction.value if sort_direction else None,
    )


@router.get("/tables/pontos-cedidos-unified", response_model=TableResponse)
async def get_pontos_cedidos_unified(
    request: Request,
    data_loader: Annotated[DataLoader, Depends(get_data_loader)],
    store: Annotated[RedisDataFrameStore, Depends(get_redis_store)],
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=20, ge=1),
    sort_by: str | None = None,
    sort_direction: SortDirection = Query(default=SortDirection.ASC),
    rodada_min: int = Query(default=1, ge=1),
    rodada_max: int | None = None,
    is_mandante: IsMandante = Query(default=IsMandante.GERAL),
    posicao_id: int = Query(default=1, ge=1),
):
    rodada_atual = store.load_rodada_id() or 1
    if rodada_max is None:
        rodada_max = rodada_atual

    df = compute_pontos_cedidos_unified(
        pontos_cedidos_df=data_loader.pontos_cedidos.df,
        rodada_min=rodada_min,
        rodada_max=rodada_max,
        is_mandante=is_mandante,
        posicao_id=posicao_id,
    )

    output_cols = [
        "clube_id",
        "media_cedida",
        "media_cedida_basica",
        "total_jogos",
        "scouts",
    ]

    df = df.loc[:, [c for c in output_cols if c in df.columns]]

    if sort_by is not None:
        if sort_by not in df.columns:
            raise HTTPException(
                status_code=422, detail=f"Invalid sort_by column: {sort_by}"
            )
        df = df.sort_values(by=sort_by, ascending=sort_direction == SortDirection.ASC)

    total = len(df)
    offset = (page - 1) * page_size
    paginated_df = df.iloc[offset : offset + page_size]

    data = paginated_df.to_dict(orient="records")

    return TableResponse(
        total=total,
        page=page,
        page_size=page_size,
        data=data,
        sort_by=sort_by,
        sort_direction=sort_direction.value if sort_direction else None,
    )


@router.get("/proximo-jogo/{clube_id}", response_model=ProximoJogoResponse)
async def get_proximo_jogo(
    clube_id: int,
    data_loader: Annotated[DataLoader, Depends(get_data_loader)],
    store: Annotated[RedisDataFrameStore, Depends(get_redis_store)],
):
    rodada_atual = store.load_rodada_id() or 1
    next_rodada = rodada_atual + 1
    cache_key = f"partidas:{next_rodada}"

    cached = store.load_json(cache_key)
    if not cached:
        cached = await fetch_partidas_from_cartola(
            data_loader.request_handler, next_rodada
        )
        store.save_json(cache_key, cached)
        store.save_last_updated(cache_key, datetime.now(timezone.utc))

    for match in cached:
        if str(match["mandante_id"]) == str(clube_id) or str(
            match["visitante_id"]
        ) == str(clube_id):
            if str(match["mandante_id"]) == str(clube_id):
                return ProximoJogoResponse(
                    mandante_escudo=match.get("mandante_escudo", ""),
                    visitante_escudo=match.get("visitante_escudo", ""),
                    mandante_id=match["mandante_id"],
                    visitante_id=match["visitante_id"],
                    rodada=next_rodada,
                )
            else:
                return ProximoJogoResponse(
                    mandante_escudo=match.get("visitante_escudo", ""),
                    visitante_escudo=match.get("mandante_escudo", ""),
                    mandante_id=match["visitante_id"],
                    visitante_id=match["mandante_id"],
                    rodada=next_rodada,
                )

    return ProximoJogoResponse(
        mandante_escudo="",
        visitante_escudo="",
        mandante_id=0,
        visitante_id=0,
        rodada=next_rodada,
    )


@router.get("/redis/all")
async def get_redis_all(
    store: Annotated[RedisDataFrameStore, Depends(get_redis_store)],
):
    keys = [
        "atletas",
        "pontuacoes",
        "confrontos",
        "pontos_cedidos",
        "clubes",
        "posicoes",
        "status",
        "rodada_id",
    ]

    result = {}
    for key in keys:
        result[key] = store.load_json(key)

    metadata = store.load_metadata()
    result["_metadata"] = metadata

    redis_keys = store.redis.keys("cartolapy:partidas:*")
    for key in redis_keys:
        decoded_key = key.decode() if isinstance(key, bytes) else key
        nome = decoded_key.replace("cartolapy:", "")
        result[nome] = store.load_json(nome)

    return result
