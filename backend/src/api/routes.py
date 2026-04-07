import logging
from datetime import datetime, timezone
from typing import Annotated

import pandas as pd
from fastapi import APIRouter, Depends, HTTPException, Query, Request, status
from slowapi import Limiter
from slowapi.util import get_remote_address

from ..dependencies import get_data_loader, get_redis_store
from ..services import DataLoader
from ..services.atletas_unified import compute_atletas_unified
from ..services.cartola_models import validate_partidas_response
from ..services.pontos_cedidos_unified import compute_pontos_cedidos_unified
from ..services.redis_store import RedisDataFrameStore
from .auth import verify_api_key, verify_admin_api_key
from .models import (
    AtletaHistoricoItem,
    AtletaHistoricoResponse,
    ConfrontoMatchResponse,
    ConfrontosResponse,
    IsMandante,
    MatchPontosCedidosListResponse,
    MatchPontosCedidosResponse,
    PlayerConfrontoResponse,
    ProximoJogoResponse,
    SortDirection,
    TableResponse,
    TableStatus,
    UpdateResponse,
)

logger = logging.getLogger(__name__)

limiter = Limiter(key_func=get_remote_address)

router = APIRouter()


@router.get("/tables/atletas", response_model=TableResponse)
@limiter.limit("100/minute")
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
@limiter.limit("100/minute")
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
@limiter.limit("100/minute")
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
@limiter.limit("100/minute")
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
@limiter.limit("100/minute")
async def get_table_status(
    request: Request,
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
    validated = validate_partidas_response(page_json)

    clubes = validated.clubes

    result = []
    for p in validated.partidas:
        if not p.valida:
            continue
        clube_casa_id_str = str(p.clube_casa_id)
        clube_visitante_id_str = str(p.clube_visitante_id)
        result.append(
            {
                "partida_id": p.partida_id,
                "mandante_id": p.clube_casa_id,
                "visitante_id": p.clube_visitante_id,
                "mandante_escudo": clubes.get(clube_casa_id_str, {}).escudos.get(
                    "60x60", ""
                ),
                "visitante_escudo": clubes.get(clube_visitante_id_str, {}).escudos.get(
                    "60x60", ""
                ),
                "mandante_nome": clubes.get(clube_casa_id_str, {}).nome_fantasia
                or clubes.get(clube_casa_id_str, {}).nome,
                "visitante_nome": clubes.get(clube_visitante_id_str, {}).nome_fantasia
                or clubes.get(clube_visitante_id_str, {}).nome,
                "placar_oficial_mandante": p.placar_oficial_mandante,
                "placar_oficial_visitante": p.placar_oficial_visitante,
                "local": p.local,
                "partida_data": p.partida_data,
            }
        )

    return result


@router.get("/partidas/{rodada}", response_model=list[dict])
@limiter.limit("100/minute")
async def get_partidas(
    request: Request,
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


@router.get("/confrontos/{rodada}", response_model=ConfrontosResponse)
@limiter.limit("100/minute")
async def get_confrontos_detail(
    request: Request,
    rodada: int,
    data_loader: Annotated[DataLoader, Depends(get_data_loader)],
    store: Annotated[RedisDataFrameStore, Depends(get_redis_store)],
):
    cache_key = f"partidas:{rodada}"
    cached = store.load_json(cache_key)

    if not cached:
        cached = await fetch_partidas_from_cartola(data_loader.request_handler, rodada)
        store.save_json(cache_key, cached)
        store.save_last_updated(cache_key, datetime.now(timezone.utc))

    posicoes_cache = store.load_json("posicoes")

    pontuacoes_df = data_loader.pontuacoes.df.copy().assign(
        atleta_id=lambda df_: pd.to_numeric(df_["atleta_id"], errors="coerce").astype(
            "Int64"
        )
    )
    atletas_df = data_loader.atletas.df

    pontuacoes_rodada = pontuacoes_df.loc[
        pontuacoes_df["rodada_id"] == rodada
    ].drop_duplicates(subset=["atleta_id"])

    matches = []
    for match in cached:
        mandante_id = match["mandante_id"]
        visitante_id = match["visitante_id"]

        def build_players(clube_id: int) -> list[PlayerConfrontoResponse]:
            club_pont = pontuacoes_rodada[pontuacoes_rodada["clube_id"] == clube_id]
            if club_pont.empty:
                return []

            club_atletas = atletas_df.loc[
                atletas_df["atleta_id"].isin(club_pont["atleta_id"])
            ][["atleta_id", "apelido"]].drop_duplicates(subset=["atleta_id"])

            merged = club_pont.merge(club_atletas, on="atleta_id", how="left")

            merged = merged.sort_values(
                ["posicao_id", "apelido"],
                ascending=[True, True],
                na_position="first",
            )

            players = []
            for _, row in merged.iterrows():
                scouts = {
                    k: int(row[k])
                    for k in [
                        "G",
                        "A",
                        "FT",
                        "FD",
                        "FF",
                        "FS",
                        "PS",
                        "V",
                        "I",
                        "PP",
                        "DS",
                        "SG",
                        "DE",
                        "DP",
                        "CV",
                        "CA",
                        "FC",
                        "GC",
                        "GS",
                        "PC",
                    ]
                    if row.get(k, 0) != 0
                }
                pos_abrev = (
                    posicoes_cache.get(str(int(row["posicao_id"])), {})
                    .get("abreviacao", "")
                    .upper()
                    if posicoes_cache
                    else ""
                )
                players.append(
                    PlayerConfrontoResponse(
                        atleta_id=int(row["atleta_id"]),
                        apelido=str(row["apelido"]) if pd.notna(row["apelido"]) else "",
                        posicao_abreviacao=pos_abrev,
                        pontuacao=float(row["pontuacao"])
                        if pd.notna(row["pontuacao"])
                        else 0.0,
                        pontuacao_basica=float(row["pontuacao_basica"])
                        if pd.notna(row["pontuacao_basica"])
                        else 0.0,
                        scouts=scouts,
                    )
                )
            return players

        matches.append(
            ConfrontoMatchResponse(
                partida_id=match.get("partida_id"),
                mandante_id=mandante_id,
                mandante_nome=match.get("mandante_nome", ""),
                mandante_escudo=match.get("mandante_escudo", ""),
                visitante_id=visitante_id,
                visitante_nome=match.get("visitante_nome", ""),
                visitante_escudo=match.get("visitante_escudo", ""),
                placar_mandante=match.get("placar_oficial_mandante"),
                placar_visitante=match.get("placar_oficial_visitante"),
                local=match.get("local"),
                partida_data=match.get("partida_data"),
                mandante_players=build_players(mandante_id),
                visitante_players=build_players(visitante_id),
            )
        )

    return ConfrontosResponse(rodada=rodada, matches=matches)


@router.get("/tables/filter-options")
@limiter.limit("100/minute")
async def get_filter_options(
    request: Request,
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
@limiter.limit("10/minute")
async def update_atletas(
    request: Request,
    _: Annotated[str, Depends(verify_api_key)],
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
    except Exception:
        logger.exception("Failed to update atletas")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to update atletas data",
        )


@router.get("/tables/atletas-unified", response_model=TableResponse)
@limiter.limit("100/minute")
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

    def parse_ids(param: str | None) -> list[int] | None:
        if not param:
            return None
        result = []
        for x in param.split(","):
            x = x.strip()
            if x.lstrip("-").isdigit():
                result.append(int(x))
        return result if result else None

    parsed_clube_ids = parse_ids(clube_ids)
    parsed_posicao_ids = parse_ids(posicao_ids)
    parsed_status_ids = parse_ids(status_ids)

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


@router.get(
    "/tables/atletas/{atleta_id}/historico", response_model=AtletaHistoricoResponse
)
@limiter.limit("100/minute")
async def get_atleta_historico(
    request: Request,
    atleta_id: int,
    data_loader: Annotated[DataLoader, Depends(get_data_loader)],
    store: Annotated[RedisDataFrameStore, Depends(get_redis_store)],
    rodada_min: int = Query(default=1, ge=1),
    rodada_max: int | None = None,
    is_mandante: IsMandante = Query(default=IsMandante.GERAL),
):
    rodada_atual = store.load_rodada_id() or 1
    if rodada_max is None:
        rodada_max = rodada_atual

    pontuacoes_df = data_loader.pontuacoes.df
    confrontos_df = data_loader.confrontos.df
    clubes_cache = store.load_json("clubes") or {}

    filtered = pontuacoes_df[
        (pontuacoes_df["atleta_id"].astype(str) == str(atleta_id))
        & (pontuacoes_df["rodada_id"] >= rodada_min)
        & (pontuacoes_df["rodada_id"] <= rodada_max)
    ].copy()

    if filtered.empty:
        return AtletaHistoricoResponse(atleta_id=atleta_id, historico=[])

    filtered = filtered.merge(
        confrontos_df[
            ["partida_id", "opponent_clube_id", "clube_id", "rodada_id", "is_mandante"]
        ],
        on=["clube_id", "rodada_id"],
        how="left",
    )

    if is_mandante == IsMandante.MANDANTE:
        filtered = filtered[filtered["is_mandante"]]
    elif is_mandante == IsMandante.VISITANTE:
        filtered = filtered[~filtered["is_mandante"]]

    scout_cols = [
        "G",
        "A",
        "FT",
        "FD",
        "FF",
        "FS",
        "PS",
        "V",
        "I",
        "PP",
        "DS",
        "SG",
        "DE",
        "DP",
        "CV",
        "CA",
        "FC",
        "GC",
        "GS",
        "PC",
    ]

    results = []
    for _, row in filtered.iterrows():
        opponent_clube_id = (
            int(row["opponent_clube_id"])
            if pd.notna(row["opponent_clube_id"])
            else None
        )
        opponent_clube_data = (
            clubes_cache.get(str(opponent_clube_id), {}) if opponent_clube_id else {}
        )
        opponent_nome = opponent_clube_data.get("nome", "")
        opponent_escudo = (
            opponent_clube_data.get("escudos", {}).get("60x60", "")
            if opponent_clube_data
            else ""
        )

        scouts = {}
        for scout in scout_cols:
            val = row.get(scout, 0)
            if pd.notna(val) and int(val) != 0:
                scouts[scout] = int(val)

        results.append(
            AtletaHistoricoItem(
                rodada_id=int(row["rodada_id"]),
                partida_id=int(row.get("partida_id_y", row.get("partida_id", 0)))
                if pd.notna(row.get("partida_id_y", row.get("partida_id")))
                else 0,
                pontuacao=float(row["pontuacao"])
                if pd.notna(row["pontuacao"])
                else 0.0,
                pontuacao_basica=float(row["pontuacao_basica"])
                if pd.notna(row["pontuacao_basica"])
                else 0.0,
                is_mandante=bool(row["is_mandante"])
                if pd.notna(row["is_mandante"])
                else True,
                opponent_clube_id=opponent_clube_id or 0,
                opponent_nome=opponent_nome,
                opponent_escudo=opponent_escudo,
                scouts=scouts,
            )
        )

    results.sort(key=lambda x: x.rodada_id, reverse=True)

    return AtletaHistoricoResponse(atleta_id=atleta_id, historico=results)


@router.get("/tables/pontos-cedidos-unified", response_model=TableResponse)
@limiter.limit("100/minute")
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


@router.get(
    "/tables/pontos-cedidos-unified/{clube_id}/matches",
    response_model=MatchPontosCedidosListResponse,
)
@limiter.limit("100/minute")
async def get_pontos_cedidos_unified_matches(
    request: Request,
    clube_id: int,
    data_loader: Annotated[DataLoader, Depends(get_data_loader)],
    store: Annotated[RedisDataFrameStore, Depends(get_redis_store)],
    rodada_min: int = Query(default=1, ge=1),
    rodada_max: int | None = None,
    posicao_id: int = Query(default=1, ge=1),
):
    rodada_atual = store.load_rodada_id() or 1
    if rodada_max is None:
        rodada_max = rodada_atual

    pontos_cedidos_df = data_loader.pontos_cedidos.df
    if pontos_cedidos_df.empty:
        return MatchPontosCedidosListResponse(matches=[])

    filtered = pontos_cedidos_df[
        (pontos_cedidos_df["clube_id"] == clube_id)
        & (pontos_cedidos_df["rodada_id"] >= rodada_min)
        & (pontos_cedidos_df["rodada_id"] <= rodada_max)
        & (pontos_cedidos_df["posicao_id"] == posicao_id)
    ].copy()

    if filtered.empty:
        return MatchPontosCedidosListResponse(matches=[])

    confrontos_df = data_loader.confrontos.df
    filtered = filtered.merge(
        confrontos_df[["partida_id", "opponent_clube_id", "rodada_id", "is_mandante"]],
        on=["partida_id", "is_mandante", "rodada_id"],
        how="left",
        suffixes=("", "_conf"),
    )

    clubes_cache = store.load_json("clubes") or {}

    results = []
    for _, row in filtered.iterrows():
        opponent_clube_id = (
            int(row["opponent_clube_id"])
            if pd.notna(row["opponent_clube_id"])
            else None
        )
        opponent_clube_data = (
            clubes_cache.get(str(opponent_clube_id), {}) if opponent_clube_id else {}
        )
        opponent_nome = opponent_clube_data.get("nome", "")
        opponent_escudo = (
            opponent_clube_data.get("escudos", {}).get("60x60", "")
            if opponent_clube_data
            else ""
        )

        results.append(
            MatchPontosCedidosResponse(
                partida_id=int(row["partida_id"]) if pd.notna(row["partida_id"]) else 0,
                rodada_id=int(row["rodada_id"]),
                opponent_clube_id=opponent_clube_id or 0,
                opponent_nome=opponent_nome,
                opponent_escudo=opponent_escudo,
                is_mandante=bool(row["is_mandante"])
                if pd.notna(row["is_mandante"])
                else True,
                pontuacao=float(row["pontuacao"])
                if pd.notna(row["pontuacao"])
                else 0.0,
                pontuacao_basica=float(row["pontuacao_basica"])
                if pd.notna(row["pontuacao_basica"])
                else 0.0,
            )
        )

    results.sort(key=lambda x: x.rodada_id, reverse=True)

    return MatchPontosCedidosListResponse(matches=results)


@router.get("/proximo-jogo/{clube_id}", response_model=ProximoJogoResponse)
@limiter.limit("100/minute")
async def get_proximo_jogo(
    request: Request,
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
@limiter.limit("10/minute")
async def get_redis_all(
    request: Request,
    _: Annotated[str, Depends(verify_admin_api_key)],
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

    cursor = 0
    while True:
        cursor, keys_batch = store.redis.scan(
            cursor=cursor, match="cartolapy:partidas:*", count=100
        )
        for key in keys_batch:
            decoded_key = key.decode() if isinstance(key, bytes) else key
            nome = decoded_key.replace("cartolapy:", "")
            result[nome] = store.load_json(nome)
        if cursor == 0:
            break

    return result
