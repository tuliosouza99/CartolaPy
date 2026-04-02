from typing import Annotated

from fastapi import APIRouter, Depends
from fastapi.responses import JSONResponse

from backend.services.updater import Updater

router = APIRouter()


def get_updater() -> Updater:
    return Updater()


@router.get("/health")
async def health():
    return {"status": "ok"}


@router.get("/market/status")
async def market_status(updater: Annotated[Updater, Depends(get_updater)]):
    return await updater.get_market_status()


@router.post("/update/trigger")
async def trigger_update(updater: Annotated[Updater, Depends(get_updater)]):
    import asyncio

    asyncio.create_task(updater.full_rebuild())
    return {"message": "Update triggered"}


@router.get("/atletas")
async def get_atletas(updater: Annotated[Updater, Depends(get_updater)]):
    df = updater.get_atletas()
    return JSONResponse(content=df.to_dict(orient="records"))


@router.get("/pontuacoes")
async def get_pontuacoes(updater: Annotated[Updater, Depends(get_updater)]):
    df = updater.get_pontuacoes()
    return JSONResponse(content=df.to_dict(orient="records"))


@router.get("/scouts")
async def get_scouts(updater: Annotated[Updater, Depends(get_updater)]):
    df = updater.get_scouts()
    return JSONResponse(content=df.to_dict(orient="records"))


@router.get("/clubes")
async def get_clubes(updater: Annotated[Updater, Depends(get_updater)]):
    df = updater.get_clubes()
    return JSONResponse(content=df.to_dict(orient="records"))


@router.get("/confrontos")
async def get_confrontos(updater: Annotated[Updater, Depends(get_updater)]):
    df = updater.get_confrontos()
    return JSONResponse(content=df.to_dict(orient="records"))


@router.get("/mandos")
async def get_mandos(updater: Annotated[Updater, Depends(get_updater)]):
    df = updater.get_mandos()
    return JSONResponse(content=df.to_dict(orient="records"))


@router.get("/posicoes")
async def get_posicoes(updater: Annotated[Updater, Depends(get_updater)]):
    df = updater.get_posicoes()
    return JSONResponse(content=df.to_dict(orient="records"))


@router.get("/pontos-cedidos/{posicao}")
async def get_pontos_cedidos(
    posicao: int, updater: Annotated[Updater, Depends(get_updater)]
):
    df = updater.get_pontos_cedidos(posicao)
    return JSONResponse(content=df.to_dict(orient="records"))


@router.get("/dict/status")
async def get_status_dict(updater: Annotated[Updater, Depends(get_updater)]):
    return updater.get_status_dict()


@router.get("/dict/clubes")
async def get_clubes_dict(updater: Annotated[Updater, Depends(get_updater)]):
    return updater.get_clubes_dict()


@router.get("/dict/posicoes")
async def get_posicoes_dict(updater: Annotated[Updater, Depends(get_updater)]):
    return updater.get_posicoes_dict()
