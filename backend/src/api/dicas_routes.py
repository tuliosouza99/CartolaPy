import asyncio
import json
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import StreamingResponse

from ..dependencies import get_redis_store
from ..services.dicas_da_rodada import (
    DicasReportCache,
    delete_archived_report,
    evaluate_matchup_strategy_from_store,
    list_archived_report_rounds,
    list_archived_report_seasons,
    list_archived_reports,
    load_archived_report,
    report_key,
)
from ..services.dicas_memory import current_season_year
from ..services.redis_store import RedisDataFrameStore
from .models import (
    DicasGenerateResponse,
    DicasHistoryResponse,
    DicasReport,
    DicasStatusResponse,
)

router = APIRouter()


def _next_rodada(store: RedisDataFrameStore) -> int:
    return (store.load_rodada_id() or 1) + 1


async def _enqueue_generation(run: dict) -> None:
    from ..tasks import generate_dicas_da_rodada_task

    await generate_dicas_da_rodada_task.kiq(
        run_id=run["run_id"],
        rodada=run["rodada"],
    )


@router.get("/dicas-da-rodada", response_model=DicasStatusResponse)
async def get_dicas_da_rodada(
    store: Annotated[RedisDataFrameStore, Depends(get_redis_store)],
):
    rodada = _next_rodada(store)
    cache = DicasReportCache(store)
    return DicasStatusResponse(
        season_year=current_season_year(),
        rodada=rodada,
        report=cache.get_report(rodada),
        active_run=cache.get_active_run(rodada),
    )


@router.post("/dicas-da-rodada/generate", response_model=DicasGenerateResponse)
async def generate_dicas_da_rodada(
    store: Annotated[RedisDataFrameStore, Depends(get_redis_store)],
):
    rodada = _next_rodada(store)
    cache = DicasReportCache(store)
    report = cache.get_report(rodada)
    if report is not None:
        return DicasGenerateResponse(
            season_year=current_season_year(),
            rodada=rodada,
            started=False,
            run=None,
            report=report,
        )

    active_run = cache.get_active_run(rodada)
    if active_run is not None:
        return DicasGenerateResponse(
            season_year=current_season_year(),
            rodada=rodada,
            started=False,
            run=active_run,
            report=None,
        )

    run = cache.create_run(rodada)
    await _enqueue_generation(run)
    return DicasGenerateResponse(
        season_year=current_season_year(),
        rodada=rodada,
        started=True,
        run=cache.get_run(run["run_id"]) or run,
        report=None,
    )


@router.post("/dicas-da-rodada/regenerate", response_model=DicasGenerateResponse)
async def regenerate_dicas_da_rodada(
    store: Annotated[RedisDataFrameStore, Depends(get_redis_store)],
):
    rodada = _next_rodada(store)
    cache = DicasReportCache(store)
    report = cache.get_report(rodada)
    active_run = cache.get_active_run(rodada)
    if active_run is not None:
        return DicasGenerateResponse(
            season_year=current_season_year(),
            rodada=rodada,
            started=False,
            run=active_run,
            report=report,
        )

    run = cache.create_run(rodada)
    await _enqueue_generation(run)
    return DicasGenerateResponse(
        season_year=current_season_year(),
        rodada=rodada,
        started=True,
        run=cache.get_run(run["run_id"]) or run,
        report=report,
    )


@router.get("/dicas-da-rodada/history", response_model=DicasHistoryResponse)
async def list_dicas_da_rodada_history(
    limit: Annotated[int, Query(ge=1, le=100)] = 30,
    rodada: Annotated[int | None, Query(ge=1)] = None,
    season_year: Annotated[int | None, Query(ge=2000, le=2100)] = None,
):
    reports, rodadas, seasons = await asyncio.gather(
        asyncio.to_thread(list_archived_reports, limit, rodada, season_year),
        asyncio.to_thread(list_archived_report_rounds, season_year),
        asyncio.to_thread(list_archived_report_seasons),
    )
    return DicasHistoryResponse(
        reports=reports,
        rodadas=rodadas,
        seasons=seasons,
    )


@router.get("/dicas-da-rodada/history/{report_id}", response_model=DicasReport)
async def get_dicas_da_rodada_history_report(report_id: str):
    try:
        report = await asyncio.to_thread(load_archived_report, report_id)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    if report is None:
        raise HTTPException(status_code=404, detail="Report not found")
    return report


@router.delete("/dicas-da-rodada/history/{report_id}")
async def delete_dicas_da_rodada_history_report(
    report_id: str,
    store: Annotated[RedisDataFrameStore, Depends(get_redis_store)],
):
    try:
        report = await asyncio.to_thread(delete_archived_report, report_id)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    if report is None:
        raise HTTPException(status_code=404, detail="Report not found")

    rodada = report.get("rodada")
    season_year = report.get("season_year")
    cleared_current = False
    if isinstance(rodada, int):
        cache_key = report_key(rodada, season_year)
        cached_report = store.load_json(cache_key)
        if cached_report and cached_report.get("report_id") == report_id:
            store.delete(cache_key)
            cleared_current = True

    return {
        "deleted": True,
        "report_id": report_id,
        "rodada": rodada,
        "season_year": season_year,
        "cleared_current": cleared_current,
    }


@router.get("/dicas-da-rodada/eval")
async def evaluate_dicas_da_rodada_strategy(
    store: Annotated[RedisDataFrameStore, Depends(get_redis_store)],
    span: Annotated[int, Query(ge=1, le=10)] = 5,
    lookback_rounds: Annotated[int, Query(ge=1, le=12)] = 6,
):
    return evaluate_matchup_strategy_from_store(
        store=store,
        span=span,
        lookback_rounds=lookback_rounds,
    )


def _format_sse(event: dict, event_id: int) -> str:
    event_type = str(event.get("type") or "message")
    payload = json.dumps(event, default=str, ensure_ascii=False)
    return f"id: {event_id}\nevent: {event_type}\ndata: {payload}\n\n"


@router.get("/dicas-da-rodada/runs/{run_id}/stream")
async def stream_dicas_da_rodada_run(
    request: Request,
    run_id: str,
    store: Annotated[RedisDataFrameStore, Depends(get_redis_store)],
):
    cache = DicasReportCache(store)
    run = cache.get_run(run_id)
    if run is None:
        raise HTTPException(status_code=404, detail="Run not found")

    async def event_generator():
        sent = 0
        while True:
            events = cache.load_events(run_id)
            for index, event in enumerate(events[sent:], start=sent + 1):
                yield _format_sse(event, index)
            sent = len(events)

            current = cache.get_run(run_id)
            terminal = current is None or current.get("status") in {
                "completed",
                "failed",
            }
            if terminal and sent >= len(events):
                break
            if await request.is_disconnected():
                break
            await asyncio.sleep(1)

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )
