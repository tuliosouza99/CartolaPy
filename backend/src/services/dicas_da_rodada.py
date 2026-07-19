from __future__ import annotations

import json
import logging
import os
import re
import ssl
import uuid
from datetime import datetime, timezone
from html import unescape
from typing import Any, Literal
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import pandas as pd

from .atletas_unified import (
    compute_atletas_unified,
    normalize_numeric_columns,
    normalize_string,
)
from .enums import Scout
from .dicas_memory import (
    DicasMemoryError,
    current_season_year,
    get_dicas_memory_store,
    load_memories_for_prediction,
)
from .pontos_cedidos_unified import compute_pontos_cedidos_unified
from .pontos_conquistados_unified import compute_pontos_conquistados_unified
from .redis_store import RedisDataFrameStore

logger = logging.getLogger(__name__)

RECOMMENDED_SPANS = [5, 10]
MAX_ANALYSIS_SPAN = 10
ACTIVE_RUN_TTL_SECONDS = 2 * 60 * 60
RUN_TTL_SECONDS = 24 * 60 * 60
DEFAULT_DICAS_MODEL = "openai:gpt-5.5"
DEFAULT_DICAS_REASONING_EFFORT = "medium"
PICKS_PER_POSITION = 7
POSITION_REPORT_LIMITS = {1: 7, 2: 7, 3: 7, 4: 7, 5: 7, 6: 7}
PRIMARY_PICK_LIMIT = PICKS_PER_POSITION * len(POSITION_REPORT_LIMITS)
PRIMARY_POSITION_CAPS = POSITION_REPORT_LIMITS
KNOWN_ODDS_SOURCES = [
    {
        "name": "FootyStats Serie A odds",
        "url": "https://footystats.org/brazil/serie-a/odds",
        "scope": "previous_and_posted",
    },
    {
        "name": "OddsAgora Brasileirao Betano",
        "url": "https://www.oddsagora.com.br/football/brazil/brasileirao-betano/",
        "scope": "future_fixtures",
    },
]
TEAM_ALIASES = {
    "Athlético-PR": ["Athlético-PR", "Athletico-PR", "Atlético PR", "Atletico PR"],
    "Atlético-MG": ["Atlético-MG", "Atlético Mineiro", "Atletico Mineiro"],
    "Chapecoense": ["Chapecoense", "Chapecoense AF"],
}


class DicasConfigurationError(RuntimeError):
    pass


def validate_analysis_span(span: int) -> int:
    if span < 1 or span > MAX_ANALYSIS_SPAN:
        raise ValueError(f"span must be between 1 and {MAX_ANALYSIS_SPAN}")
    return span


def round_window(rodada: int, span: int) -> tuple[int, int]:
    span = validate_analysis_span(span)
    return max(1, rodada - span), max(1, rodada - 1)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def report_key(rodada: int, season_year: int | None = None) -> str:
    season_year = season_year or current_season_year()
    return f"dicas:{season_year}:report:{rodada}"


def active_run_key(rodada: int, season_year: int | None = None) -> str:
    season_year = season_year or current_season_year()
    return f"dicas:{season_year}:active:{rodada}"


def run_key(run_id: str) -> str:
    return f"dicas:run:{run_id}"


def events_key(run_id: str) -> str:
    return f"dicas:events:{run_id}"


def _json_safe(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): _json_safe(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_json_safe(v) for v in value]
    if isinstance(value, tuple):
        return [_json_safe(v) for v in value]
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if not isinstance(value, (dict, list, tuple, str)):
        try:
            if pd.isna(value):
                return None
        except (TypeError, ValueError):
            pass
    if hasattr(value, "item"):
        try:
            return value.item()
        except (ValueError, TypeError):
            return value
    return value


def save_archived_report(report: dict) -> dict:
    return get_dicas_memory_store().save_report(report)


def load_archived_report(report_id: str) -> dict | None:
    return get_dicas_memory_store().load_report(report_id)


def delete_archived_report(report_id: str) -> dict | None:
    return get_dicas_memory_store().delete_report(report_id)


def list_archived_report_rounds(
    season_year: int | None = None,
) -> list[int]:
    return get_dicas_memory_store().list_report_rounds(season_year=season_year)


def list_archived_report_seasons() -> list[int]:
    return get_dicas_memory_store().list_seasons()


def list_archived_reports(
    limit: int = 30,
    rodada: int | None = None,
    season_year: int | None = None,
) -> list[dict]:
    return get_dicas_memory_store().list_reports(
        limit=limit, rodada=rodada, season_year=season_year
    )


class DicasReportCache:
    def __init__(self, store: RedisDataFrameStore):
        self.store = store

    def get_report(self, rodada: int) -> dict | None:
        value = self.store.load_json(report_key(rodada))
        return value if isinstance(value, dict) else None

    def get_active_run(self, rodada: int) -> dict | None:
        value = self.store.load_json(active_run_key(rodada))
        if not isinstance(value, dict):
            return None
        if value.get("status") in {"completed", "failed"}:
            return None
        return value

    def get_run(self, run_id: str) -> dict | None:
        value = self.store.load_json(run_key(run_id))
        return value if isinstance(value, dict) else None

    def create_run(self, rodada: int) -> dict:
        now = utc_now().isoformat()
        metadata = {
            "run_id": uuid.uuid4().hex,
            "season_year": current_season_year(),
            "rodada": rodada,
            "status": "queued",
            "created_at": now,
            "updated_at": now,
            "completed_at": None,
            "error": None,
        }
        metadata = self.save_run(metadata, active=True)
        self.append_event(
            metadata["run_id"],
            "status",
            "Relatório enfileirado para geração.",
            {"status": "queued", "rodada": rodada},
        )
        return metadata

    def save_run(self, metadata: dict, active: bool = False) -> dict:
        metadata = {**metadata, "updated_at": utc_now().isoformat()}
        self.store.save_json(run_key(metadata["run_id"]), metadata, RUN_TTL_SECONDS)
        if active:
            self.store.save_json(
                active_run_key(metadata["rodada"], metadata.get("season_year")),
                metadata,
                ACTIVE_RUN_TTL_SECONDS,
            )
        return metadata

    def update_run(
        self,
        run_id: str,
        status: Literal["queued", "running", "completed", "failed"],
        error: str | None = None,
    ) -> dict:
        metadata = self.get_run(run_id)
        if metadata is None:
            raise RuntimeError(f"Run not found: {run_id}")
        metadata["status"] = status
        metadata["error"] = error
        if status in {"completed", "failed"}:
            metadata["completed_at"] = utc_now().isoformat()
        metadata = self.save_run(metadata, active=status in {"queued", "running"})
        if status in {"completed", "failed"}:
            self.store.delete(
                active_run_key(metadata["rodada"], metadata.get("season_year"))
            )
        return metadata

    def append_event(
        self,
        run_id: str,
        event_type: str,
        message: str,
        data: dict | None = None,
    ) -> dict:
        event = {
            "type": event_type,
            "message": message,
            "data": data or {},
            "created_at": utc_now().isoformat(),
        }
        length = self.store.append_json(events_key(run_id), event, RUN_TTL_SECONDS)
        event["id"] = length
        return event

    def load_events(self, run_id: str) -> list[dict]:
        return self.store.load_json_list(events_key(run_id))

    def complete_run(self, run_id: str, report: dict) -> dict:
        metadata = self.get_run(run_id)
        if metadata is None:
            raise RuntimeError(f"Run not found: {run_id}")
        archived_report = report
        try:
            archived_report = save_archived_report(report)
        except DicasMemoryError as exc:
            logger.exception("Failed to archive Dicas da Rodada report in S3")
            self.append_event(
                run_id,
                "warning",
                "Relatório gerado, mas não foi possível salvar no histórico da AWS.",
                {"error": str(exc)},
            )
        self.store.save_json(
            report_key(
                archived_report["rodada"],
                archived_report.get("season_year"),
            ),
            archived_report,
        )
        self.append_event(
            run_id,
            "final_report",
            "Relatório final gerado.",
            {"report": archived_report},
        )
        completed = self.update_run(run_id, "completed")
        self.append_event(run_id, "done", "Geração concluída.", {"status": "completed"})
        return completed

    def fail_run(self, run_id: str, error: str) -> dict:
        self.append_event(run_id, "error", error, {"status": "failed"})
        failed = self.update_run(run_id, "failed", error=error)
        self.append_event(
            run_id, "done", "Geração encerrada com erro.", {"status": "failed"}
        )
        return failed


class DicasEventSink:
    def __init__(self, cache: DicasReportCache, run_id: str):
        self.cache = cache
        self.run_id = run_id

    def status(self, message: str, data: dict | None = None) -> None:
        self.cache.append_event(self.run_id, "status", message, data)

    def progress(self, message: str, data: dict | None = None) -> None:
        self.cache.append_event(self.run_id, "progress", message, data)

    def tool_call(self, name: str, data: dict | None = None) -> None:
        self.cache.append_event(
            self.run_id,
            "tool_call",
            f"Consultando {name}.",
            {"tool": name, **(data or {})},
        )

    def tool_result(self, name: str, data: dict | None = None) -> None:
        self.cache.append_event(
            self.run_id,
            "tool_result",
            f"Consulta {name} concluída.",
            {"tool": name, **(data or {})},
        )

    def report_delta(self, text: str) -> None:
        if text:
            self.cache.append_event(
                self.run_id,
                "report_delta",
                "Trecho do relatório recebido.",
                {"text": text},
            )


class CartolaPyApiClient:
    def __init__(self, base_url: str, sink: DicasEventSink):
        self.base_url = base_url.rstrip("/")
        self.sink = sink

    def get(self, path: str, params: dict[str, Any] | None = None) -> Any:
        params = {k: v for k, v in (params or {}).items() if v is not None}
        query = f"?{urlencode(params)}" if params else ""
        url = f"{self.base_url}{path}{query}"
        self.sink.tool_call("CartolaPy API", {"path": path})
        request = Request(url, headers={"Accept": "application/json"})
        try:
            with urlopen(request, timeout=30) as response:
                payload = json.loads(response.read().decode())
        except (HTTPError, URLError, TimeoutError) as exc:
            raise RuntimeError(
                f"CartolaPy API request failed for {path}: {exc}"
            ) from exc
        self.sink.tool_result("CartolaPy API", {"path": path})
        return payload


def _club_name(clubes_cache: dict | None, clube_id: int) -> str:
    clube = (clubes_cache or {}).get(str(int(clube_id)), {})
    return clube.get("nome_fantasia") or clube.get("nome") or f"Clube {clube_id}"


def _position_label(posicoes_cache: dict | None, posicao_id: int) -> str:
    posicao = (posicoes_cache or {}).get(str(int(posicao_id)), {})
    abreviacao = posicao.get("abreviacao")
    nome = posicao.get("nome")
    if abreviacao and nome:
        return f"{abreviacao.upper()} ({nome})"
    return (abreviacao or nome or f"Posição {posicao_id}").upper()


def _scout_label(scout: str) -> str:
    try:
        return Scout[scout].value["name"]
    except KeyError:
        return scout


def _latest_status_by_player(atletas_df: pd.DataFrame) -> pd.DataFrame:
    cols = ["atleta_id", "status_id"]
    if not all(col in atletas_df.columns for col in cols):
        return pd.DataFrame(columns=cols)
    atletas_df = normalize_numeric_columns(atletas_df, ["atleta_id", "rodada_id"])
    return atletas_df.sort_values("rodada_id", ascending=False).drop_duplicates(
        subset=["atleta_id"], keep="first"
    )[cols]


def _pontuacoes_with_mando_and_status(
    pontuacoes_df: pd.DataFrame,
    atletas_df: pd.DataFrame,
    confrontos_df: pd.DataFrame,
) -> pd.DataFrame:
    result = normalize_numeric_columns(
        pontuacoes_df, ["atleta_id", "clube_id", "rodada_id"]
    )
    status_df = _latest_status_by_player(atletas_df)
    if not status_df.empty:
        result = result.merge(status_df, on="atleta_id", how="left")
    if not confrontos_df.empty:
        confrontos_subset = normalize_numeric_columns(
            confrontos_df[["clube_id", "rodada_id", "partida_id", "is_mandante"]],
            ["clube_id", "rodada_id"],
        ).drop_duplicates()
        result = result.merge(
            confrontos_subset, on=["clube_id", "rodada_id"], how="left"
        )
    return result


def _team_form(
    pontuacoes_df: pd.DataFrame,
    clubes_cache: dict | None,
    rodada_min: int,
    rodada_max: int,
    limit: int = 10,
) -> list[dict]:
    if pontuacoes_df.empty:
        return []
    filtered = pontuacoes_df.loc[
        (pontuacoes_df["rodada_id"] >= rodada_min)
        & (pontuacoes_df["rodada_id"] <= rodada_max)
    ].copy()
    if filtered.empty:
        return []
    by_round = (
        filtered.groupby(["clube_id", "rodada_id"], as_index=False)
        .agg(
            pontos_totais=("pontuacao", "sum"),
            pontos_basicos=("pontuacao_basica", "sum"),
            atletas=("atleta_id", "nunique"),
        )
        .sort_values(["clube_id", "rodada_id"])
    )
    form = (
        by_round.groupby("clube_id")
        .agg(
            media_total=("pontos_totais", "mean"),
            media_basica=("pontos_basicos", "mean"),
            jogos=("rodada_id", "nunique"),
            ultima_rodada=("rodada_id", "max"),
            ultimo_total=("pontos_totais", "last"),
        )
        .reset_index()
        .sort_values("media_total", ascending=False)
        .head(limit)
    )
    return [
        {
            "clube_id": int(row["clube_id"]),
            "clube_nome": _club_name(clubes_cache, int(row["clube_id"])),
            "media_total": round(float(row["media_total"]), 2),
            "media_basica": round(float(row["media_basica"]), 2),
            "jogos": int(row["jogos"]),
            "ultima_rodada": int(row["ultima_rodada"]),
            "ultimo_total": round(float(row["ultimo_total"]), 2),
        }
        for _, row in form.iterrows()
    ]


def _round_or_none(value: Any, digits: int = 2) -> float | None:
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    try:
        return round(float(value), digits)
    except (TypeError, ValueError):
        return None


def _matches_from_confrontos(
    confrontos_df: pd.DataFrame,
    rodada: int,
    clubes_cache: dict | None,
) -> list[dict]:
    if confrontos_df.empty:
        return []
    required = {"clube_id", "opponent_clube_id", "is_mandante", "rodada_id"}
    if not required.issubset(confrontos_df.columns):
        return []
    normalized = normalize_numeric_columns(
        confrontos_df,
        ["clube_id", "opponent_clube_id", "rodada_id", "partida_id"],
    )
    round_matches = normalized.loc[
        (normalized["rodada_id"] == rodada) & (normalized["is_mandante"])
    ].drop_duplicates(subset=["clube_id", "opponent_clube_id"])

    matches = []
    for _, row in round_matches.iterrows():
        mandante_id = int(row["clube_id"])
        visitante_id = int(row["opponent_clube_id"])
        matches.append(
            {
                "partida_id": int(row["partida_id"])
                if "partida_id" in row and pd.notna(row["partida_id"])
                else None,
                "mandante_id": mandante_id,
                "visitante_id": visitante_id,
                "mandante_nome": _club_name(clubes_cache, mandante_id),
                "visitante_nome": _club_name(clubes_cache, visitante_id),
            }
        )
    return matches


def _team_aliases(name: str) -> list[str]:
    aliases = [name]
    aliases.extend(TEAM_ALIASES.get(name, []))
    compact = name.replace("-", " ")
    if compact != name:
        aliases.append(compact)
    seen = set()
    result = []
    for alias in aliases:
        normalized = normalize_string(alias)
        if normalized and normalized not in seen:
            seen.add(normalized)
            result.append(alias)
    return result


def _fetch_public_page_text(url: str, timeout: int = 20) -> str:
    request = Request(
        url,
        headers={
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 Chrome/126 Safari/537.36"
            ),
            "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8",
        },
    )
    try:
        with urlopen(request, timeout=timeout) as response:
            html = response.read().decode("utf-8", "ignore")
    except URLError as exc:
        if "CERTIFICATE_VERIFY_FAILED" not in str(exc):
            raise
        context = ssl._create_unverified_context()
        with urlopen(request, timeout=timeout, context=context) as response:
            html = response.read().decode("utf-8", "ignore")

    html = re.sub(r"<(script|style).*?</\1>", " ", html, flags=re.S | re.I)
    text = re.sub(r"<[^>]+>", " ", html)
    text = unescape(text)
    return re.sub(r"\s+", " ", text).strip()


def _match_snippets_from_text(
    text: str,
    mandante_nome: str,
    visitante_nome: str,
    max_snippets: int = 2,
) -> list[str]:
    normalized_text = normalize_string(text)
    home_aliases = [
        alias
        for alias in (normalize_string(alias) for alias in _team_aliases(mandante_nome))
        if alias
    ]
    away_aliases = [
        alias
        for alias in (
            normalize_string(alias) for alias in _team_aliases(visitante_nome)
        )
        if alias
    ]
    snippets = []
    for first_aliases, second_aliases in (
        (home_aliases, away_aliases),
        (away_aliases, home_aliases),
    ):
        for first_alias in first_aliases:
            for second_alias in second_aliases:
                pattern = (
                    rf"(?<![a-z0-9]){re.escape(first_alias)}(?![a-z0-9])"
                    rf"(?:\s+\d+(?:\.\d+)?){{0,2}}\s+\bvs\b\s+"
                    rf"(?:\d+(?:\.\d+)?\s+){{0,2}}"
                    rf"(?<![a-z0-9]){re.escape(second_alias)}(?![a-z0-9])"
                )
                for match in re.finditer(pattern, normalized_text):
                    start = max(0, match.start() - 80)
                    end = min(len(normalized_text), match.end() + 120)
                    snippet = normalized_text[start:end].strip()
                    if snippet not in snippets:
                        snippets.append(snippet)
                    if len(snippets) >= max_snippets:
                        return snippets
    return snippets


def search_known_odds_sources_for_matches(matches: list[dict]) -> dict:
    if not matches:
        return {
            "available": False,
            "reason": "Nenhuma partida informada.",
            "matches": [],
        }

    fetched_sources = []
    match_results = []
    for source in KNOWN_ODDS_SOURCES:
        try:
            text = _fetch_public_page_text(source["url"])
        except Exception as exc:  # pragma: no cover - network/source failure
            logger.exception("Failed to fetch odds source %s", source["url"])
            fetched_sources.append(
                {
                    **source,
                    "available": False,
                    "reason": str(exc),
                }
            )
            continue

        fetched_sources.append(
            {
                **source,
                "available": True,
                "characters": len(text),
            }
        )
        for match in matches:
            mandante_nome = str(match.get("mandante_nome") or "")
            visitante_nome = str(match.get("visitante_nome") or "")
            if not mandante_nome or not visitante_nome:
                continue
            snippets = _match_snippets_from_text(text, mandante_nome, visitante_nome)
            if snippets:
                match_results.append(
                    {
                        "source": source["name"],
                        "url": source["url"],
                        "scope": source["scope"],
                        "mandante_nome": mandante_nome,
                        "visitante_nome": visitante_nome,
                        "snippets": snippets,
                    }
                )

    return {
        "available": any(source.get("available") for source in fetched_sources),
        "sources": fetched_sources,
        "matches": match_results[:30],
        "notes": [
            "FootyStats costuma expor odds passadas e algumas odds postadas.",
            "OddsAgora ajuda a confirmar jogos futuros e pode trazer odds quando disponíveis.",
            "Use somente trechos que cruzem os mesmos clubes da lista CartolaPy.",
        ],
    }


def _confidence_label(
    score: float, status: str, total_jogos: int, risks: list[str]
) -> str:
    critical_risks = sum(
        1 for risk in risks if risk.startswith(("Status", "Amostra", "Sem pontuação"))
    )
    if (
        status == "Provável"
        and total_jogos >= 4
        and score >= 10
        and critical_risks == 0
    ):
        return "Alta"
    if status == "Provável" and total_jogos >= 2 and critical_risks <= 1:
        return "Média"
    return "Baixa"


def _report_candidate(item: dict) -> dict:
    return {
        "score": item.get("score"),
        "atleta_id": item.get("atleta_id"),
        "apelido": item.get("apelido"),
        "clube_nome": item.get("clube_nome"),
        "posicao_id": item.get("posicao_id"),
        "posicao": item.get("posicao"),
        "status": item.get("status"),
        "preco": item.get("preco"),
        "media": item.get("media"),
        "media_basica": item.get("media_basica"),
        "teto": item.get("teto"),
        "consistencia_5pts": item.get("consistencia_5pts"),
        "mando": item.get("mando"),
        "media_no_mando": item.get("media_no_mando"),
        "jogos_no_mando": item.get("jogos_no_mando"),
        "adversario_nome": item.get("adversario_nome"),
        "adversario_cede": item.get("adversario_cede"),
        "scouts_chave": item.get("overlap_scouts", [])[:3],
        "ultimas_pontuacoes": item.get("ultimas_pontuacoes"),
        "confidence": item.get("confidence"),
        "risk_flags": item.get("risk_flags"),
        "edge_summary": item.get("edge_summary"),
    }


def _candidate_window(item: dict, span: int) -> dict:
    return {
        "span": span,
        "score": item.get("score"),
        "media": item.get("media"),
        "media_basica": item.get("media_basica"),
        "teto": item.get("teto"),
        "piso": item.get("piso"),
        "consistencia_5pts": item.get("consistencia_5pts"),
        "total_jogos": item.get("total_jogos"),
        "media_no_mando": item.get("media_no_mando"),
        "jogos_no_mando": item.get("jogos_no_mando"),
        "ultimas_pontuacoes": item.get("ultimas_pontuacoes", []),
        "adversario_cede": item.get("adversario_cede"),
        "scouts_chave": item.get("overlap_scouts", [])[:3],
        "risk_flags": item.get("risk_flags", []),
    }


def _merge_candidate_windows(atleta_id: int, windows: dict[int, dict]) -> dict:
    preferred = windows.get(10) or windows.get(5) or next(iter(windows.values()))
    short = windows.get(5)
    long = windows.get(10)
    short_score = float(short.get("score") or 0) if short else None
    long_score = float(long.get("score") or 0) if long else None
    if short_score is not None and long_score is not None:
        score = long_score * 0.58 + short_score * 0.42
    else:
        score = long_score if long_score is not None else short_score or 0

    same_mando_values = [
        float(item.get("media_no_mando") or 0)
        for item in windows.values()
        if item.get("media_no_mando") is not None and item.get("jogos_no_mando", 0) > 0
    ]
    if same_mando_values:
        score += (
            min(max(max(same_mando_values) - float(preferred.get("media") or 0), -2), 3)
            * 0.22
        )

    risk_flags = []
    for item in (long, short, preferred):
        if not item:
            continue
        for risk in item.get("risk_flags", []):
            if risk not in risk_flags:
                risk_flags.append(risk)

    status = str(preferred.get("status") or "")
    total_jogos = max(int(item.get("total_jogos") or 0) for item in windows.values())
    confidence = _confidence_label(score, status, total_jogos, risk_flags)

    candidate = _report_candidate(preferred)
    candidate.update(
        {
            "score": _round_or_none(score),
            "atleta_id": atleta_id,
            "confidence": confidence,
            "risk_flags": risk_flags[:4],
            "janelas": {
                str(span): _candidate_window(item, span)
                for span, item in sorted(windows.items())
            },
            "resumo_humano": _human_candidate_summary(preferred, short, long),
        }
    )
    return candidate


def _human_candidate_summary(
    preferred: dict,
    short: dict | None,
    long: dict | None,
) -> str:
    pieces = []
    if long:
        pieces.append(
            f"{_round_or_none(long.get('media'))} pts em {long.get('total_jogos')} jogos"
        )
    if short and long:
        pieces.append(f"{_round_or_none(short.get('media'))} nos últimos 5")
    mando = preferred.get("mando")
    media_mando = preferred.get("media_no_mando")
    jogos_mando = preferred.get("jogos_no_mando")
    if mando and media_mando is not None and jogos_mando:
        pieces.append(f"{_round_or_none(media_mando)} como {mando}")
    cedida = (preferred.get("adversario_cede") or {}).get("media_cedida")
    adversario = preferred.get("adversario_nome")
    if adversario and cedida is not None:
        pieces.append(f"{adversario} cede {_round_or_none(cedida)} na posição")
    return "; ".join(str(piece) for piece in pieces if piece)


def build_position_recommendation_board_from_store(
    store: RedisDataFrameStore,
    rodada: int,
    spans: tuple[int, ...] = (5, 10),
    picks_per_position: int = PICKS_PER_POSITION,
    next_matches: list[dict] | None = None,
) -> dict:
    picks_per_position = max(1, min(picks_per_position, 12))
    by_player: dict[int, dict[int, dict]] = {}
    raw_by_span = {}
    for span in spans:
        span = validate_analysis_span(span)
        payload = build_matchup_insights_from_store(
            store=store,
            rodada=rodada,
            span=span,
            limit=180,
            next_matches=next_matches,
        )
        raw_by_span[str(span)] = {
            "rodada_min": payload.get("rodada_min"),
            "rodada_max": payload.get("rodada_max"),
            "error": payload.get("error"),
        }
        for item in payload.get("matchups", []):
            atleta_id = int(item["atleta_id"])
            by_player.setdefault(atleta_id, {})[span] = item

    candidates = [
        _merge_candidate_windows(atleta_id, windows)
        for atleta_id, windows in by_player.items()
    ]
    candidates.sort(key=lambda item: float(item.get("score") or 0), reverse=True)

    position_picks: dict[str, list[dict]] = {}
    low_confidence_watch: dict[str, list[dict]] = {}
    for posicao_id in sorted(POSITION_REPORT_LIMITS):
        position_candidates = [
            item
            for item in candidates
            if int(item.get("posicao_id") or 0) == posicao_id
        ]
        trusted_candidates = [
            item for item in position_candidates if item.get("confidence") != "Baixa"
        ]
        position_picks[str(posicao_id)] = trusted_candidates[:picks_per_position]
        low_confidence_watch[str(posicao_id)] = [
            item for item in position_candidates if item.get("confidence") == "Baixa"
        ][:3]

    captain_candidates = [
        item
        for item in candidates
        if int(item.get("posicao_id") or 0) in {4, 5}
        and item.get("confidence") in {"Alta", "Média"}
    ][:8]
    value_candidates = sorted(
        [
            item
            for item in candidates
            if item.get("preco_num") is not None
            and float(item.get("preco_num") or 0) <= 9
            and item.get("confidence") in {"Alta", "Média"}
        ],
        key=lambda item: (
            float(item.get("score") or 0) / max(float(item.get("preco_num") or 1), 1)
        ),
        reverse=True,
    )[:10]

    return {
        "rodada": rodada,
        "picks_per_position": picks_per_position,
        "spans": list(spans),
        "windows": raw_by_span,
        "position_picks": position_picks,
        "low_confidence_watch": low_confidence_watch,
        "captain_candidates": captain_candidates,
        "value_candidates": value_candidates,
        "notes": [
            "Cada posição traz até 7 opções com confiança suficiente; não force completar a lista com nomes frágeis.",
            "A pontuação combina forma recente, forma no mando do próximo jogo, adversário cedendo pontos na posição, scouts fortes e status.",
            "Nomes com confiança baixa ficam em low_confidence_watch para checagem ou alerta, não como pick principal.",
            "Use confiança e riscos para decidir exposição; esportes têm variância alta.",
        ],
    }


def _select_recommendation_summary(matchups: list[dict]) -> dict:
    primary_raw = []
    position_counts: dict[int, int] = {}
    for item in matchups:
        posicao_id = int(item.get("posicao_id") or 0)
        if item.get("confidence") == "Baixa":
            continue
        if position_counts.get(posicao_id, 0) >= PRIMARY_POSITION_CAPS.get(
            posicao_id, 2
        ):
            continue
        primary_raw.append(item)
        position_counts[posicao_id] = position_counts.get(posicao_id, 0) + 1
        if len(primary_raw) >= PRIMARY_PICK_LIMIT:
            break

    primary_picks = [_report_candidate(item) for item in primary_raw]
    by_position: dict[str, list[dict]] = {}
    for item in primary_picks:
        key = str(item.get("posicao_id"))
        by_position.setdefault(key, []).append(item)

    fallback_by_position: dict[str, list[dict]] = {}
    for posicao_id, limit in POSITION_REPORT_LIMITS.items():
        candidates = [
            item
            for item in matchups
            if int(item.get("posicao_id") or 0) == posicao_id
            and item.get("confidence") != "Baixa"
        ]
        if candidates:
            fallback_by_position[str(posicao_id)] = [
                _report_candidate(item) for item in candidates[:limit]
            ]

    captain_pool = [
        item
        for item in matchups
        if int(item.get("posicao_id") or 0) in {4, 5}
        and item.get("confidence") != "Baixa"
    ]
    captain_pool.sort(
        key=lambda item: (
            float(item.get("score") or 0)
            + float(item.get("teto") or 0) * 0.35
            + float(item.get("consistencia_5pts") or 0) * 2
        ),
        reverse=True,
    )

    prices = [
        float(item["preco_num"])
        for item in matchups
        if item.get("preco_num") is not None and pd.notna(item.get("preco_num"))
    ]
    median_price = pd.Series(prices).median() if prices else 0
    differentials = [
        item
        for item in matchups
        if float(item.get("preco_num") or 0) <= median_price
        and item.get("confidence") != "Baixa"
    ]
    differentials.sort(
        key=lambda item: (
            float(item.get("value_index") or 0),
            float(item.get("score") or 0),
        ),
        reverse=True,
    )

    risk_watch = [
        _report_candidate(item)
        for item in matchups
        if item.get("risk_flags") and item.get("score", 0) >= 8
    ][:6]

    return {
        "primary_picks": primary_picks,
        "by_position": by_position,
        "fallback_by_position": fallback_by_position,
        "captain_candidates": [_report_candidate(item) for item in captain_pool[:4]],
        "differentials": [_report_candidate(item) for item in differentials[:6]],
        "risk_watch": risk_watch,
        "readability_contract": {
            "max_player_rows": PRIMARY_PICK_LIMIT,
            "preferred_format": "tabelas curtas com 1 evidência e 1 risco por pick",
            "avoid_in_picks": "não coloque confiança Baixa na tabela principal; use em armadilhas/monitorar",
        },
    }


def build_matchup_insights_from_store(
    store: RedisDataFrameStore,
    rodada: int,
    span: int = 5,
    limit: int = 24,
    posicao_id: int | None = None,
    next_matches: list[dict] | None = None,
) -> dict:
    rodada_min, rodada_max = round_window(rodada, span)

    atletas_df = store.load_dataframe("atletas")
    pontuacoes_df = store.load_dataframe("pontuacoes")
    confrontos_df = store.load_dataframe("confrontos")
    pontos_cedidos_df = store.load_dataframe("pontos_cedidos")

    required = {
        "atletas": atletas_df,
        "pontuacoes": pontuacoes_df,
        "confrontos": confrontos_df,
        "pontos_cedidos": pontos_cedidos_df,
    }
    missing = [
        name for name, df in required.items() if not isinstance(df, pd.DataFrame)
    ]
    if missing:
        return {"error": f"Dados ausentes no Redis: {', '.join(missing)}"}

    clubes_cache = store.load_json("clubes") or {}
    posicoes_cache = store.load_json("posicoes") or {}
    status_cache = store.load_json("status") or {}
    matches = (
        next_matches
        if next_matches is not None
        else store.load_json(f"partidas:{rodada}")
    )
    matches = matches or []

    players_df = compute_atletas_unified(
        atletas_df=atletas_df,
        pontuacoes_df=pontuacoes_df,
        confrontos_df=confrontos_df,
        rodada_min=rodada_min,
        rodada_max=rodada_max,
        is_mandante="geral",
        rodada_atual=rodada - 1,
        clubes_cache=clubes_cache,
        posicoes_cache=posicoes_cache,
        status_cache=status_cache,
        proximo_jogo_cache=matches,
    )
    if posicao_id is not None:
        players_df = players_df.loc[players_df["posicao_id"] == posicao_id]
    players_df = players_df.loc[players_df["total_jogos"] > 0].copy()

    pontuacoes_with_mando = _pontuacoes_with_mando_and_status(
        pontuacoes_df=pontuacoes_df,
        atletas_df=atletas_df,
        confrontos_df=confrontos_df,
    )
    span_points = pontuacoes_with_mando.loc[
        (pontuacoes_with_mando["rodada_id"] >= rodada_min)
        & (pontuacoes_with_mando["rodada_id"] <= rodada_max)
    ].copy()
    span_points = normalize_numeric_columns(
        span_points, ["atleta_id", "clube_id", "rodada_id"]
    )

    player_form: dict[int, dict] = {}
    for atleta_id, group in span_points.sort_values("rodada_id").groupby("atleta_id"):
        scores = [
            {
                "rodada": int(item["rodada_id"]),
                "pontos": _round_or_none(item["pontuacao"]),
            }
            for item in group.tail(5).to_dict(orient="records")
        ]
        points = pd.to_numeric(group["pontuacao"], errors="coerce").dropna()
        player_form[int(atleta_id)] = {
            "teto": _round_or_none(points.max()) if not points.empty else None,
            "piso": _round_or_none(points.min()) if not points.empty else None,
            "consistencia_5pts": _round_or_none((points >= 5).mean())
            if not points.empty
            else None,
            "ultimas_pontuacoes": scores,
        }

    mando_form: dict[tuple[int, bool], dict] = {}
    if "is_mandante" in span_points.columns:
        valid_mando = span_points.dropna(subset=["is_mandante"])
        for (atleta_id, is_home), group in valid_mando.groupby(
            ["atleta_id", "is_mandante"]
        ):
            mando_points = pd.to_numeric(group["pontuacao"], errors="coerce").dropna()
            mando_form[(int(atleta_id), bool(is_home))] = {
                "media_no_mando": _round_or_none(mando_points.mean())
                if not mando_points.empty
                else None,
                "jogos_no_mando": int(mando_points.count()),
            }

    ceded_cache: dict[tuple[int, str], dict[int, dict]] = {}

    def ceded_lookup(position_id: int, mando: str) -> dict[int, dict]:
        cache_key = (position_id, mando)
        if cache_key not in ceded_cache:
            ceded_df = compute_pontos_cedidos_unified(
                pontos_cedidos_df=pontos_cedidos_df,
                rodada_min=rodada_min,
                rodada_max=rodada_max,
                is_mandante=mando,
                posicao_id=position_id,
            )
            ceded_cache[cache_key] = {
                int(row["clube_id"]): _json_safe(row.to_dict())
                for _, row in ceded_df.iterrows()
            }
        return ceded_cache[cache_key]

    insights = []
    for _, row in players_df.iterrows():
        jogo = row.get("proximo_jogo") or {}
        clube_id = int(row["clube_id"])
        mandante_id = int(jogo.get("mandante_id") or 0)
        visitante_id = int(jogo.get("visitante_id") or 0)
        if not mandante_id or not visitante_id:
            continue

        player_is_home = clube_id == mandante_id
        opponent_id = visitante_id if player_is_home else mandante_id
        mando = "mandante" if player_is_home else "visitante"
        position_id = int(row["posicao_id"])
        opponent_ceded = ceded_lookup(position_id, mando).get(opponent_id)
        if not opponent_ceded:
            continue

        player_scouts = row.get("scouts") or {}
        ceded_scouts = opponent_ceded.get("scouts") or {}
        ceded_contributions = opponent_ceded.get("scout_contributions") or {}

        overlaps = []
        for scout in Scout.as_list():
            player_total = float(player_scouts.get(scout) or 0)
            opponent_avg = float(ceded_scouts.get(scout) or 0)
            if player_total <= 0 or opponent_avg <= 0 or Scout.get_value(scout) <= 0:
                continue
            contribution = ceded_contributions.get(scout, {})
            points_contribution = float(
                contribution.get(
                    "points_contribution", opponent_avg * Scout.get_value(scout)
                )
                or 0
            )
            overlaps.append(
                {
                    "scout": scout,
                    "scout_nome": _scout_label(scout),
                    "player_total": round(player_total, 2),
                    "opponent_avg": round(opponent_avg, 2),
                    "points_contribution": round(points_contribution, 2),
                    "percentage": contribution.get("percentage"),
                }
            )
        overlaps.sort(key=lambda item: item["points_contribution"], reverse=True)

        atleta_id = int(row["atleta_id"])
        form = player_form.get(atleta_id, {})
        same_mando = mando_form.get((atleta_id, player_is_home), {})
        media = float(row.get("media") or 0)
        media_basica = float(row.get("media_basica") or 0)
        media_no_mando = same_mando.get("media_no_mando")
        jogos_no_mando = int(same_mando.get("jogos_no_mando") or 0)
        teto = float(form.get("teto") or media)
        consistencia = float(form.get("consistencia_5pts") or 0)
        preco_num = float(row.get("preco_num") or 0)

        risk_flags = []
        status_nome = str(row.get("status_nome") or "")
        if status_nome and status_nome != "Provável":
            risk_flags.append(f"Status {status_nome}")
        if int(row.get("total_jogos") or 0) < max(2, min(4, span // 2)):
            risk_flags.append("Amostra curta")
        if not overlaps:
            risk_flags.append("Sem scout sobreposto forte")
        if jogos_no_mando == 0:
            risk_flags.append(f"Sem amostra como {mando}")
        elif media_no_mando is not None and media_no_mando + 2 < media:
            risk_flags.append(f"Queda recente como {mando}")
        if media > 0 and media_basica / media < 0.45:
            risk_flags.append("Mais dependente de scouts de teto")

        status_boost = (
            1.5 if status_nome == "Provável" else -1.0 if status_nome else 0.0
        )
        overlap_score = sum(item["points_contribution"] for item in overlaps[:3])
        mando_delta = (
            float(media_no_mando) - media if media_no_mando is not None else -0.4
        )
        sample_penalty = -1.0 if "Amostra curta" in risk_flags else 0.0
        matchup_score = (
            media * 0.56
            + media_basica * 0.18
            + max(float(opponent_ceded.get("media_cedida") or 0), 0) * 0.3
            + overlap_score * 0.18
            + teto * 0.08
            + consistencia * 1.8
            + max(min(mando_delta, 2.5), -2.5) * 0.2
            + status_boost
            + sample_penalty
        )
        value_index = matchup_score / max(preco_num, 1)
        confidence = _confidence_label(
            matchup_score, status_nome, int(row.get("total_jogos") or 0), risk_flags
        )
        top_scout = overlaps[0] if overlaps else None
        scout_summary = (
            f"{top_scout['scout']} ({top_scout['points_contribution']} pts cedidos)"
            if top_scout
            else "sem scout-chave claro"
        )
        edge_summary = (
            f"{media:.2f} pts em {int(row.get('total_jogos') or 0)} jogos; "
            f"{_club_name(clubes_cache, opponent_id)} cede "
            f"{_round_or_none(opponent_ceded.get('media_cedida'))} para "
            f"{_position_label(posicoes_cache, position_id)}; scout-chave {scout_summary}."
        )

        insights.append(
            {
                "score": round(matchup_score, 2),
                "atleta_id": atleta_id,
                "apelido": str(row.get("apelido") or ""),
                "clube_id": clube_id,
                "clube_nome": _club_name(clubes_cache, clube_id),
                "posicao_id": position_id,
                "posicao": _position_label(posicoes_cache, position_id),
                "status": status_nome,
                "preco": row.get("preco"),
                "preco_num": _round_or_none(preco_num),
                "media": _round_or_none(media),
                "media_basica": _round_or_none(media_basica),
                "total_jogos": int(row.get("total_jogos") or 0),
                "teto": _round_or_none(teto),
                "piso": form.get("piso"),
                "consistencia_5pts": _round_or_none(consistencia),
                "media_no_mando": media_no_mando,
                "jogos_no_mando": jogos_no_mando,
                "ultimas_pontuacoes": form.get("ultimas_pontuacoes", []),
                "value_index": _round_or_none(value_index),
                "confidence": confidence,
                "risk_flags": risk_flags[:4],
                "edge_summary": edge_summary,
                "mando": mando,
                "adversario_id": opponent_id,
                "adversario_nome": _club_name(clubes_cache, opponent_id),
                "partida": {
                    "mandante_id": mandante_id,
                    "mandante_nome": _club_name(clubes_cache, mandante_id),
                    "visitante_id": visitante_id,
                    "visitante_nome": _club_name(clubes_cache, visitante_id),
                },
                "adversario_cede": {
                    "media_cedida": opponent_ceded.get("media_cedida"),
                    "media_cedida_basica": opponent_ceded.get("media_cedida_basica"),
                    "total_jogos": opponent_ceded.get("total_jogos"),
                },
                "overlap_scouts": overlaps[:5],
            }
        )

    insights.sort(key=lambda item: item["score"], reverse=True)
    position_form = {}
    for pos_id in sorted(players_df["posicao_id"].dropna().astype(int).unique()):
        scored = compute_pontos_conquistados_unified(
            pontuacoes_df=pontuacoes_with_mando,
            rodada_min=rodada_min,
            rodada_max=rodada_max,
            is_mandante="geral",
            posicao_id=int(pos_id),
        )
        position_form[str(int(pos_id))] = [
            {
                "clube_id": int(item["clube_id"]),
                "clube_nome": _club_name(clubes_cache, int(item["clube_id"])),
                "media_conquistada": item.get("media_conquistada"),
                "media_conquistada_basica": item.get("media_conquistada_basica"),
                "total_jogos": item.get("total_jogos"),
            }
            for item in scored.head(10).to_dict(orient="records")
        ]

    return {
        "rodada": rodada,
        "span": span,
        "recommended_spans": RECOMMENDED_SPANS,
        "rodada_min": rodada_min,
        "rodada_max": rodada_max,
        "matchups": insights[:limit],
        "recommendation_board": _select_recommendation_summary(insights),
        "team_form": _team_form(pontuacoes_df, clubes_cache, rodada_min, rodada_max),
        "position_team_form": position_form,
        "matches": matches,
        "notes": [
            "Score combina média recente do atleta, média cedida pelo adversário na posição, scouts sobrepostos, mando e status.",
            "Para relatório final, prefira position_picks do quadro por posição.",
            "Forma de time é produção fantasy agregada, não sequência real de resultados.",
        ],
    }


def evaluate_matchup_strategy_from_store(
    store: RedisDataFrameStore,
    span: int = 5,
    lookback_rounds: int = 6,
    limit_per_round: int = PICKS_PER_POSITION * len(POSITION_REPORT_LIMITS),
) -> dict:
    span = validate_analysis_span(span)
    lookback_rounds = max(1, min(lookback_rounds, 12))
    limit_per_round = max(6, min(limit_per_round, 80))

    pontuacoes_df = store.load_dataframe("pontuacoes")
    confrontos_df = store.load_dataframe("confrontos")
    if not isinstance(pontuacoes_df, pd.DataFrame) or not isinstance(
        confrontos_df, pd.DataFrame
    ):
        return {"available": False, "reason": "Dados históricos insuficientes."}

    clubes_cache = store.load_json("clubes") or {}
    pontuacoes_df = normalize_numeric_columns(
        pontuacoes_df, ["atleta_id", "clube_id", "posicao_id", "rodada_id"]
    )
    confrontos_df = normalize_numeric_columns(
        confrontos_df, ["clube_id", "opponent_clube_id", "rodada_id", "partida_id"]
    )

    completed_rounds = sorted(
        set(pontuacoes_df["rodada_id"].dropna().astype(int))
        & set(confrontos_df["rodada_id"].dropna().astype(int))
    )
    target_rounds = [
        rodada
        for rodada in completed_rounds
        if rodada > min(completed_rounds) and round_window(rodada, span)[1] < rodada
    ][-lookback_rounds:]
    if not target_rounds:
        return {"available": False, "reason": "Sem rodadas completas para avaliar."}

    all_evaluated = []
    rounds = []
    for rodada in target_rounds:
        matches = _matches_from_confrontos(confrontos_df, rodada, clubes_cache)
        board = build_position_recommendation_board_from_store(
            store=store,
            rodada=rodada,
            spans=(span,),
            picks_per_position=min(7, max(1, limit_per_round // 6)),
            next_matches=matches,
        )
        if all(window.get("error") for window in board.get("windows", {}).values()):
            rounds.append({"rodada": rodada, "error": board["windows"]})
            continue

        position_picks = board.get("position_picks") or {}
        selected = [item for picks in position_picks.values() for item in picks]
        seen = set()
        selected = [
            item
            for item in selected
            if not (item["atleta_id"] in seen or seen.add(item["atleta_id"]))
        ]

        actual = pontuacoes_df.loc[pontuacoes_df["rodada_id"] == rodada].copy()
        if actual.empty:
            rounds.append({"rodada": rodada, "picks": 0, "reason": "Sem pontuações."})
            continue
        actual_by_player = (
            actual.sort_values("atleta_id")
            .drop_duplicates(subset=["atleta_id"], keep="last")
            .set_index("atleta_id")
        )
        club_wins = actual.groupby("clube_id")["V"].max().fillna(0).to_dict()
        position_thresholds = {}
        for posicao_id, group in actual.groupby("posicao_id"):
            scores = pd.to_numeric(group["pontuacao"], errors="coerce").dropna()
            if scores.empty:
                continue
            posicao_id = int(posicao_id)
            position_thresholds[posicao_id] = float(scores.quantile(0.75))

        evaluated = []
        for item in selected:
            atleta_id = int(item["atleta_id"])
            posicao_id = int(item.get("posicao_id") or 0)
            threshold = position_thresholds.get(posicao_id)
            actual_row = (
                actual_by_player.loc[atleta_id]
                if atleta_id in actual_by_player.index
                else None
            )
            played = actual_row is not None
            actual_points = _round_or_none(actual_row["pontuacao"]) if played else None
            top_quartile = bool(
                played
                and actual_points is not None
                and threshold is not None
                and float(actual_points) >= threshold
            )
            team_won = bool(
                club_wins.get(int(actual_row["clube_id"]), 0) > 0 if played else False
            )
            evaluated_item = {
                "rodada": rodada,
                "atleta_id": atleta_id,
                "apelido": item.get("apelido"),
                "posicao_id": posicao_id,
                "posicao": item.get("posicao"),
                "confidence": item.get("confidence"),
                "predicted_score": item.get("score"),
                "actual_points": actual_points,
                "top25_threshold": _round_or_none(threshold)
                if threshold is not None
                else None,
                "played": played,
                "hit": top_quartile,
                "top_quartile": top_quartile,
                "team_won": team_won,
            }
            evaluated.append(evaluated_item)
            all_evaluated.append(evaluated_item)

        played_items = [item for item in evaluated if item["played"]]
        hits = [item for item in played_items if item["top_quartile"]]
        rounds.append(
            {
                "rodada": rodada,
                "picks": len(evaluated),
                "played": len(played_items),
                "dnp": len(evaluated) - len(played_items),
                "avg_actual_points": _round_or_none(
                    pd.Series([item["actual_points"] for item in played_items]).mean()
                )
                if played_items
                else None,
                "hit_rate": _round_or_none(len(hits) / len(played_items))
                if played_items
                else None,
                "top_quartile_rate": _round_or_none(len(hits) / len(played_items))
                if played_items
                else None,
                "team_win_rate": _round_or_none(
                    sum(item["team_won"] for item in played_items) / len(played_items)
                )
                if played_items
                else None,
                "top_hits": sorted(
                    hits,
                    key=lambda item: item["actual_points"] or 0,
                    reverse=True,
                )[:4],
                "misses": sorted(
                    [
                        item
                        for item in played_items
                        if not item["hit"] and item["actual_points"] is not None
                    ],
                    key=lambda item: item["actual_points"] or 0,
                )[:4],
            }
        )

    played_all = [item for item in all_evaluated if item["played"]]
    hits_all = [item for item in played_all if item["top_quartile"]]
    by_position = {}
    for posicao_id in sorted({int(item["posicao_id"]) for item in all_evaluated}):
        pos_items = [item for item in all_evaluated if item["posicao_id"] == posicao_id]
        pos_played = [item for item in pos_items if item["played"]]
        pos_hits = [item for item in pos_played if item["top_quartile"]]
        by_position[str(posicao_id)] = {
            "picks": len(pos_items),
            "played": len(pos_played),
            "avg_actual_points": _round_or_none(
                pd.Series([item["actual_points"] for item in pos_played]).mean()
            )
            if pos_played
            else None,
            "hit_rate": _round_or_none(len(pos_hits) / len(pos_played))
            if pos_played
            else None,
            "top_quartile_rate": _round_or_none(len(pos_hits) / len(pos_played))
            if pos_played
            else None,
        }

    dnp_rate = (
        _round_or_none((len(all_evaluated) - len(played_all)) / len(all_evaluated))
        if all_evaluated
        else None
    )
    recommendations = []
    if dnp_rate is not None and dnp_rate > 0.2:
        recommendations.append("Reduzir confiança quando status/amostra não sustentam.")
    if played_all and len(hits_all) / len(played_all) < 0.35:
        recommendations.append(
            "Aumentar peso de mando recente e reduzir nomes com amostra curta."
        )
    recommendations.append(
        "Priorizar jogadores com confiança Alta/Média, scout sobreposto e média forte no mando do próximo jogo."
    )

    return {
        "available": True,
        "span": span,
        "lookback_rounds": lookback_rounds,
        "rounds_evaluated": len(rounds),
        "target_rounds": target_rounds,
        "overall": {
            "picks": len(all_evaluated),
            "played": len(played_all),
            "dnp_rate": dnp_rate,
            "avg_actual_points": _round_or_none(
                pd.Series([item["actual_points"] for item in played_all]).mean()
            )
            if played_all
            else None,
            "hit_rate": _round_or_none(len(hits_all) / len(played_all))
            if played_all
            else None,
            "top_quartile_rate": _round_or_none(len(hits_all) / len(played_all))
            if played_all
            else None,
            "team_win_rate": _round_or_none(
                sum(item["team_won"] for item in played_all) / len(played_all)
            )
            if played_all
            else None,
        },
        "by_position": by_position,
        "rounds": rounds,
        "recommendations": recommendations,
        "caveats": [
            "Avaliação usa apenas pontuações e confrontos antes da rodada-alvo para forma/matchup.",
            "Acerto significa terminar no top 25% de pontuação da própria posição naquela rodada.",
            "Resultado de vitória é inferido pelo scout V quando placares oficiais não estão arquivados.",
            "Status e preço históricos dependem do snapshot disponível no Redis.",
        ],
    }


def _compact_table(payload: dict, limit: int) -> dict:
    data = payload.get("data", []) if isinstance(payload, dict) else []
    return {
        "total": payload.get("total") if isinstance(payload, dict) else len(data),
        "page": payload.get("page", 1) if isinstance(payload, dict) else 1,
        "page_size": payload.get("page_size", limit)
        if isinstance(payload, dict)
        else limit,
        "data": data[:limit],
    }


def _extract_report_text(result: Any) -> str:
    if isinstance(result, str):
        return result.strip()
    if isinstance(result, dict):
        messages = result.get("messages") or []
    else:
        messages = getattr(result, "messages", [])
    if not messages:
        return ""
    last = messages[-1]
    content = getattr(last, "content", None)
    if content is None and isinstance(last, dict):
        content = last.get("content")
    if isinstance(content, str):
        return content.strip()
    if isinstance(content, list):
        parts = []
        for item in content:
            if isinstance(item, str):
                parts.append(item)
            elif isinstance(item, dict):
                parts.append(str(item.get("text") or item.get("content") or ""))
        return "".join(parts).strip()
    text = getattr(last, "text", None)
    if isinstance(text, str):
        return text.strip()
    if callable(text):
        try:
            return str(text()).strip()
        except TypeError:
            return ""
    return ""


def _system_prompt() -> str:
    return """
Você é o analista de Cartola FC do CartolaPy. Gere um relatório em português do Brasil,
com recomendações úteis para montar o time da próxima rodada. O objetivo é consistência:
explicar boas probabilidades, não prometer acertos. Use somente evidências obtidas pelas
ferramentas. Não invente odds, status, adversários, scouts ou médias.

Regras:
- Sempre consulte os dados do CartolaPy antes de recomendar jogadores.
- Consulte get_previous_round_memories no início. Use os aprendizados de rodadas anteriores
  para calibrar sinais e riscos, mas nunca deixe a memória histórica substituir os dados
  atuais nem generalize um caso isolado. Dê preferência a memórias que identifiquem
  claramente clube, adversário, mando e scouts daquele confronto.
- Use build_position_recommendation_board como fonte principal: ele já combina últimos
  5 e 10 jogos, desempenho no mando do próximo jogo, adversário, scouts e status.
- Chame evaluate_historical_strategy para span 5 e span 10. A régua de acerto é ficar
  entre os 25% melhores da própria posição na rodada seguinte. Use isso para calibrar
  confiança por posição, não para fazer promessa.
- Chame search_brazil_odds_sources para cruzar FootyStats/OddsAgora com a lista de
  jogos do CartolaPy. Use a busca complementar de mercado no máximo uma vez, e só se
  precisar de odds numéricas mais legíveis.
- Depois de consultar status, jogos, quadro por posição, avaliação histórica e odds,
  escreva o relatório final. Não faça rodadas extras de consulta por posição a menos
  que uma informação essencial esteja ausente.
- O usuário quer opções por posição. Traga até 7 nomes por posição quando houver dados
  e confiança suficientes; não complete tabela com pick frágil só para bater número.
- Evite linguagem técnica no relatório final. Não use nomes internos de ferramentas,
  métricas ou estruturas de dados. Fale como analista humano: "minha conferência
  histórica", "melhores da posição", "bom caminho", "risco".
- Cada jogador precisa ter motivo prático: forma recente, mando/casa-fora, adversário
  cedendo pontos na posição, scouts principais, confiança e risco.
- Nunca cite um scout histórico sem deixar claro por qual clube o jogador atuou e contra
  qual adversário ele produziu aquele scout. Trate o confronto específico como contexto,
  não como garantia de repetição.
- Prefira tabelas por posição. Não escreva blocos longos explicando metodologia.
- Cite janelas analisadas, riscos e checagens finais antes de escalar.
- Não exponha raciocínio interno. Entregue apenas o relatório final em Markdown.

O relatório deve ter exatamente estas seções:
# Resumo da rodada
# Jogos-alvo e expectativa de vitória
# Picks por posição
# Diferenciais
# Capitão e teto
# Armadilhas / evitar
# Riscos e checagens antes de escalar
# Fontes e janela analisada
""".strip()


def _user_prompt(rodada: int) -> str:
    last_5_min, last_5_max = round_window(rodada, 5)
    last_10_min, last_10_max = round_window(rodada, 10)
    return f"""
Gere o relatório Dicas da Rodada para a rodada {rodada}.
Escolha as janelas de análise conforme a pergunta que estiver investigando.
Janelas recomendadas para começar:
- últimos 5 jogos/rodadas disponíveis: rodadas {last_5_min} a {last_5_max}
- últimos 10 jogos/rodadas disponíveis: rodadas {last_10_min} a {last_10_max}

Critérios mínimos:
- Comece consultando get_previous_round_memories para recuperar aprendizados já consolidados.
- Comece por build_position_recommendation_board.
- Consulte evaluate_historical_strategy com span 5 e span 10 para entender quais
  posições tiveram melhor retorno na conferência histórica.
- Consulte search_brazil_odds_sources e cruze com os jogos retornados por get_next_matches.
- Use busca complementar de mercado no máximo uma vez, com uma consulta agregada da rodada,
  somente se a fonte principal não trouxer odds legíveis suficientes.
- Traga picks por posição, até 7 por posição, em tabelas separadas. Colunas:
  jogador, clube x adversário/mando, últimos 5/10, casa/fora, por que gosto, confiança/risco.
- Se uma posição tiver poucos nomes confiáveis, diga isso em uma frase curta e não force filler.
- Compare forma recente dos times por produção fantasy apenas quando ela mudar a decisão.
- Quando fizer sentido, compare janelas diferentes (por exemplo últimos 5 vs últimos 10)
  para separar forma quente de tendência mais estável.
- Priorize jogadores prováveis quando esse status estiver disponível.
- Use odds/expectativa de vitória apenas quando as fontes trouxerem evidência legível.
- Inclua alertas de status, pequena amostra, adversário difícil e dependência de scout.
- Use tom prático: "prefiro", "bom caminho", "risco", "evitar por enquanto".
""".strip()


def _make_agent_tools(
    store: RedisDataFrameStore,
    api_client: CartolaPyApiClient,
    sink: DicasEventSink,
    rodada: int,
) -> list:
    def get_previous_round_memories(limit: int = 5) -> dict:
        """Carrega da AWS memórias resumidas de rodadas anteriores à rodada analisada."""
        sink.tool_call(
            "memórias de rodadas anteriores",
            {"rodada": rodada, "limit": limit},
        )
        try:
            result = load_memories_for_prediction(
                rodada=rodada,
                season_year=current_season_year(),
                limit=min(max(limit, 1), 10),
            )
        except DicasMemoryError as exc:
            result = {
                "available": False,
                "target_round": rodada,
                "rounds": [],
                "memories": [],
                "reason": str(exc),
            }
        sink.tool_result(
            "memórias de rodadas anteriores",
            {
                "available": result.get("available", False),
                "rounds": result.get("rounds", []),
            },
        )
        return result

    def get_cartolapy_status() -> dict:
        """Retorna status das tabelas e rodada atual do CartolaPy."""
        return api_client.get("/api/tables/status")

    def get_next_matches() -> list[dict]:
        """Retorna as partidas da rodada analisada."""
        matches = api_client.get(f"/api/partidas/{rodada}")
        if isinstance(matches, list):
            store.save_json(f"partidas:{rodada}", matches)
        return matches

    def get_top_players(
        posicao_id: int | None = None,
        scout: str | None = None,
        span: int = 5,
        limit: int = 30,
    ) -> dict:
        """Lista atletas por média ou scout. Span recomendado: 5 ou 10; aceita 1 a 10."""
        rodada_min, rodada_max = round_window(rodada, span)
        params = {
            "page": 1,
            "page_size": min(max(limit, 1), 100),
            "rodada_min": rodada_min,
            "rodada_max": rodada_max,
            "sort_by": scout or "media",
            "sort_direction": "desc",
            "posicao_ids": str(posicao_id) if posicao_id else None,
            "scout": scout,
        }
        return _compact_table(
            api_client.get("/api/tables/atletas-unified", params), limit
        )

    def get_points_conceded(
        posicao_id: int,
        is_mandante: Literal["geral", "mandante", "visitante"] = "geral",
        scout: str | None = None,
        span: int = 5,
        limit: int = 20,
    ) -> dict:
        """Lista times que mais cedem pontos para uma posição. Span recomendado: 5 ou 10."""
        rodada_min, rodada_max = round_window(rodada, span)
        params = {
            "page": 1,
            "page_size": min(max(limit, 1), 100),
            "rodada_min": rodada_min,
            "rodada_max": rodada_max,
            "sort_by": scout or "media_cedida",
            "sort_direction": "desc",
            "is_mandante": is_mandante,
            "posicao_id": posicao_id,
            "scout": scout,
        }
        return _compact_table(
            api_client.get("/api/tables/pontos-cedidos-unified", params), limit
        )

    def get_points_scored(
        posicao_id: int,
        is_mandante: Literal["geral", "mandante", "visitante"] = "geral",
        scout: str | None = None,
        span: int = 5,
        limit: int = 20,
    ) -> dict:
        """Lista times com melhor produção fantasy por posição. Span recomendado: 5 ou 10."""
        rodada_min, rodada_max = round_window(rodada, span)
        params = {
            "page": 1,
            "page_size": min(max(limit, 1), 100),
            "rodada_min": rodada_min,
            "rodada_max": rodada_max,
            "sort_by": scout or "media_conquistada",
            "sort_direction": "desc",
            "is_mandante": is_mandante,
            "posicao_id": posicao_id,
            "scout": scout,
        }
        return _compact_table(
            api_client.get("/api/tables/pontos-conquistados-unified", params), limit
        )

    def get_player_history(atleta_id: int, span: int = 5) -> dict:
        """Retorna histórico rodada a rodada de um atleta. Span recomendado: 5 ou 10."""
        rodada_min, rodada_max = round_window(rodada, span)
        return api_client.get(
            f"/api/tables/atletas/{atleta_id}/historico",
            {"rodada_min": rodada_min, "rodada_max": rodada_max},
        )

    def build_curated_matchup_insights(
        limit: int = 24,
        posicao_id: int | None = None,
        span: int = 5,
    ) -> dict:
        """Cruza atletas, jogos, pontos cedidos, mando e scouts. Span recomendado: 5 ou 10."""
        span = validate_analysis_span(span)
        sink.tool_call(
            "insights consolidados",
            {"limit": limit, "posicao_id": posicao_id, "span": span},
        )
        matches = store.load_json(f"partidas:{rodada}") or []
        if not matches:
            try:
                matches = get_next_matches()
            except RuntimeError:
                matches = []
        insights = build_matchup_insights_from_store(
            store=store,
            rodada=rodada,
            span=span,
            limit=min(max(limit, 1), 50),
            posicao_id=posicao_id,
            next_matches=matches if isinstance(matches, list) else [],
        )
        sink.tool_result(
            "insights consolidados",
            {
                "items": len(insights.get("matchups", []))
                if isinstance(insights, dict)
                else 0
            },
        )
        return insights

    def build_position_recommendation_board(
        picks_per_position: int = PICKS_PER_POSITION,
    ) -> dict:
        """Monta até 7 opções por posição combinando últimos 5/10 jogos e mando do próximo jogo."""
        sink.tool_call(
            "quadro por posição",
            {"picks_per_position": picks_per_position, "spans": RECOMMENDED_SPANS},
        )
        matches = store.load_json(f"partidas:{rodada}") or []
        if not matches:
            try:
                matches = get_next_matches()
            except RuntimeError:
                matches = []
        board = build_position_recommendation_board_from_store(
            store=store,
            rodada=rodada,
            spans=tuple(RECOMMENDED_SPANS),
            picks_per_position=picks_per_position,
            next_matches=matches if isinstance(matches, list) else [],
        )
        sink.tool_result(
            "quadro por posição",
            {
                "positions": len(board.get("position_picks", {})),
                "picks_per_position": board.get("picks_per_position"),
            },
        )
        return board

    def evaluate_historical_strategy(
        span: int = 5,
        lookback_rounds: int = 6,
    ) -> dict:
        """Confere, por posição, se recomendações antigas ficaram entre os melhores 25% da posição."""
        span = validate_analysis_span(span)
        sink.tool_call(
            "avaliação histórica",
            {"span": span, "lookback_rounds": lookback_rounds},
        )
        evaluation = evaluate_matchup_strategy_from_store(
            store=store,
            span=span,
            lookback_rounds=lookback_rounds,
            limit_per_round=PICKS_PER_POSITION * len(POSITION_REPORT_LIMITS),
        )
        sink.tool_result(
            "avaliação histórica",
            {
                "available": evaluation.get("available", False),
                "rounds": evaluation.get("rounds_evaluated", 0),
            },
        )
        return evaluation

    def search_brazil_odds_sources() -> dict:
        """Consulta FootyStats e OddsAgora cruzando as partidas da rodada do CartolaPy."""
        sink.tool_call("fontes de odds Brasil", {"rodada": rodada})
        matches = store.load_json(f"partidas:{rodada}") or []
        if not matches:
            try:
                matches = get_next_matches()
            except RuntimeError:
                matches = []
        result = search_known_odds_sources_for_matches(
            matches if isinstance(matches, list) else []
        )
        sink.tool_result(
            "fontes de odds Brasil",
            {
                "available": result.get("available", False),
                "matches": len(result.get("matches", [])),
            },
        )
        return result

    def search_odds_for_match(query: str, max_results: int = 5) -> dict:
        """Busca odds e expectativa de vitória na web via Tavily quando configurado."""
        tavily_key = os.environ.get("TAVILY_API_KEY")
        if not tavily_key:
            return {
                "available": False,
                "reason": "TAVILY_API_KEY não configurada; odds externas não foram consultadas.",
                "results": [],
            }
        sink.tool_call("Tavily odds", {"query": query})
        try:
            from tavily import TavilyClient

            client = TavilyClient(api_key=tavily_key)
            payload = client.search(
                query=query,
                max_results=min(max(max_results, 1), 8),
                topic="general",
                include_raw_content=False,
            )
        except Exception as exc:  # pragma: no cover - network/provider failure
            logger.exception("Tavily search failed")
            return {"available": False, "reason": str(exc), "results": []}
        results = [
            {
                "title": item.get("title"),
                "url": item.get("url"),
                "content": item.get("content"),
                "score": item.get("score"),
            }
            for item in payload.get("results", [])
        ]
        sink.tool_result("Tavily odds", {"results": len(results)})
        return {"available": True, "results": results}

    return [
        get_previous_round_memories,
        get_cartolapy_status,
        get_next_matches,
        get_top_players,
        get_points_conceded,
        get_points_scored,
        get_player_history,
        build_curated_matchup_insights,
        build_position_recommendation_board,
        evaluate_historical_strategy,
        search_brazil_odds_sources,
        search_odds_for_match,
    ]


def _build_agent_model(model: str, reasoning_effort: str):
    if model.startswith("openai:"):
        from langchain_openai import ChatOpenAI

        return ChatOpenAI(
            model=model.split(":", 1)[1],
            reasoning_effort=reasoning_effort,
            use_responses_api=True,
        )
    return model


async def generate_dicas_report(
    store: RedisDataFrameStore,
    run_id: str,
    rodada: int,
) -> dict:
    openai_key = os.environ.get("OPENAI_API_KEY")
    if not openai_key:
        raise DicasConfigurationError(
            "OPENAI_API_KEY is required to generate Dicas da Rodada."
        )

    cache = DicasReportCache(store)
    sink = DicasEventSink(cache, run_id)
    model = os.environ.get("DICAS_MODEL", DEFAULT_DICAS_MODEL)
    reasoning_effort = os.environ.get(
        "DICAS_REASONING_EFFORT", DEFAULT_DICAS_REASONING_EFFORT
    )
    api_base_url = os.environ.get("CARTOLAPY_API_BASE_URL", "http://localhost:8000")
    api_client = CartolaPyApiClient(api_base_url, sink)

    sink.progress("Preparando ferramentas e contexto do agente.")
    try:
        from deepagents import create_deep_agent
    except ModuleNotFoundError as exc:  # pragma: no cover - dependency guard
        raise DicasConfigurationError(
            "The deepagents package is required to generate Dicas da Rodada."
        ) from exc

    tools = _make_agent_tools(store, api_client, sink, rodada)
    agent = create_deep_agent(
        model=_build_agent_model(model, reasoning_effort),
        tools=tools,
        system_prompt=_system_prompt(),
    )

    sink.progress("Agente iniciado; coletando dados e gerando recomendações.")
    result = await agent.ainvoke(
        {"messages": [{"role": "user", "content": _user_prompt(rodada)}]},
        config={"configurable": {"thread_id": f"dicas-{rodada}-{run_id}"}},
    )
    report_text = _extract_report_text(result)
    if not report_text:
        raise RuntimeError("Deep Agents returned an empty report.")
    sink.report_delta(report_text)

    sources = [
        {"name": "CartolaPy API", "type": "internal", "base_url": api_base_url},
        {
            "name": "Tavily odds/search",
            "type": "web",
            "available": bool(os.environ.get("TAVILY_API_KEY")),
        },
    ]
    return {
        "season_year": current_season_year(),
        "rodada": rodada,
        "report_markdown": report_text,
        "generated_at": utc_now().isoformat(),
        "model": model,
        "reasoning_effort": reasoning_effort,
        "recommended_spans": RECOMMENDED_SPANS,
        "sources": sources,
    }


async def run_dicas_report_generation(
    store: RedisDataFrameStore,
    run_id: str,
    rodada: int,
) -> dict:
    cache = DicasReportCache(store)
    metadata = cache.update_run(run_id, "running")
    cache.append_event(
        run_id,
        "status",
        "Geração iniciada no worker.",
        {"status": "running", "rodada": rodada},
    )
    try:
        report = await generate_dicas_report(store, run_id, rodada)
    except Exception as exc:
        logger.exception("Failed to generate Dicas da Rodada")
        error = str(exc)
        cache.fail_run(run_id, error)
        return {**metadata, "status": "failed", "error": error}
    completed = cache.complete_run(run_id, report)
    return completed
