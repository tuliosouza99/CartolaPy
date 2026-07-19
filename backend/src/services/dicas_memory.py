from __future__ import annotations

import json
import logging
import os
import re
import uuid
from datetime import datetime, timezone
from functools import lru_cache
from typing import Any

import pandas as pd
from pydantic import BaseModel, Field

from .enums import Scout
from .redis_store import RedisDataFrameStore

logger = logging.getLogger(__name__)

REPORT_ID_PATTERN = re.compile(r"^[A-Za-z0-9_.-]+$")
DEFAULT_DICAS_MODEL = "openai:gpt-5.5"
DEFAULT_DICAS_REASONING_EFFORT = "medium"
DEFAULT_MEMORY_LOOKBACK_ROUNDS = 38
DEFAULT_MEMORY_CONTEXT_ROUNDS = 5
MEMORY_SCHEMA_VERSION = 2


class DicasMemoryError(RuntimeError):
    """Base error for durable Dicas da Rodada storage."""


class DicasMemoryConfigurationError(DicasMemoryError):
    """Raised when the S3 memory store is not configured."""


class DicasMemoryStorageError(DicasMemoryError):
    """Raised when an S3 operation fails."""


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def current_season_year() -> int:
    configured = os.environ.get("DICAS_SEASON_YEAR")
    if configured:
        try:
            return int(configured)
        except ValueError as exc:
            raise DicasMemoryConfigurationError(
                "DICAS_SEASON_YEAR must be a four-digit year."
            ) from exc
    return utc_now().year


def _season_year_from_payload(payload: dict, fallback: int | None = None) -> int:
    value = payload.get("season_year")
    if value is not None:
        try:
            return int(value)
        except (TypeError, ValueError):
            pass
    generated_at = str(payload.get("generated_at") or payload.get("captured_at") or "")
    match = re.match(r"^(\d{4})-", generated_at)
    if match:
        return int(match.group(1))
    return fallback or current_season_year()


def _json_safe(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
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
        except (TypeError, ValueError):
            return value
    return value


def _report_id(report: dict) -> str:
    existing = str(report.get("report_id") or "")
    if existing and REPORT_ID_PATTERN.fullmatch(existing):
        return existing
    rodada = str(report.get("rodada") or "unknown")
    generated_at = str(report.get("generated_at") or utc_now().isoformat())
    timestamp = re.sub(r"[^0-9A-Za-z]+", "-", generated_at).strip("-")
    return f"rodada-{rodada}-{timestamp}-{uuid.uuid4().hex[:8]}"


def _report_title(report: dict) -> str:
    markdown = str(report.get("report_markdown") or "")
    for line in markdown.splitlines():
        stripped = line.strip()
        if stripped.startswith("#"):
            return stripped.lstrip("#").strip() or "Dicas da Rodada"
    rodada = report.get("rodada")
    return f"Dicas da Rodada {rodada}" if rodada else "Dicas da Rodada"


def _report_round(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


class S3DicasMemoryStore:
    """S3-backed reports, immutable round sources, and derived round memories."""

    def __init__(
        self,
        bucket: str,
        prefix: str = "cartolapy/dicas-da-rodada",
        *,
        region_name: str | None = None,
        client: Any | None = None,
    ):
        if not bucket.strip():
            raise DicasMemoryConfigurationError("DICAS_S3_BUCKET must be configured.")
        self.bucket = bucket
        self.prefix = prefix.strip("/")
        if client is None:
            try:
                import boto3
            except ModuleNotFoundError as exc:  # pragma: no cover - dependency guard
                raise DicasMemoryConfigurationError(
                    "boto3 is required for Dicas da Rodada S3 storage."
                ) from exc
            client = boto3.client("s3", region_name=region_name)
        self.client = client

    @classmethod
    def from_env(cls) -> S3DicasMemoryStore:
        return cls(
            bucket=os.environ.get("DICAS_S3_BUCKET", ""),
            prefix=os.environ.get("DICAS_S3_PREFIX", "cartolapy/dicas-da-rodada"),
            region_name=os.environ.get("AWS_REGION")
            or os.environ.get("AWS_DEFAULT_REGION"),
        )

    def _key(self, suffix: str) -> str:
        suffix = suffix.lstrip("/")
        return f"{self.prefix}/{suffix}" if self.prefix else suffix

    def _put_json(self, suffix: str, payload: dict | list) -> None:
        body = json.dumps(
            _json_safe(payload), ensure_ascii=False, indent=2, default=str
        ).encode("utf-8")
        try:
            self.client.put_object(
                Bucket=self.bucket,
                Key=self._key(suffix),
                Body=body,
                ContentType="application/json; charset=utf-8",
            )
        except Exception as exc:
            raise DicasMemoryStorageError(
                f"Could not write s3://{self.bucket}/{self._key(suffix)}: {exc}"
            ) from exc

    def _get_json(self, suffix: str) -> dict | list | None:
        try:
            response = self.client.get_object(Bucket=self.bucket, Key=self._key(suffix))
        except Exception as exc:
            response_code = getattr(exc, "response", {}).get("Error", {}).get("Code")
            if str(response_code) in {"NoSuchKey", "404", "NotFound"}:
                return None
            raise DicasMemoryStorageError(
                f"Could not read s3://{self.bucket}/{self._key(suffix)}: {exc}"
            ) from exc
        try:
            payload = json.loads(response["Body"].read().decode("utf-8"))
        except (KeyError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise DicasMemoryStorageError(
                f"Invalid JSON at s3://{self.bucket}/{self._key(suffix)}"
            ) from exc
        return payload

    def _delete(self, suffix: str) -> None:
        try:
            self.client.delete_object(Bucket=self.bucket, Key=self._key(suffix))
        except Exception as exc:
            raise DicasMemoryStorageError(
                f"Could not delete s3://{self.bucket}/{self._key(suffix)}: {exc}"
            ) from exc

    def _load_index(self, suffix: str) -> list[dict]:
        value = self._get_json(suffix)
        return value if isinstance(value, list) else []

    def _save_index(self, suffix: str, items: list[dict]) -> None:
        self._put_json(suffix, items)

    def _season_root(self, season_year: int) -> str:
        return f"seasons/{int(season_year)}"

    def _register_season(self, season_year: int) -> None:
        seasons = [
            item
            for item in self._load_index("seasons/index.json")
            if _report_round(item.get("season_year")) != season_year
        ]
        seasons.append(
            {
                "season_year": season_year,
                "updated_at": utc_now().isoformat(),
                "key": f"{self._season_root(season_year)}/index.json",
            }
        )
        seasons.sort(
            key=lambda item: _report_round(item.get("season_year")) or 0,
            reverse=True,
        )
        self._save_index("seasons/index.json", seasons)
        self._put_json(
            f"{self._season_root(season_year)}/index.json",
            {
                "season_year": season_year,
                "reports_index": (
                    f"{self._season_root(season_year)}/reports/index.json"
                ),
                "memories_index": (
                    f"{self._season_root(season_year)}/memories/index.json"
                ),
                "updated_at": utc_now().isoformat(),
            },
        )

    def list_seasons(self) -> list[int]:
        return [
            season_year
            for item in self._load_index("seasons/index.json")
            if (season_year := _report_round(item.get("season_year"))) is not None
        ]

    def save_report(self, report: dict, *, season_year: int | None = None) -> dict:
        archived = _json_safe({**report})
        season_year = season_year or _season_year_from_payload(archived)
        archived["season_year"] = season_year
        report_id = _report_id(archived)
        archived["report_id"] = report_id
        rodada = _report_round(archived.get("rodada"))
        rodada_path = str(rodada) if rodada is not None else "unknown"
        root = self._season_root(season_year)
        object_key = f"{root}/reports/rounds/{rodada_path}/{report_id}.json"
        self._put_json(object_key, archived)

        summary = {
            "report_id": report_id,
            "season_year": season_year,
            "rodada": rodada,
            "title": _report_title(archived),
            "generated_at": archived.get("generated_at"),
            "model": archived.get("model"),
            "key": object_key,
        }
        index_key = f"{root}/reports/index.json"
        index = [
            item
            for item in self._load_index(index_key)
            if item.get("report_id") != report_id
        ]
        index.append(summary)
        index.sort(key=lambda item: str(item.get("generated_at") or ""), reverse=True)
        self._save_index(index_key, index)
        self._register_season(season_year)
        return archived

    def load_report(self, report_id: str) -> dict | None:
        if not REPORT_ID_PATTERN.fullmatch(report_id):
            raise ValueError("Invalid report id")
        summary = None
        for season_year in self.list_seasons():
            index = self._load_index(
                f"{self._season_root(season_year)}/reports/index.json"
            )
            summary = next(
                (item for item in index if item.get("report_id") == report_id),
                None,
            )
            if summary is not None:
                break
        if summary is None:
            return None
        payload = self._get_json(str(summary["key"]))
        if not isinstance(payload, dict):
            return None
        payload.setdefault("report_id", report_id)
        return payload

    def delete_report(self, report_id: str) -> dict | None:
        report = self.load_report(report_id)
        if report is None:
            return None
        season_year = _season_year_from_payload(report)
        index_key = f"{self._season_root(season_year)}/reports/index.json"
        index = self._load_index(index_key)
        summary = next(
            (item for item in index if item.get("report_id") == report_id), None
        )
        if summary is not None:
            self._delete(str(summary["key"]))
        self._save_index(
            index_key,
            [item for item in index if item.get("report_id") != report_id],
        )
        return report

    def list_reports(
        self,
        limit: int = 30,
        rodada: int | None = None,
        season_year: int | None = None,
    ) -> list[dict]:
        seasons = [season_year] if season_year is not None else self.list_seasons()
        items = [
            item
            for year in seasons
            for item in self._load_index(
                f"{self._season_root(year)}/reports/index.json"
            )
        ]
        if rodada is not None:
            items = [item for item in items if item.get("rodada") == rodada]
        items.sort(key=lambda item: str(item.get("generated_at") or ""), reverse=True)
        return items[:limit]

    def list_report_rounds(self, season_year: int | None = None) -> list[int]:
        rounds = {
            rodada
            for item in self.list_reports(limit=10_000, season_year=season_year)
            if (rodada := _report_round(item.get("rodada"))) is not None
        }
        return sorted(rounds, reverse=True)

    def latest_report_for_round(self, rodada: int, *, season_year: int) -> dict | None:
        summaries = self.list_reports(limit=1, rodada=rodada, season_year=season_year)
        if not summaries:
            return None
        return self.load_report(str(summaries[0]["report_id"]))

    def save_round_source(self, season_year: int, rodada: int, source: dict) -> None:
        payload = {**source, "season_year": season_year, "rodada": rodada}
        self._put_json(
            (f"{self._season_root(season_year)}/sources/rounds/rodada-{rodada}.json"),
            payload,
        )
        self._register_season(season_year)

    def load_round_source(self, season_year: int, rodada: int) -> dict | None:
        value = self._get_json(
            (f"{self._season_root(season_year)}/sources/rounds/rodada-{rodada}.json")
        )
        return value if isinstance(value, dict) else None

    def save_round_memory(self, season_year: int, rodada: int, memory: dict) -> dict:
        payload = _json_safe({**memory, "season_year": season_year, "rodada": rodada})
        root = self._season_root(season_year)
        key = f"{root}/memories/rounds/rodada-{rodada}.json"
        self._put_json(key, payload)
        index_key = f"{root}/memories/index.json"
        index = [
            item
            for item in self._load_index(index_key)
            if _report_round(item.get("rodada")) != rodada
        ]
        index.append(
            {
                "season_year": season_year,
                "rodada": rodada,
                "generated_at": payload.get("generated_at"),
                "headline": payload.get("headline"),
                "key": key,
                "schema_version": payload.get("schema_version"),
            }
        )
        index.sort(
            key=lambda item: _report_round(item.get("rodada")) or 0,
            reverse=True,
        )
        self._save_index(index_key, index)
        self._register_season(season_year)
        return payload

    def load_round_memory(self, season_year: int, rodada: int) -> dict | None:
        value = self._get_json(
            (f"{self._season_root(season_year)}/memories/rounds/rodada-{rodada}.json")
        )
        return value if isinstance(value, dict) else None

    def list_round_memories(
        self,
        *,
        season_year: int,
        before_round: int | None = None,
        limit: int = 5,
    ) -> list[dict]:
        index = self._load_index(
            f"{self._season_root(season_year)}/memories/index.json"
        )
        if before_round is not None:
            index = [
                item
                for item in index
                if (_report_round(item.get("rodada")) or 0) < before_round
            ]
        memories = []
        for item in index[: max(1, limit)]:
            rodada = _report_round(item.get("rodada"))
            if rodada is None:
                continue
            memory = self.load_round_memory(season_year, rodada)
            if memory is not None:
                memories.append(memory)
        return memories

    def migrate_legacy_layout(self, *, fallback_season_year: int | None = None) -> dict:
        fallback_season_year = fallback_season_year or current_season_year()
        migrated_reports = []
        legacy_reports = self._load_index("reports/index.json")
        for summary in legacy_reports:
            key = str(summary.get("key") or "")
            payload = self._get_json(key) if key else None
            if not isinstance(payload, dict):
                continue
            season_year = _season_year_from_payload(
                payload, fallback=fallback_season_year
            )
            archived = self.save_report(payload, season_year=season_year)
            migrated_reports.append(archived["report_id"])
            self._delete(key)
        if legacy_reports:
            self._delete("reports/index.json")

        migrated_memories = []
        legacy_memories = self._load_index("memories/index.json")
        for summary in legacy_memories:
            rodada = _report_round(summary.get("rodada"))
            key = str(summary.get("key") or "")
            payload = self._get_json(key) if key else None
            if rodada is None or not isinstance(payload, dict):
                continue
            season_year = _season_year_from_payload(
                payload, fallback=fallback_season_year
            )
            self.save_round_memory(season_year, rodada, payload)
            migrated_memories.append({"season_year": season_year, "rodada": rodada})
            self._delete(key)
            legacy_source_key = f"sources/rounds/rodada-{rodada}.json"
            source = self._get_json(legacy_source_key)
            if isinstance(source, dict):
                self.save_round_source(season_year, rodada, source)
                self._delete(legacy_source_key)
        if legacy_memories:
            self._delete("memories/index.json")
        return {
            "reports": migrated_reports,
            "memories": migrated_memories,
        }


@lru_cache
def get_dicas_memory_store() -> S3DicasMemoryStore:
    return S3DicasMemoryStore.from_env()


class PositionLesson(BaseModel):
    position: str = Field(description="Posição do Cartola FC.")
    lesson: str = Field(description="Aprendizado objetivo observado na rodada.")
    evidence: str = Field(description="Evidência factual curta que sustenta a lição.")


class MatchupLesson(BaseModel):
    matchup: str = Field(
        description="Confronto no formato Time mandante x Time visitante."
    )
    lesson: str = Field(
        description="Aprendizado objetivo sobre como o confronto afetou os scouts."
    )
    evidence: str = Field(
        description=(
            "Evidência com jogador, clube, adversário e scouts que sustentam a lição."
        )
    )


class RoundMemorySynthesis(BaseModel):
    headline: str = Field(description="Resumo de uma frase da rodada.")
    best_insights: list[str] = Field(
        description="De três a seis insights mais úteis da rodada."
    )
    position_lessons: list[PositionLesson] = Field(
        description="Aprendizados por posição, somente quando houver evidência."
    )
    matchup_lessons: list[MatchupLesson] = Field(
        min_length=1,
        description=(
            "Aprendizados por confronto, sempre identificando o clube do jogador, "
            "o adversário e os scouts observados."
        ),
    )
    prediction_lessons: list[str] = Field(
        description="O que funcionou ou falhou no relatório pré-rodada."
    )
    signals_to_reuse: list[str] = Field(
        description="Sinais que merecem peso ao analisar a próxima rodada."
    )
    risks_to_watch: list[str] = Field(
        description="Riscos, exceções e padrões frágeis que não devem ser generalizados."
    )


def _club_name(clubs: dict, clube_id: int) -> str:
    club = clubs.get(str(clube_id), {})
    return club.get("nome_fantasia") or club.get("nome") or f"Clube {clube_id}"


def _position_name(positions: dict, posicao_id: int) -> str:
    position = positions.get(str(posicao_id), {})
    return position.get("nome") or position.get("abreviacao") or f"Posição {posicao_id}"


def _optional_int(value: Any) -> int | None:
    try:
        if pd.isna(value):
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


def build_round_source_snapshot(
    store: RedisDataFrameStore,
    memory_store: S3DicasMemoryStore,
    season_year: int,
    rodada: int,
) -> dict:
    pontuacoes = store.load_dataframe("pontuacoes")
    if not isinstance(pontuacoes, pd.DataFrame) or pontuacoes.empty:
        raise RuntimeError("Pontuações históricas não estão disponíveis.")
    required_columns = {
        "atleta_id",
        "rodada_id",
        "clube_id",
        "posicao_id",
        "pontuacao",
    }
    missing_columns = required_columns - set(pontuacoes.columns)
    if missing_columns:
        raise RuntimeError(
            "Pontuações históricas incompletas: " + ", ".join(sorted(missing_columns))
        )
    rodada_df = pontuacoes.loc[pontuacoes["rodada_id"] == rodada].copy()
    if rodada_df.empty:
        raise RuntimeError(f"A rodada {rodada} ainda não possui pontuações.")

    confrontos = store.load_dataframe("confrontos")
    matchup_columns = {
        "clube_id",
        "opponent_clube_id",
        "is_mandante",
        "rodada_id",
        "partida_id",
    }
    if not isinstance(confrontos, pd.DataFrame) or not matchup_columns.issubset(
        confrontos.columns
    ):
        raise RuntimeError("Confrontos históricos não estão disponíveis.")
    rodada_confrontos = confrontos.loc[confrontos["rodada_id"] == rodada].copy()
    if rodada_confrontos.empty:
        raise RuntimeError(f"A rodada {rodada} ainda não possui confrontos.")

    rodada_df["pontuacao"] = pd.to_numeric(
        rodada_df["pontuacao"], errors="coerce"
    ).fillna(0)
    if "pontuacao_basica" in rodada_df.columns:
        rodada_df["pontuacao_basica"] = pd.to_numeric(
            rodada_df["pontuacao_basica"], errors="coerce"
        ).fillna(0)
    else:
        rodada_df["pontuacao_basica"] = 0.0
    clubs = store.load_json("clubes") or {}
    positions = store.load_json("posicoes") or {}
    atletas = store.load_dataframe("atletas")
    player_names: dict[int, str] = {}
    if isinstance(atletas, pd.DataFrame) and {
        "atleta_id",
        "apelido",
    }.issubset(atletas.columns):
        player_names = {
            int(row.atleta_id): str(row.apelido)
            for row in atletas[["atleta_id", "apelido"]]
            .dropna(subset=["atleta_id"])
            .itertuples(index=False)
        }

    matchup_by_club = {
        int(row["clube_id"]): row for row in rodada_confrontos.to_dict(orient="records")
    }

    def matchup_context(clube_id: int) -> dict:
        row = matchup_by_club.get(clube_id)
        if row is None:
            raise RuntimeError(
                f"Confronto do clube {clube_id} não encontrado na rodada {rodada}."
            )
        adversario_id = int(row["opponent_clube_id"])
        is_mandante = bool(row["is_mandante"])
        mandante_id = clube_id if is_mandante else adversario_id
        visitante_id = adversario_id if is_mandante else clube_id
        placar_clube = _optional_int(row.get("placar_clube"))
        placar_adversario = _optional_int(row.get("placar_adversario"))
        placar_mandante = placar_clube if is_mandante else placar_adversario
        placar_visitante = placar_adversario if is_mandante else placar_clube
        mandante = _club_name(clubs, mandante_id)
        visitante = _club_name(clubs, visitante_id)
        confronto = f"{mandante} x {visitante}"
        if placar_mandante is not None and placar_visitante is not None:
            confronto = f"{mandante} {placar_mandante} x {placar_visitante} {visitante}"
        return {
            "partida_id": _optional_int(row.get("partida_id")),
            "adversario_id": adversario_id,
            "adversario": _club_name(clubs, adversario_id),
            "is_mandante": is_mandante,
            "mando": "mandante" if is_mandante else "visitante",
            "placar_clube": placar_clube,
            "placar_adversario": placar_adversario,
            "confronto": confronto,
        }

    def player_record(row: pd.Series) -> dict:
        atleta_id = int(row["atleta_id"])
        clube_id = int(row["clube_id"])
        posicao_id = int(row["posicao_id"])
        scouts = {}
        for scout in Scout.as_list():
            value = pd.to_numeric(row.get(scout, 0), errors="coerce")
            scout_value = 0 if pd.isna(value) else int(value)
            if scout_value != 0:
                scouts[scout] = scout_value
        return {
            "atleta_id": atleta_id,
            "apelido": player_names.get(atleta_id, f"Atleta {atleta_id}"),
            "clube_id": clube_id,
            "clube": _club_name(clubs, clube_id),
            "posicao_id": posicao_id,
            "posicao": _position_name(positions, posicao_id),
            "pontuacao": round(float(row["pontuacao"]), 2),
            "pontuacao_basica": round(float(row["pontuacao_basica"]), 2),
            "scouts": scouts,
            "matchup": matchup_context(clube_id),
        }

    sorted_players = rodada_df.sort_values("pontuacao", ascending=False)
    top_performers = [
        player_record(row) for _, row in sorted_players.head(20).iterrows()
    ]
    top_by_position = {}
    for posicao_id, group in sorted_players.groupby("posicao_id", sort=True):
        position_id = int(posicao_id)
        top_by_position[str(position_id)] = {
            "position": _position_name(positions, position_id),
            "players": [player_record(row) for _, row in group.head(7).iterrows()],
        }

    club_summary = (
        rodada_df.groupby("clube_id", as_index=False)
        .agg(
            total_fantasy_points=("pontuacao", "sum"),
            average_fantasy_points=("pontuacao", "mean"),
            players=("atleta_id", "nunique"),
        )
        .sort_values("total_fantasy_points", ascending=False)
    )
    team_performance = [
        {
            "clube_id": int(row.clube_id),
            "clube": _club_name(clubs, int(row.clube_id)),
            "matchup": matchup_context(int(row.clube_id)),
            "total_fantasy_points": round(float(row.total_fantasy_points), 2),
            "average_fantasy_points": round(float(row.average_fantasy_points), 2),
            "players": int(row.players),
        }
        for row in club_summary.itertuples(index=False)
    ]

    matchups = []
    mandantes = rodada_confrontos.loc[rodada_confrontos["is_mandante"].astype(bool)]
    for row in mandantes.to_dict(orient="records"):
        mandante_id = int(row["clube_id"])
        visitante_id = int(row["opponent_clube_id"])
        match_players = sorted_players.loc[
            sorted_players["clube_id"].isin([mandante_id, visitante_id])
        ]
        matchups.append(
            {
                "partida_id": _optional_int(row.get("partida_id")),
                "confronto": matchup_context(mandante_id)["confronto"],
                "mandante": {
                    "clube_id": mandante_id,
                    "clube": _club_name(clubs, mandante_id),
                    "placar": _optional_int(row.get("placar_clube")),
                    "total_fantasy_points": round(
                        float(
                            rodada_df.loc[
                                rodada_df["clube_id"] == mandante_id, "pontuacao"
                            ].sum()
                        ),
                        2,
                    ),
                },
                "visitante": {
                    "clube_id": visitante_id,
                    "clube": _club_name(clubs, visitante_id),
                    "placar": _optional_int(row.get("placar_adversario")),
                    "total_fantasy_points": round(
                        float(
                            rodada_df.loc[
                                rodada_df["clube_id"] == visitante_id, "pontuacao"
                            ].sum()
                        ),
                        2,
                    ),
                },
                "top_performers": [
                    player_record(player)
                    for _, player in match_players.head(6).iterrows()
                ],
            }
        )

    previous_report = memory_store.latest_report_for_round(
        rodada, season_year=season_year
    )
    return _json_safe(
        {
            "schema_version": MEMORY_SCHEMA_VERSION,
            "season_year": season_year,
            "rodada": rodada,
            "captured_at": utc_now().isoformat(),
            "actuals": {
                "players_with_points": int(rodada_df["atleta_id"].nunique()),
                "matchups": matchups,
                "top_performers": top_performers,
                "top_by_position": top_by_position,
                "team_performance": team_performance,
            },
            "pre_round_report": previous_report,
        }
    )


async def synthesize_round_memory(source: dict) -> dict:
    if not os.environ.get("OPENAI_API_KEY"):
        raise DicasMemoryConfigurationError(
            "OPENAI_API_KEY is required to synthesize round memory."
        )
    model_name = os.environ.get("DICAS_MODEL", DEFAULT_DICAS_MODEL)
    if not model_name.startswith("openai:"):
        raise DicasMemoryConfigurationError(
            "Round-memory synthesis currently requires an openai: DICAS_MODEL."
        )

    from langchain_openai import ChatOpenAI

    model = ChatOpenAI(
        model=model_name.split(":", 1)[1],
        reasoning_effort=os.environ.get(
            "DICAS_REASONING_EFFORT", DEFAULT_DICAS_REASONING_EFFORT
        ),
        use_responses_api=True,
    ).with_structured_output(RoundMemorySynthesis)
    result = await model.ainvoke(
        [
            {
                "role": "system",
                "content": (
                    "Você mantém a memória analítica do CartolaPy. Resuma apenas "
                    "evidências presentes no snapshot. Compare o relatório pré-rodada "
                    "com o resultado real quando ele existir. Não trate um caso isolado "
                    "como regra geral, não invente contexto de jogo e escreva em "
                    "português do Brasil. Dê prioridade ao contexto de cada confronto: "
                    "ao citar um jogador ou scout, diga obrigatoriamente por qual clube "
                    "ele jogou, contra qual adversário, se foi mandante ou visitante e "
                    "o placar quando disponível. Explique quais scouts apareceram "
                    "naquele matchup e evite transformar desempenho contra um adversário "
                    "específico em tendência universal. Produza memória curta e acionável "
                    "para a previsão da próxima rodada."
                ),
            },
            {
                "role": "user",
                "content": json.dumps(source, ensure_ascii=False, default=str),
            },
        ]
    )
    if isinstance(result, RoundMemorySynthesis):
        return result.model_dump()
    if isinstance(result, dict):
        return RoundMemorySynthesis.model_validate(result).model_dump()
    raise RuntimeError("The memory model returned an invalid structured response.")


async def create_round_memory(
    store: RedisDataFrameStore,
    memory_store: S3DicasMemoryStore,
    season_year: int,
    rodada: int,
    *,
    force: bool = False,
) -> dict:
    existing = memory_store.load_round_memory(season_year, rodada)
    if existing is not None and not force:
        return {
            "season_year": season_year,
            "rodada": rodada,
            "status": "already_exists",
            "memory": existing,
        }

    source = build_round_source_snapshot(store, memory_store, season_year, rodada)
    synthesis = await synthesize_round_memory(source)
    memory_store.save_round_source(season_year, rodada, source)
    memory = memory_store.save_round_memory(
        season_year,
        rodada,
        {
            "schema_version": MEMORY_SCHEMA_VERSION,
            "season_year": season_year,
            "rodada": rodada,
            "generated_at": utc_now().isoformat(),
            "source_key": (
                f"seasons/{season_year}/sources/rounds/rodada-{rodada}.json"
            ),
            "source_report_id": (
                (source.get("pre_round_report") or {}).get("report_id")
            ),
            "actual_players": source["actuals"]["players_with_points"],
            **synthesis,
        },
    )
    return {
        "season_year": season_year,
        "rodada": rodada,
        "status": "regenerated" if existing is not None else "created",
        "memory": memory,
    }


def _positive_int_env(name: str, default: int) -> int:
    try:
        value = int(os.environ.get(name, str(default)))
    except ValueError:
        return default
    return max(1, value)


async def refresh_round_memories(
    store: RedisDataFrameStore,
    memory_store: S3DicasMemoryStore | None = None,
    season_year: int | None = None,
    *,
    force: bool = False,
) -> dict:
    memory_store = memory_store or get_dicas_memory_store()
    season_year = season_year or current_season_year()
    current_round = store.load_rodada_id()
    pontuacoes = store.load_dataframe("pontuacoes")
    if (
        current_round is None
        or not isinstance(pontuacoes, pd.DataFrame)
        or "rodada_id" not in pontuacoes.columns
    ):
        return {
            "status": "no_completed_round",
            "season_year": season_year,
            "current_round": current_round,
            "created": [],
            "skipped": [],
        }
    completed_rounds = sorted(
        rodada
        for rodada in set(
            pd.to_numeric(pontuacoes["rodada_id"], errors="coerce").dropna().astype(int)
        )
        if rodada <= int(current_round)
    )
    lookback = _positive_int_env(
        "DICAS_MEMORY_LOOKBACK_ROUNDS", DEFAULT_MEMORY_LOOKBACK_ROUNDS
    )
    target_rounds = completed_rounds[-lookback:]
    created = []
    regenerated = []
    skipped = []
    for rodada in target_rounds:
        result = await create_round_memory(
            store,
            memory_store,
            season_year,
            rodada,
            force=force,
        )
        if result["status"] == "created":
            created.append(rodada)
        elif result["status"] == "regenerated":
            regenerated.append(rodada)
        else:
            skipped.append(rodada)
    return {
        "status": "completed",
        "season_year": season_year,
        "current_round": int(current_round),
        "checked": target_rounds,
        "created": created,
        "regenerated": regenerated,
        "skipped": skipped,
    }


def load_memories_for_prediction(
    rodada: int,
    *,
    season_year: int | None = None,
    limit: int | None = None,
    memory_store: S3DicasMemoryStore | None = None,
) -> dict:
    memory_store = memory_store or get_dicas_memory_store()
    season_year = season_year or current_season_year()
    limit = limit or _positive_int_env(
        "DICAS_MEMORY_CONTEXT_ROUNDS", DEFAULT_MEMORY_CONTEXT_ROUNDS
    )
    memories = memory_store.list_round_memories(
        season_year=season_year,
        before_round=rodada,
        limit=min(max(limit, 1), 10),
    )
    return {
        "available": bool(memories),
        "season_year": season_year,
        "target_round": rodada,
        "rounds": [memory.get("rodada") for memory in memories],
        "memories": memories,
    }
