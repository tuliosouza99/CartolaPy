import asyncio
import re
import unicodedata
from datetime import datetime, timezone
from difflib import SequenceMatcher
from typing import Any, Literal
from urllib.parse import urlencode

from .fotmob_mappings import build_predefined_club_mapping


FOTMOB_BASE_URL = "https://www.fotmob.com/api/data"
FOTMOB_SEARCH_URL = "https://apigw.fotmob.com/searchapi/suggest"
BRASILEIRAO_LEAGUE_ID = 268


class FotmobMappingError(RuntimeError):
    """Raised when a Cartola entity cannot be matched safely to FotMob."""


def normalize_name(value: str | None) -> str:
    if not value:
        return ""
    normalized = unicodedata.normalize("NFKD", value)
    ascii_value = "".join(
        character for character in normalized if not unicodedata.combining(character)
    )
    return re.sub(r"[^a-z0-9]+", " ", ascii_value.lower()).strip()


def name_score(reference_names: list[str], candidate_name: str) -> float:
    candidate = normalize_name(candidate_name)
    if not candidate:
        return 0.0

    candidate_tokens = set(candidate.split())
    best = 0.0
    for reference_name in reference_names:
        reference = normalize_name(reference_name)
        if not reference:
            continue
        if reference == candidate:
            return 1.0

        reference_tokens = set(reference.split())
        if len(candidate_tokens) >= 2 and candidate_tokens.issubset(reference_tokens):
            best = max(best, 0.92)
        if len(reference_tokens) >= 2 and reference_tokens.issubset(candidate_tokens):
            best = max(best, 0.9)

        overlap = len(reference_tokens & candidate_tokens) / max(
            len(reference_tokens | candidate_tokens), 1
        )
        sequence = SequenceMatcher(None, reference, candidate).ratio()
        best = max(best, overlap * 0.55 + sequence * 0.45)
    return best


def _as_int(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


class FotmobService:
    def __init__(self, store, request_handler):
        self.store = store
        self.request_handler = request_handler

    async def _cached_get(
        self, cache_key: str, url: str, ttl_seconds: int
    ) -> dict[str, Any]:
        cached = self.store.load_json(cache_key)
        if isinstance(cached, dict) and cached:
            return cached

        payload = await self.request_handler.make_get_request(url)
        if not isinstance(payload, dict):
            raise RuntimeError(f"Unexpected FotMob response for {cache_key}")
        self.store.save_json(cache_key, payload, ttl_seconds=ttl_seconds)
        return payload

    async def get_league(self) -> dict[str, Any]:
        query = urlencode({"id": BRASILEIRAO_LEAGUE_ID, "ccode3": "BRA"})
        return await self._cached_get(
            f"fotmob:league:{BRASILEIRAO_LEAGUE_ID}",
            f"{FOTMOB_BASE_URL}/leagues?{query}",
            ttl_seconds=6 * 60 * 60,
        )

    async def get_team(self, team_id: int) -> dict[str, Any]:
        query = urlencode({"id": team_id, "ccode3": "BRA"})
        return await self._cached_get(
            f"fotmob:team:{team_id}",
            f"{FOTMOB_BASE_URL}/teams?{query}",
            ttl_seconds=6 * 60 * 60,
        )

    async def get_player(self, player_id: int) -> dict[str, Any]:
        query = urlencode({"id": player_id})
        return await self._cached_get(
            f"fotmob:player:{player_id}",
            f"{FOTMOB_BASE_URL}/playerData?{query}",
            ttl_seconds=6 * 60 * 60,
        )

    async def _search(self, term: str) -> dict[str, Any]:
        query = urlencode({"term": term, "lang": "en"})
        return await self.request_handler.make_get_request(
            f"{FOTMOB_SEARCH_URL}?{query}"
        )

    @staticmethod
    def _league_teams(league: dict[str, Any]) -> list[dict[str, Any]]:
        teams: dict[int, dict[str, Any]] = {}
        for match in league.get("fixtures", {}).get("allMatches", []):
            for side in ("home", "away"):
                team = match.get(side, {})
                team_id = _as_int(team.get("id"))
                if team_id is not None:
                    teams[team_id] = {"id": team_id, "name": team.get("name", "")}
        return list(teams.values())

    async def resolve_club(
        self, cartola_club_id: int, cartola_club: dict[str, Any]
    ) -> dict[str, Any]:
        mapping_key = f"fotmob:mapping:club:{cartola_club_id}"
        predefined = build_predefined_club_mapping(cartola_club_id)
        if predefined is not None:
            cached = self.store.load_json(mapping_key)
            if (
                not isinstance(cached, dict)
                or cached.get("fotmob_id") != predefined["fotmob_id"]
            ):
                self.store.save_json(mapping_key, predefined)
            return predefined

        cached = self.store.load_json(mapping_key)
        if isinstance(cached, dict) and _as_int(cached.get("fotmob_id")) is not None:
            return cached

        aliases = [
            cartola_club.get("nome", ""),
            cartola_club.get("nome_fantasia", ""),
            cartola_club.get("apelido", ""),
            cartola_club.get("abreviacao", ""),
        ]
        candidates = self._league_teams(await self.get_league())
        ranked = sorted(
            (
                (name_score(aliases, candidate.get("name", "")), candidate)
                for candidate in candidates
            ),
            key=lambda item: item[0],
            reverse=True,
        )
        if not ranked or ranked[0][0] < 0.58:
            raise FotmobMappingError(
                f"Clube Cartola {cartola_club_id} não encontrado no Brasileirão do FotMob"
            )

        score, candidate = ranked[0]
        mapping = {
            "cartola_id": cartola_club_id,
            "fotmob_id": candidate["id"],
            "cartola_name": next((alias for alias in aliases if alias), ""),
            "fotmob_name": candidate.get("name", ""),
            "matched_by": "brasileirao_club_name",
            "confidence": round(score, 3),
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
        self.store.save_json(mapping_key, mapping)
        return mapping

    @staticmethod
    def _search_player_candidates(payload: dict[str, Any]) -> list[dict[str, Any]]:
        candidates = []
        for suggestion in payload.get("squadMemberSuggest", []):
            for option in suggestion.get("options", []):
                option_payload = option.get("payload", {})
                name = option.get("text", "").split("|", 1)[0]
                player_id = _as_int(option_payload.get("id"))
                team_id = _as_int(option_payload.get("teamId"))
                if player_id is not None and not option_payload.get("isCoach", False):
                    candidates.append(
                        {"id": player_id, "team_id": team_id, "name": name}
                    )
        return candidates

    @staticmethod
    def _squad_player_candidates(team: dict[str, Any]) -> list[dict[str, Any]]:
        candidates = []
        for group in team.get("squad", {}).get("squad", []):
            if group.get("title") == "coach":
                continue
            for member in group.get("members", []):
                player_id = _as_int(member.get("id"))
                if player_id is not None:
                    candidates.append(
                        {
                            "id": player_id,
                            "name": member.get("name", ""),
                            "team_id": _as_int(team.get("details", {}).get("id")),
                        }
                    )
        return candidates

    async def resolve_player(
        self,
        cartola_player_id: int,
        player_names: list[str],
        fotmob_team_id: int,
    ) -> dict[str, Any]:
        mapping_key = f"fotmob:mapping:player:{cartola_player_id}"
        cached = self.store.load_json(mapping_key)
        if isinstance(cached, dict) and _as_int(cached.get("fotmob_id")) is not None:
            return cached

        unique_names = list(dict.fromkeys(name for name in player_names if name))
        full_name = unique_names[0] if unique_names else ""
        search_results = await asyncio.gather(
            *(self._search(name) for name in unique_names), return_exceptions=True
        )
        all_search_candidates = []
        for search_result in search_results:
            if isinstance(search_result, dict):
                all_search_candidates.extend(
                    self._search_player_candidates(search_result)
                )

        squad_candidates = self._squad_player_candidates(
            await self.get_team(fotmob_team_id)
        )
        team_search_candidates = [
            candidate
            for candidate in all_search_candidates
            if candidate.get("team_id") == fotmob_team_id
        ]
        combined: dict[int, dict[str, Any]] = {
            candidate["id"]: candidate
            for candidate in [*squad_candidates, *team_search_candidates]
        }
        candidates = list(combined.values())
        matched_by = "team_squad_and_name_search"

        ranked = sorted(
            (
                (name_score(player_names, candidate.get("name", "")), candidate)
                for candidate in candidates
            ),
            key=lambda item: item[0],
            reverse=True,
        )
        if not ranked or ranked[0][0] < 0.55:
            unique_global_candidates = {
                candidate["id"]: candidate for candidate in all_search_candidates
            }.values()
            global_ranked = sorted(
                (
                    (name_score(player_names, candidate.get("name", "")), candidate)
                    for candidate in unique_global_candidates
                ),
                key=lambda item: item[0],
                reverse=True,
            )
            if global_ranked and global_ranked[0][0] >= 0.9:
                ranked = global_ranked
                matched_by = "global_exact_name_search"

        if not ranked or ranked[0][0] < 0.55:
            raise FotmobMappingError(
                f"Atleta Cartola {cartola_player_id} não encontrado no elenco do FotMob"
            )

        score, candidate = ranked[0]
        if len(ranked) > 1 and score < 0.9 and score - ranked[1][0] < 0.06:
            exact_team_candidates = [
                item
                for item in ranked
                if item[1].get("team_id") == fotmob_team_id and item[0] == score
            ]
            if len(exact_team_candidates) == 1:
                score, candidate = exact_team_candidates[0]
            else:
                raise FotmobMappingError(
                    f"Mapeamento ambíguo para o atleta Cartola {cartola_player_id}"
                )

        mapping = {
            "cartola_id": cartola_player_id,
            "fotmob_id": candidate["id"],
            "cartola_name": full_name,
            "fotmob_name": candidate.get("name", ""),
            "fotmob_team_id": fotmob_team_id,
            "matched_by": matched_by,
            "confidence": round(score, 3),
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
        self.store.save_json(mapping_key, mapping)
        return mapping

    async def _get_match(self, match: dict[str, Any]) -> dict[str, Any]:
        match_id = _as_int(match.get("id"))
        if match_id is None:
            return {}
        query = urlencode({"matchId": match_id})
        finished = bool(match.get("status", {}).get("finished"))
        return await self._cached_get(
            f"fotmob:match:{match_id}",
            f"{FOTMOB_BASE_URL}/matchDetails?{query}",
            ttl_seconds=30 * 24 * 60 * 60 if finished else 5 * 60,
        )

    @staticmethod
    def _find_player_stats(
        match_details: dict[str, Any], player_id: int
    ) -> dict[str, Any] | None:
        player_stats = match_details.get("content", {}).get("playerStats", {})
        if not isinstance(player_stats, dict):
            return None
        direct = player_stats.get(str(player_id)) or player_stats.get(player_id)
        if isinstance(direct, dict):
            return direct
        return next(
            (
                value
                for value in player_stats.values()
                if isinstance(value, dict) and _as_int(value.get("id")) == player_id
            ),
            None,
        )

    @staticmethod
    def _select_matches(
        league: dict[str, Any],
        team_id: int,
        rodada_min: int,
        rodada_max: int,
        is_mandante: Literal["geral", "mandante", "visitante"],
    ) -> list[dict[str, Any]]:
        selected = []
        for match in league.get("fixtures", {}).get("allMatches", []):
            round_id = _as_int(match.get("roundName") or match.get("round"))
            home_id = _as_int(match.get("home", {}).get("id"))
            away_id = _as_int(match.get("away", {}).get("id"))
            if round_id is None or not rodada_min <= round_id <= rodada_max:
                continue
            is_home = home_id == team_id
            if not is_home and away_id != team_id:
                continue
            if is_mandante == "mandante" and not is_home:
                continue
            if is_mandante == "visitante" and is_home:
                continue
            if not match.get("status", {}).get("started"):
                continue
            selected.append(match)
        return selected

    @staticmethod
    def _aggregate_stats(
        appearances: list[tuple[dict[str, Any], dict[str, Any]]],
    ) -> tuple[list[dict[str, Any]], int]:
        groups: dict[str, dict[str, Any]] = {}
        minutes = 0

        for _, player_stats in appearances:
            match_minutes = 0
            for group in player_stats.get("stats", []):
                for metric in group.get("stats", {}).values():
                    if metric.get("key") == "minutes_played":
                        match_minutes = (
                            _as_int(metric.get("stat", {}).get("value")) or 0
                        )
                        break
            minutes += match_minutes

            for group in player_stats.get("stats", []):
                group_key = group.get("key") or normalize_name(group.get("title", ""))
                aggregate_group = groups.setdefault(
                    group_key,
                    {"key": group_key, "title": group.get("title", ""), "metrics": {}},
                )
                for title, metric in group.get("stats", {}).items():
                    stat = metric.get("stat", {})
                    value = stat.get("value")
                    if not isinstance(value, (int, float)):
                        continue
                    metric_key = metric.get("key") or normalize_name(title)
                    aggregate_metric = aggregate_group["metrics"].setdefault(
                        metric_key,
                        {
                            "key": metric_key,
                            "title": title,
                            "format": stat.get("type", "number"),
                            "value": 0.0,
                            "attempts": 0.0,
                            "rating_weighted": 0.0,
                            "rating_minutes": 0,
                        },
                    )
                    if metric_key == "rating_title":
                        aggregate_metric["rating_weighted"] += float(value) * max(
                            match_minutes, 1
                        )
                        aggregate_metric["rating_minutes"] += max(match_minutes, 1)
                    else:
                        aggregate_metric["value"] += float(value)
                        total = stat.get("total")
                        if isinstance(total, (int, float)):
                            aggregate_metric["attempts"] += float(total)

        result = []
        for group in groups.values():
            metrics = []
            for metric in group["metrics"].values():
                if metric["key"] == "rating_title":
                    divisor = metric.pop("rating_minutes") or 1
                    value = metric.pop("rating_weighted") / divisor
                    per90 = None
                else:
                    metric.pop("rating_minutes")
                    metric.pop("rating_weighted")
                    value = metric["value"]
                    per90 = value * 90 / minutes if minutes else 0.0
                metric["value"] = round(value, 2)
                metric["per90"] = round(per90, 2) if per90 is not None else None
                metric["attempts"] = (
                    round(metric["attempts"], 2) if metric["attempts"] else None
                )
                metrics.append(metric)
            result.append(
                {"key": group["key"], "title": group["title"], "metrics": metrics}
            )
        return result, minutes

    async def get_opta_view(
        self,
        cartola_player: dict[str, Any],
        cartola_club: dict[str, Any],
        rodada_min: int,
        rodada_max: int,
        is_mandante: Literal["geral", "mandante", "visitante"],
    ) -> dict[str, Any]:
        cartola_player_id = int(cartola_player["atleta_id"])
        club_mapping = await self.resolve_club(
            int(cartola_player["clube_id"]), cartola_club
        )
        player_mapping = await self.resolve_player(
            cartola_player_id,
            [cartola_player.get("nome", ""), cartola_player.get("apelido", "")],
            int(club_mapping["fotmob_id"]),
        )
        player_id = int(player_mapping["fotmob_id"])
        league = await self.get_league()
        selected_matches = self._select_matches(
            league,
            int(club_mapping["fotmob_id"]),
            rodada_min,
            rodada_max,
            is_mandante,
        )
        fetched = await asyncio.gather(
            *(self._get_match(match) for match in selected_matches),
            return_exceptions=True,
        )

        appearances = []
        shots = []
        matches = []
        team_id = int(club_mapping["fotmob_id"])
        for fixture, details in zip(selected_matches, fetched, strict=True):
            if isinstance(details, Exception):
                continue
            player_stats = self._find_player_stats(details, player_id)
            if not player_stats or not player_stats.get("stats"):
                continue

            general = details.get("general", {})
            round_id = _as_int(general.get("matchRound") or fixture.get("round")) or 0
            home_team = general.get("homeTeam", {})
            away_team = general.get("awayTeam", {})
            is_home = _as_int(home_team.get("id")) == team_id
            opponent = away_team if is_home else home_team
            score = fixture.get("status", {}).get("scoreStr", "")
            match_meta = {
                "match_id": _as_int(general.get("matchId") or fixture.get("id")) or 0,
                "round": round_id,
                "date": general.get("matchTimeUTCDate")
                or fixture.get("status", {}).get("utcTime"),
                "is_home": is_home,
                "opponent_id": _as_int(opponent.get("id")) or 0,
                "opponent_name": opponent.get("name", ""),
                "score": score,
            }
            matches.append(match_meta)
            appearances.append((match_meta, player_stats))
            for shot in player_stats.get("shotmap", []):
                shots.append({**shot, **match_meta})

        stat_groups, minutes = self._aggregate_stats(appearances)
        profile = await self.get_player(player_id)
        return {
            "available": True,
            "data_provider": profile.get("dataProvider", "opta"),
            "league": {"id": BRASILEIRAO_LEAGUE_ID, "name": "Brasileirão Série A"},
            "mapping": {"club": club_mapping, "player": player_mapping},
            "profile": {
                "id": player_id,
                "name": profile.get("name") or player_mapping.get("fotmob_name"),
                "position": profile.get("positionDescription", {})
                .get("primaryPosition", {})
                .get("label")
                if isinstance(profile.get("positionDescription"), dict)
                else profile.get("positionDescription"),
                "birth_date": profile.get("birthDate", {}).get("utcTime")
                if isinstance(profile.get("birthDate"), dict)
                else profile.get("birthDate"),
                "team_id": team_id,
                "team_name": club_mapping.get("fotmob_name", ""),
            },
            "summary": {
                "matches": len(appearances),
                "minutes": minutes,
                "shots": len(shots),
                "goals": sum(1 for shot in shots if shot.get("eventType") == "Goal"),
                "xg": round(
                    sum(float(shot.get("expectedGoals") or 0) for shot in shots), 2
                ),
                "shots_on_target": sum(1 for shot in shots if shot.get("isOnTarget")),
            },
            "stat_groups": stat_groups,
            "shots": shots,
            "matches": sorted(matches, key=lambda match: match["round"], reverse=True),
        }
