from datetime import datetime, timezone
from typing import Any


# Brasileirão Série A 2026. Cartola club IDs come from /atletas/mercado and
# FotMob IDs come from league 268. This table is authoritative for the season;
# runtime name matching is only a fallback for future promoted clubs.
BRASILEIRAO_CLUB_MAPPINGS: dict[int, dict[str, Any]] = {
    262: {"fotmob_id": 9770, "cartola_slug": "flamengo", "fotmob_name": "Flamengo"},
    263: {"fotmob_id": 8517, "cartola_slug": "botafogo", "fotmob_name": "Botafogo RJ"},
    264: {
        "fotmob_id": 9808,
        "cartola_slug": "corinthians",
        "fotmob_name": "Corinthians",
    },
    265: {"fotmob_id": 7877, "cartola_slug": "bahia", "fotmob_name": "Bahia"},
    266: {"fotmob_id": 9863, "cartola_slug": "fluminense", "fotmob_name": "Fluminense"},
    267: {"fotmob_id": 10276, "cartola_slug": "vasco", "fotmob_name": "Vasco da Gama"},
    275: {"fotmob_id": 10283, "cartola_slug": "palmeiras", "fotmob_name": "Palmeiras"},
    276: {"fotmob_id": 10277, "cartola_slug": "sao-paulo", "fotmob_name": "Sao Paulo"},
    277: {"fotmob_id": 8514, "cartola_slug": "santos", "fotmob_name": "Santos FC"},
    280: {
        "fotmob_id": 109705,
        "cartola_slug": "bragantino",
        "fotmob_name": "Red Bull Bragantino",
    },
    282: {
        "fotmob_id": 10272,
        "cartola_slug": "atletico-mg",
        "fotmob_name": "Atletico MG",
    },
    283: {"fotmob_id": 9781, "cartola_slug": "cruzeiro", "fotmob_name": "Cruzeiro"},
    284: {"fotmob_id": 9769, "cartola_slug": "gremio", "fotmob_name": "Gremio"},
    285: {
        "fotmob_id": 8702,
        "cartola_slug": "internacional",
        "fotmob_name": "Internacional",
    },
    287: {"fotmob_id": 7733, "cartola_slug": "vitoria", "fotmob_name": "Vitoria"},
    293: {
        "fotmob_id": 10273,
        "cartola_slug": "atletico-pr",
        "fotmob_name": "Athletico Paranaense",
    },
    294: {"fotmob_id": 9767, "cartola_slug": "coritiba", "fotmob_name": "Coritiba"},
    315: {
        "fotmob_id": 197693,
        "cartola_slug": "chapecoense",
        "fotmob_name": "Chapecoense AF",
    },
    364: {"fotmob_id": 1626, "cartola_slug": "remo", "fotmob_name": "Remo"},
    2305: {"fotmob_id": 163782, "cartola_slug": "mirassol", "fotmob_name": "Mirassol"},
}


def build_predefined_club_mapping(cartola_club_id: int) -> dict[str, Any] | None:
    predefined = BRASILEIRAO_CLUB_MAPPINGS.get(cartola_club_id)
    if predefined is None:
        return None
    return {
        "cartola_id": cartola_club_id,
        **predefined,
        "cartola_name": predefined["cartola_slug"],
        "matched_by": "predefined_brasileirao_2026",
        "confidence": 1.0,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }


def seed_fotmob_club_mappings(store) -> None:
    mappings = {}
    for cartola_club_id in BRASILEIRAO_CLUB_MAPPINGS:
        mapping = build_predefined_club_mapping(cartola_club_id)
        mappings[str(cartola_club_id)] = mapping
        store.save_json(f"fotmob:mapping:club:{cartola_club_id}", mapping)
    store.save_json("fotmob:mapping:clubs", mappings)
