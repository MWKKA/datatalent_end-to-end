"""Ingestion raw des offres France Travail dans la base team_working."""

from __future__ import annotations

import argparse
import json
import sqlite3
import os
from pathlib import Path
from uuid import uuid4
import time
import sys
from datetime import datetime, timezone

import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from scripts.ingestion.france_travail_auth import get_access_token  # noqa: E402
from scripts.core.common import load_project_env  # noqa: E402
from team_working.scripts.init_team_working_db import init_db  # noqa: E402

DB_PATH = Path(__file__).resolve().parents[1] / "db" / "team_working.db"
OFFRES_SEARCH_URL = "https://api.francetravail.io/partenaire/offresdemploi/v2/offres/search"
ADZUNA_SEARCH_URL_TEMPLATE = "https://api.adzuna.com/v1/api/jobs/fr/search/{page}"
DEFAULT_MOTS_CLES = "data engineer"
PAGE_SIZE = 150
MAX_OFFSET = 1000
REQUEST_TIMEOUT_SECONDS = 30
DELAY_BETWEEN_REQUESTS_SEC = 0.2
ADZUNA_PAGE_SIZE = 50

load_project_env()


def build_headers(token: str, range_start: int, range_end: int) -> dict[str, str]:
    """Construit les en-tetes HTTP pour une page d'offres."""
    return {"Authorization": f"Bearer {token}", "Range": f"offres={range_start}-{range_end}"}


def fetch_offres_page(token: str, range_start: int, range_end: int, mots_cles: str) -> dict:
    """Recupere une page d'offres via l'API France Travail."""
    response = requests.get(
        OFFRES_SEARCH_URL,
        headers=build_headers(token, range_start, range_end),
        params={"motsCles": mots_cles},
        timeout=REQUEST_TIMEOUT_SECONDS,
    )
    response.raise_for_status()
    return response.json()


def fetch_offres_api(mots_cles: str, max_results: int) -> list[dict]:
    """Recupere les offres via pagination API."""
    token = get_access_token()
    all_offers: list[dict] = []
    range_start = 0

    while range_start < max_results:
        range_end = min(range_start + PAGE_SIZE - 1, range_start + 149)
        print(f"Range {range_start}-{range_end}...", end=" ")
        try:
            payload = fetch_offres_page(token, range_start, range_end, mots_cles)
        except requests.HTTPError as e:
            if e.response is not None and e.response.status_code == 416:
                print("fin (416).")
                break
            raise

        offers = payload.get("resultats") or payload.get("offres") or []
        if not offers:
            print("0 resultat, fin.")
            break

        all_offers.extend(offers)
        print(f"{len(offers)} offres (total: {len(all_offers)})")

        if len(offers) < PAGE_SIZE:
            break
        range_start = range_end + 1
        if range_start > MAX_OFFSET:
            break
        time.sleep(DELAY_BETWEEN_REQUESTS_SEC)

    return all_offers


def fetch_adzuna_api(mots_cles: str, max_results: int) -> list[dict]:
    """Recupere les offres Adzuna via pagination API."""
    app_id = (os.environ.get("ADZUNA_APP_ID") or "").strip()
    app_key = (os.environ.get("ADZUNA_APP_KEY") or "").strip()
    if not app_id or not app_key:
        raise ValueError("ADZUNA_APP_ID et ADZUNA_APP_KEY doivent etre definis.")

    offers: list[dict] = []
    page = 1
    while len(offers) < max_results:
        params = {
            "app_id": app_id,
            "app_key": app_key,
            "results_per_page": ADZUNA_PAGE_SIZE,
            "what": mots_cles,
            "content-type": "application/json",
        }
        response = requests.get(
            ADZUNA_SEARCH_URL_TEMPLATE.format(page=page),
            params=params,
            timeout=REQUEST_TIMEOUT_SECONDS,
        )
        response.raise_for_status()
        payload = response.json()
        page_results = payload.get("results") or []
        if not page_results:
            break
        offers.extend(page_results)
        print(f"Adzuna page {page}: {len(page_results)} offres (total: {len(offers)})")
        if len(page_results) < ADZUNA_PAGE_SIZE:
            break
        page += 1
        time.sleep(DELAY_BETWEEN_REQUESTS_SEC)
    return offers[:max_results]


def extract_company_name(offre: dict) -> str | None:
    """Extrait le nom d'entreprise depuis differents formats d'offres."""
    company = offre.get("company")
    if isinstance(company, dict) and company.get("display_name"):
        return str(company["display_name"]).strip()
    entreprise = offre.get("entreprise")
    if isinstance(entreprise, dict) and entreprise.get("nom"):
        return str(entreprise["nom"]).strip()
    return None


def extract_location_name(offre: dict) -> str | None:
    """Extrait le lieu depuis differents formats d'offres."""
    location = offre.get("location")
    if isinstance(location, dict) and location.get("display_name"):
        return str(location["display_name"]).strip()
    lieu = offre.get("lieuTravail")
    if isinstance(lieu, dict) and lieu.get("libelle"):
        return str(lieu["libelle"]).strip()
    return None


def extract_code_naf(offre: dict) -> str | None:
    """Extrait le code NAF de l'offre si present."""
    value = offre.get("codeNAF") or offre.get("code_naf")
    if value:
        return str(value).strip()
    return None


def extract_intitule(offre: dict) -> str:
    value = offre.get("intitule")
    return str(value).strip() if value else ""


def extract_rome_code(offre: dict) -> str | None:
    value = offre.get("romeCode") or offre.get("rome_code")
    return str(value).strip() if value else None


def extract_adzuna_offer_id(offre: dict) -> str:
    """Extrait l'identifiant d'une offre Adzuna."""
    value = offre.get("id")
    return str(value).strip() if value else f"adzuna_generated_{uuid4().hex}"


def extract_adzuna_company_name(offre: dict) -> str | None:
    company = offre.get("company")
    if isinstance(company, dict):
        value = company.get("display_name")
        if value:
            return str(value).strip()
    return None


def extract_adzuna_location_name(offre: dict) -> str | None:
    location = offre.get("location")
    if isinstance(location, dict):
        value = location.get("display_name")
        if value:
            return str(value).strip()
    return None


def extract_adzuna_intitule(offre: dict) -> str:
    value = offre.get("title")
    return str(value).strip() if value else ""


def is_data_engineer_offer(offre: dict, source_name: str) -> bool:
    """
    Filtre de securite: garde seulement les offres Data Engineer.

    L'API est deja appelee avec mots-cles, mais ce filtre evite les derives.
    """
    if source_name == "adzuna":
        intitule = extract_adzuna_intitule(offre).lower()
    else:
        intitule = extract_intitule(offre).lower()
    description = str(offre.get("description") or "").lower()
    haystack = f"{intitule} {description}"
    return (
        "data engineer" in haystack
        or "ingenieur data" in haystack
        or "ingénieur data" in haystack
    )


def upsert_france_travail_offers(conn: sqlite3.Connection, offers: list[dict], run_ts: str) -> tuple[int, int]:
    """Upsert les offres France Travail."""
    inserted = 0
    seen_offer_ids: set[str] = set()
    for offre in offers:
        source_offer_id = str(offre.get("id") or f"generated_{uuid4().hex}")
        offer_id = source_offer_id
        seen_offer_ids.add(offer_id)
        conn.execute(
            """
            INSERT INTO raw_offres_team
            (offer_id, source_offer_id, source_name, is_france_travail, is_adzuna,
             company_name, location_name, intitule, rome_code, code_naf_offre, payload_json, last_seen_at)
            VALUES (?, ?, 'france_travail', 1, 0, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(offer_id) DO UPDATE SET
                source_offer_id = COALESCE(NULLIF(excluded.source_offer_id, ''), raw_offres_team.source_offer_id),
                source_name = 'france_travail',
                is_france_travail = 1,
                is_adzuna = 0,
                company_name = COALESCE(NULLIF(excluded.company_name, ''), raw_offres_team.company_name),
                location_name = COALESCE(NULLIF(excluded.location_name, ''), raw_offres_team.location_name),
                intitule = COALESCE(NULLIF(excluded.intitule, ''), raw_offres_team.intitule),
                rome_code = COALESCE(NULLIF(excluded.rome_code, ''), raw_offres_team.rome_code),
                code_naf_offre = COALESCE(NULLIF(excluded.code_naf_offre, ''), raw_offres_team.code_naf_offre),
                payload_json = excluded.payload_json,
                last_seen_at = excluded.last_seen_at
            """,
            (
                offer_id,
                source_offer_id,
                extract_company_name(offre),
                extract_location_name(offre),
                extract_intitule(offre) or None,
                extract_rome_code(offre),
                extract_code_naf(offre),
                json.dumps(offre, ensure_ascii=False),
                run_ts,
            ),
        )
        inserted += 1
    return inserted, len(seen_offer_ids)


def upsert_adzuna_offers(conn: sqlite3.Connection, offers: list[dict], run_ts: str) -> tuple[int, int]:
    """Upsert les offres Adzuna."""
    inserted = 0
    seen_offer_ids: set[str] = set()
    for offre in offers:
        source_offer_id = extract_adzuna_offer_id(offre)
        offer_id = f"adzuna_{source_offer_id}"
        seen_offer_ids.add(offer_id)
        conn.execute(
            """
            INSERT INTO raw_offres_team
            (offer_id, source_offer_id, source_name, is_france_travail, is_adzuna,
             company_name, location_name, intitule, rome_code, code_naf_offre, payload_json, last_seen_at)
            VALUES (?, ?, 'adzuna', 0, 1, ?, ?, ?, NULL, NULL, ?, ?)
            ON CONFLICT(offer_id) DO UPDATE SET
                source_offer_id = COALESCE(NULLIF(excluded.source_offer_id, ''), raw_offres_team.source_offer_id),
                source_name = 'adzuna',
                is_france_travail = 0,
                is_adzuna = 1,
                company_name = COALESCE(NULLIF(excluded.company_name, ''), raw_offres_team.company_name),
                location_name = COALESCE(NULLIF(excluded.location_name, ''), raw_offres_team.location_name),
                intitule = COALESCE(NULLIF(excluded.intitule, ''), raw_offres_team.intitule),
                payload_json = excluded.payload_json,
                last_seen_at = excluded.last_seen_at
            """,
            (
                offer_id,
                source_offer_id,
                extract_adzuna_company_name(offre),
                extract_adzuna_location_name(offre),
                extract_adzuna_intitule(offre) or None,
                json.dumps(offre, ensure_ascii=False),
                run_ts,
            ),
        )
        inserted += 1
    return inserted, len(seen_offer_ids)


def ingest(mots_cles: str, max_results: int) -> tuple[int, int, int, str, int, int]:
    """Charge les offres France Travail + Adzuna dans raw_offres_team."""
    init_db()
    run_ts = datetime.now(timezone.utc).isoformat(timespec="seconds")

    ft_offers = fetch_offres_api(mots_cles, max_results)
    ft_filtered = [offer for offer in ft_offers if is_data_engineer_offer(offer, "france_travail")]
    print(f"France Travail: {len(ft_filtered)}/{len(ft_offers)} offres gardees")

    adzuna_offers = fetch_adzuna_api(mots_cles, max_results)
    adzuna_filtered = [offer for offer in adzuna_offers if is_data_engineer_offer(offer, "adzuna")]
    print(f"Adzuna: {len(adzuna_filtered)}/{len(adzuna_offers)} offres gardees")

    conn = sqlite3.connect(DB_PATH)
    ft_inserted, ft_unique = upsert_france_travail_offers(conn, ft_filtered, run_ts)
    adzuna_inserted, adzuna_unique = upsert_adzuna_offers(conn, adzuna_filtered, run_ts)
    db_count = conn.execute("SELECT COUNT(*) FROM raw_offres_team").fetchone()[0]
    conn.commit()
    conn.close()
    return ft_inserted + adzuna_inserted, ft_unique + adzuna_unique, db_count, run_ts, ft_inserted, adzuna_inserted


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ingestion raw des offres API dans team_working")
    parser.add_argument("--mots-cles", default=DEFAULT_MOTS_CLES, help="Mots-cles de recherche API.")
    parser.add_argument("--max-results", type=int, default=1150, help="Nombre max d'offres a recuperer.")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    fetched_count, unique_payload_ids, db_count, run_ts, ft_count, adzuna_count = ingest(
        mots_cles=args.mots_cles, max_results=args.max_results
    )
    print(f"run_ts: {run_ts}")
    print(f"{fetched_count} lignes recuperees depuis API (FT + Adzuna)")
    print(f"France Travail: {ft_count} lignes")
    print(f"Adzuna: {adzuna_count} lignes")
    print(f"{unique_payload_ids} ids offre uniques dans ce run")
    print(f"{db_count} offres uniques actuellement en base ({DB_PATH})")
