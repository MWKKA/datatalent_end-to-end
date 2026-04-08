"""
Recupere les offres d'emploi via l'API France Travail Offres v2.
"""

from __future__ import annotations

import argparse
import json
import time
from pathlib import Path
import sys

import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from scripts.core.common import get_data_raw_dir, load_project_env, slugify
from scripts.ingestion.france_travail_auth import get_access_token

load_project_env()

OFFRES_SEARCH_URL = "https://api.francetravail.io/partenaire/offresdemploi/v2/offres/search"
MOTS_CLES_DATA = "data engineer"
PAGE_SIZE = 150
MAX_OFFSET = 1000
OUT_DIR = get_data_raw_dir() / "france_travail"
DELAY_BETWEEN_REQUESTS_SEC = 0.2
REQUEST_TIMEOUT_SECONDS = 30


def build_headers(token: str, range_start: int, range_end: int) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}", "Range": f"offres={range_start}-{range_end}"}


def extract_results(payload: dict) -> list[dict]:
    return payload.get("resultats") or payload.get("offres") or []


def fetch_offres_page(token: str, range_start: int, range_end: int, mots_cles: str = MOTS_CLES_DATA) -> dict:
    resp = requests.get(
        OFFRES_SEARCH_URL,
        headers=build_headers(token, range_start, range_end),
        params={"motsCles": mots_cles},
        timeout=REQUEST_TIMEOUT_SECONDS,
    )
    resp.raise_for_status()
    return resp.json()


def download_offres(mots_cles: str = MOTS_CLES_DATA, max_results: int = 1150, out_dir: Path | None = None) -> Path:
    out_dir = out_dir or OUT_DIR
    out_dir.mkdir(parents=True, exist_ok=True)

    token = get_access_token()
    all_resultats: list[dict] = []
    range_start = 0
    while range_start < max_results:
        range_end = min(range_start + PAGE_SIZE - 1, range_start + 149)
        print(f"  Range {range_start}-{range_end}...", end=" ")
        try:
            data = fetch_offres_page(token, range_start, range_end, mots_cles)
        except requests.HTTPError as e:
            if e.response is not None and e.response.status_code == 416:
                print("fin.")
                break
            raise
        resultats = extract_results(data)
        if not resultats:
            print("0 resultat, fin.")
            break
        all_resultats.extend(resultats)
        print(f"{len(resultats)} offres (total: {len(all_resultats)})")
        if len(resultats) < PAGE_SIZE:
            break
        range_start = range_end + 1
        if range_start > MAX_OFFSET:
            break
        time.sleep(DELAY_BETWEEN_REQUESTS_SEC)

    out_file = out_dir / f"offres_{slugify(mots_cles, max_length=30)}.json"
    out_file.write_text(
        json.dumps({"motsCles": mots_cles, "total": len(all_resultats), "resultats": all_resultats}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return out_file


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Telecharge les offres France Travail")
    parser.add_argument("--mots-cles", default=MOTS_CLES_DATA)
    parser.add_argument("--max-results", type=int, default=1150)
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    print(f"Recuperation des offres (mots-cles: '{args.mots_cles}')...")
    path = download_offres(mots_cles=args.mots_cles, max_results=args.max_results)
    print(f"Termine. Enregistre dans {path}")

