"""
Telecharge les referentiels de l'API Geo et les enregistre en JSON.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from scripts.core.common import get_data_raw_dir

BASE_URL = "https://geo.api.gouv.fr"
OUT_DIR = get_data_raw_dir() / "geo"
REQUEST_TIMEOUT_SECONDS = 30


def fetch_json(url: str) -> list | dict:
    response = requests.get(url, timeout=REQUEST_TIMEOUT_SECONDS)
    response.raise_for_status()
    return response.json()


def save_json_file(path: Path, payload: list | dict) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def download_resource(resource_name: str, endpoint: str, out_file: Path) -> None:
    records = fetch_json(f"{BASE_URL}{endpoint}")
    save_json_file(out_file, records)
    print(f"  -> {len(records)} {resource_name} -> {out_file}")


def download_geo_referentiels(*, include_communes: bool = True) -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    resources: list[tuple[str, str, str]] = [
        ("regions", "/regions", "regions.json"),
        ("departements", "/departements", "departements.json"),
    ]
    if include_communes:
        resources.append(
            (
                "communes",
                "/communes?fields=code,nom,departement,region,codesPostaux,population",
                "communes.json",
            )
        )
    for resource_name, endpoint, file_name in resources:
        download_resource(resource_name, endpoint, OUT_DIR / file_name)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Telecharge les referentiels API Geo")
    parser.add_argument("--without-communes", action="store_true")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    print("Telechargement des referentiels API Geo...")
    download_geo_referentiels(include_communes=not args.without_communes)
    print("Termine.")

