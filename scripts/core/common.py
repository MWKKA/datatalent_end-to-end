"""
Utilitaires partages pour les scripts du projet.
"""

from __future__ import annotations

import os
import re
from pathlib import Path

from dotenv import load_dotenv


def get_project_root() -> Path:
    """Retourne la racine du projet."""
    return Path(__file__).resolve().parents[2]


def load_project_env() -> None:
    """Charge le .env de la racine projet."""
    load_dotenv(get_project_root() / ".env")


def get_data_raw_dir() -> Path:
    """Retourne le dossier data/raw."""
    return get_project_root() / "data" / "raw"


def get_database_path() -> Path:
    """Retourne le chemin de base SQLite (env puis defaut)."""
    db_path = os.environ.get("DATABASE_PATH")
    if db_path:
        return Path(db_path)
    return get_project_root() / "data" / "local.db"


def normalize_spaces(value: str) -> str:
    """Nettoie les espaces (trim + collapse)."""
    return " ".join(value.split()).strip()


def slugify(value: str, max_length: int = 40) -> str:
    """Construit un slug simple."""
    cleaned = normalize_spaces(value).lower()
    cleaned = re.sub(r"[^a-z0-9]+", "_", cleaned)
    cleaned = re.sub(r"_+", "_", cleaned).strip("_")
    return cleaned[:max_length] or "export"

