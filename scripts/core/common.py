"""Fonctions communes pour les scripts d'ingestion."""

from __future__ import annotations

from pathlib import Path

from dotenv import load_dotenv


def load_project_env() -> None:
    """Charge les variables d'environnement du projet depuis `.env`."""
    repo_root = Path(__file__).resolve().parents[2]
    env_path = repo_root / ".env"
    load_dotenv(env_path)
