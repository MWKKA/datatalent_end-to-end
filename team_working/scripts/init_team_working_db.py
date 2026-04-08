"""Initialise la base SQLite dediee team_working."""

from __future__ import annotations

import sqlite3
from pathlib import Path

DB_PATH = Path(__file__).resolve().parents[1] / "db" / "team_working.db"


def ensure_column(conn: sqlite3.Connection, table_name: str, column_name: str, column_sql: str) -> None:
    """Ajoute une colonne si elle n'existe pas encore."""
    existing = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    existing_names = {row[1] for row in existing}
    if column_name not in existing_names:
        conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_sql}")


def init_db() -> None:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS raw_offres_team (
            offer_id TEXT PRIMARY KEY,
            source_offer_id TEXT,
            source_name TEXT,
            is_france_travail INTEGER DEFAULT 0,
            is_adzuna INTEGER DEFAULT 0,
            company_name TEXT,
            location_name TEXT,
            intitule TEXT,
            rome_code TEXT,
            code_naf_offre TEXT,
            payload_json TEXT NOT NULL,
            ingested_at TEXT DEFAULT CURRENT_TIMESTAMP,
            last_seen_at TEXT DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS raw_sirene_results_team (
            company_name TEXT PRIMARY KEY,
            sirene_name TEXT,
            siren TEXT,
            siret TEXT,
            is_headquarter INTEGER,
            payload_json TEXT NOT NULL,
            fetched_at TEXT DEFAULT CURRENT_TIMESTAMP,
            last_seen_at TEXT DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS offres_enriched_team (
            offer_id TEXT PRIMARY KEY,
            company_name TEXT,
            sirene_matched INTEGER NOT NULL,
            match_method TEXT,
            match_score INTEGER,
            naf_match INTEGER,
            code_naf_offre TEXT,
            code_naf_sirene TEXT,
            sirene_name TEXT,
            siren TEXT,
            siret TEXT,
            sirene_payload_json TEXT,
            enriched_at TEXT DEFAULT CURRENT_TIMESTAMP,
            last_seen_at TEXT DEFAULT CURRENT_TIMESTAMP
        );
        """
    )
    # Evolution schema pour bases deja creees
    ensure_column(conn, "raw_offres_team", "intitule", "intitule TEXT")
    ensure_column(conn, "raw_offres_team", "rome_code", "rome_code TEXT")
    ensure_column(conn, "raw_offres_team", "code_naf_offre", "code_naf_offre TEXT")
    ensure_column(conn, "raw_offres_team", "source_offer_id", "source_offer_id TEXT")
    ensure_column(conn, "raw_offres_team", "source_name", "source_name TEXT")
    ensure_column(conn, "raw_offres_team", "is_france_travail", "is_france_travail INTEGER DEFAULT 0")
    ensure_column(conn, "raw_offres_team", "is_adzuna", "is_adzuna INTEGER DEFAULT 0")
    ensure_column(conn, "raw_offres_team", "last_seen_at", "last_seen_at TEXT")

    ensure_column(conn, "offres_enriched_team", "match_score", "match_score INTEGER")
    ensure_column(conn, "offres_enriched_team", "naf_match", "naf_match INTEGER")
    ensure_column(conn, "offres_enriched_team", "code_naf_offre", "code_naf_offre TEXT")
    ensure_column(conn, "offres_enriched_team", "code_naf_sirene", "code_naf_sirene TEXT")
    ensure_column(conn, "offres_enriched_team", "last_seen_at", "last_seen_at TEXT")
    ensure_column(conn, "raw_sirene_results_team", "last_seen_at", "last_seen_at TEXT")

    # Backfill des lignes anciennes pour eviter d'exclure des offres au filtre run_ts
    conn.execute(
        """
        UPDATE raw_offres_team
        SET last_seen_at = COALESCE(last_seen_at, ingested_at, CURRENT_TIMESTAMP)
        WHERE last_seen_at IS NULL
        """
    )
    conn.execute(
        """
        UPDATE raw_offres_team
        SET source_name = COALESCE(NULLIF(source_name, ''), 'france_travail'),
            source_offer_id = COALESCE(NULLIF(source_offer_id, ''), offer_id),
            is_france_travail = CASE
                WHEN COALESCE(NULLIF(source_name, ''), 'france_travail') = 'france_travail' THEN 1
                WHEN COALESCE(NULLIF(source_name, ''), 'france_travail') = 'adzuna' THEN 0
                ELSE COALESCE(is_france_travail, 0)
            END,
            is_adzuna = CASE
                WHEN COALESCE(NULLIF(source_name, ''), 'france_travail') = 'adzuna' THEN 1
                WHEN COALESCE(NULLIF(source_name, ''), 'france_travail') = 'france_travail' THEN 0
                ELSE COALESCE(is_adzuna, 0)
            END
        WHERE source_name IS NULL
           OR source_offer_id IS NULL
           OR is_france_travail IS NULL
           OR is_adzuna IS NULL
           OR (source_name = 'france_travail' AND (is_france_travail != 1 OR is_adzuna != 0))
           OR (source_name = 'adzuna' AND (is_france_travail != 0 OR is_adzuna != 1))
        """
    )
    conn.execute(
        """
        UPDATE raw_sirene_results_team
        SET last_seen_at = COALESCE(last_seen_at, fetched_at, CURRENT_TIMESTAMP)
        WHERE last_seen_at IS NULL
        """
    )
    conn.execute(
        """
        UPDATE offres_enriched_team
        SET last_seen_at = COALESCE(last_seen_at, enriched_at, CURRENT_TIMESTAMP)
        WHERE last_seen_at IS NULL
        """
    )

    conn.commit()
    conn.close()


if __name__ == "__main__":
    init_db()
    print(f"Base initialisee: {DB_PATH}")
