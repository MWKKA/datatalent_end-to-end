"""
Charge les donnees raw (Geo, offres France Travail, Sirene) dans SQLite.
"""

from __future__ import annotations

import json
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from scripts.core.common import get_data_raw_dir, get_database_path, get_project_root, load_project_env

PROJECT_ROOT = get_project_root()
load_project_env()
DATA_RAW = get_data_raw_dir()
DB_PATH = get_database_path()


def ensure_data_dir() -> None:
    default_path = PROJECT_ROOT / "data" / "local.db"
    if DB_PATH == default_path:
        default_path.parent.mkdir(parents=True, exist_ok=True)


def load_geo(conn: sqlite3.Connection, *, load_communes: bool = False) -> None:
    regions_path = DATA_RAW / "geo" / "regions.json"
    if regions_path.exists():
        regions = json.loads(regions_path.read_text(encoding="utf-8"))
        conn.executemany("INSERT OR REPLACE INTO raw_geo_regions (code, nom) VALUES (?, ?)", [(r["code"], r["nom"]) for r in regions])
        print(f"  -> {len(regions)} regions")

    depts_path = DATA_RAW / "geo" / "departements.json"
    if depts_path.exists():
        depts = json.loads(depts_path.read_text(encoding="utf-8"))
        conn.executemany(
            "INSERT OR REPLACE INTO raw_geo_departements (code, nom, code_region) VALUES (?, ?, ?)",
            [(d["code"], d["nom"], d.get("codeRegion") or "") for d in depts],
        )
        print(f"  -> {len(depts)} departements")

    communes_path = DATA_RAW / "geo" / "communes.json"
    if communes_path.exists() and load_communes:
        communes = json.loads(communes_path.read_text(encoding="utf-8"))
        conn.executemany(
            """INSERT OR REPLACE INTO raw_geo_communes
               (code, nom, code_departement, code_region, codes_postaux, population)
               VALUES (?, ?, ?, ?, ?, ?)""",
            [
                (
                    c["code"],
                    c["nom"],
                    c.get("departement", {}).get("code", ""),
                    c.get("region", {}).get("code", ""),
                    json.dumps(c.get("codesPostaux") or []),
                    c.get("population") or 0,
                )
                for c in communes
            ],
        )
        print(f"  -> {len(communes)} communes")
    elif communes_path.exists():
        print("  -> communes ignorees (load_communes=False)")


def load_offres(conn: sqlite3.Connection, max_offres: int | None = None) -> None:
    ft_dir = DATA_RAW / "france_travail"
    json_files = list(ft_dir.glob("offres_*.json")) if ft_dir.exists() else []
    if not json_files:
        print("  -> aucun offres_*.json trouve")
        return

    data = json.loads(json_files[0].read_text(encoding="utf-8"))
    resultats = data.get("resultats") or []
    if max_offres is not None:
        resultats = resultats[:max_offres]
        print(f"  (limite a {max_offres} offres)")
    if not resultats:
        print("  -> 0 offre")
        return

    def row(offre: dict) -> tuple:
        lieu = offre.get("lieuTravail") or {}
        entreprise = offre.get("entreprise") or {}
        salaire = offre.get("salaire") or {}
        return (
            offre.get("id"),
            offre.get("intitule"),
            offre.get("romeCode"),
            offre.get("romeLibelle"),
            offre.get("typeContrat"),
            offre.get("dateCreation"),
            lieu.get("libelle"),
            lieu.get("codePostal"),
            lieu.get("commune"),
            entreprise.get("nom"),
            salaire.get("libelle"),
            offre.get("codeNAF"),
            offre.get("secteurActiviteLibelle"),
            offre.get("description", "")[:5000],
        )

    conn.executemany(
        """INSERT OR REPLACE INTO raw_france_travail_offres
           (id, intitule, rome_code, rome_libelle, type_contrat, date_creation,
            lieu_libelle, code_postal, commune_code, entreprise_nom, salaire_libelle,
            code_naf, secteur_activite_libelle, description_extrait)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        [row(o) for o in resultats],
    )
    print(f"  -> {len(resultats)} offres (depuis {json_files[0].name})")


def _load_parquet_by_row_groups(conn: sqlite3.Connection, path: Path, table_name: str) -> int:
    import pyarrow.parquet as pq

    pf = pq.ParquetFile(path)
    total = 0
    for i in range(pf.num_row_groups):
        df = pf.read_row_group(i).to_pandas()
        df.to_sql(table_name, conn, if_exists="replace" if i == 0 else "append", index=False)
        total += len(df)
    return total


def load_sirene_if_present(conn: sqlite3.Connection) -> None:
    sirene_dir = DATA_RAW / "sirene"
    if not sirene_dir.exists():
        return
    try:
        import pyarrow.parquet as pq  # noqa: F401
    except ImportError:
        print("  -> pyarrow requis pour Sirene")
        return

    etab_files = [f for f in sorted(sirene_dir.glob("*Etablissement*.parquet")) if not f.name.endswith(":Zone.Identifier")]
    if etab_files:
        n = _load_parquet_by_row_groups(conn, etab_files[0], "raw_sirene_etablissement")
        print(f"  -> {n} etablissements")

    ul_files = [f for f in sorted(sirene_dir.glob("*UniteLegale*.parquet")) if not f.name.endswith(":Zone.Identifier")]
    if ul_files:
        n = _load_parquet_by_row_groups(conn, ul_files[0], "raw_sirene_unite_legale")
        print(f"  -> {n} unites legales")


def create_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS raw_geo_regions (code TEXT PRIMARY KEY, nom TEXT);
        CREATE TABLE IF NOT EXISTS raw_geo_departements (code TEXT PRIMARY KEY, nom TEXT, code_region TEXT);
        CREATE TABLE IF NOT EXISTS raw_geo_communes (
            code TEXT PRIMARY KEY, nom TEXT, code_departement TEXT, code_region TEXT, codes_postaux TEXT, population INTEGER
        );
        CREATE TABLE IF NOT EXISTS raw_france_travail_offres (
            id TEXT PRIMARY KEY, intitule TEXT, rome_code TEXT, rome_libelle TEXT, type_contrat TEXT, date_creation TEXT,
            lieu_libelle TEXT, code_postal TEXT, commune_code TEXT, entreprise_nom TEXT, salaire_libelle TEXT,
            code_naf TEXT, secteur_activite_libelle TEXT, description_extrait TEXT
        );
        """
    )
    conn.commit()


def main(*, load_communes: bool = False, geo_only: bool = False, offres_only: bool = False, sirene_only: bool = False, max_offres: int | None = None) -> None:
    ensure_data_dir()
    print(f"Base SQLite : {DB_PATH}")
    conn = sqlite3.connect(DB_PATH)
    try:
        create_schema(conn)
        if sirene_only:
            load_sirene_if_present(conn)
        else:
            if not offres_only:
                load_geo(conn, load_communes=load_communes)
            if not geo_only:
                load_offres(conn, max_offres=max_offres)
            load_sirene_if_present(conn)
        conn.commit()
    finally:
        conn.close()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Charge les donnees raw dans data/local.db")
    parser.add_argument("--communes", action="store_true")
    parser.add_argument("--geo-only", action="store_true")
    parser.add_argument("--offres-only", action="store_true")
    parser.add_argument("--sirene-only", action="store_true")
    parser.add_argument("--max-offres", type=int, default=None, metavar="N")
    args = parser.parse_args()
    main(
        load_communes=args.communes,
        geo_only=args.geo_only,
        offres_only=args.offres_only,
        sirene_only=args.sirene_only,
        max_offres=args.max_offres,
    )
    sys.exit(0)

