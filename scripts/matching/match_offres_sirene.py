"""
Point d'entree CLI pour le matching offres -> Sirene.
"""

from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from scripts.core.common import get_database_path, normalize_spaces
from scripts.matching.service import (
    compute_match,
    create_result_table,
    ensure_support_indexes,
    fetch_offres,
    insert_result_row,
)

DB_PATH = get_database_path()


def main() -> None:
    print(f"Base utilisee : {DB_PATH}")
    if not DB_PATH.exists():
        print(f"Base introuvable : {DB_PATH}")
        sys.exit(1)

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    print("Verification de l'index sur denominationUniteLegale...")
    ensure_support_indexes(conn)
    print("Index OK.")
    offres = fetch_offres(conn)
    create_result_table(conn)

    matched = 0
    n_offres = len(offres)
    for i, row in enumerate(offres):
        offre_id = row["id"]
        nom = normalize_spaces(row["entreprise_nom"] or "")
        code_naf_offre = (row["code_naf"] or "").strip() or None
        match = compute_match(conn, nom, code_naf_offre)
        if match.siren:
            matched += 1
        insert_result_row(conn, offre_id=offre_id, entreprise_nom=nom or None, result=match)
        if (i + 1) % 10 == 0:
            conn.commit()
            print(f"  Traite {i + 1}/{n_offres} offres, {matched} matchs...")

    conn.commit()
    conn.close()
    print(f"Termine. {matched}/{len(offres)} offres avec un SIREN trouve.")


if __name__ == "__main__":
    main()
    sys.exit(0)

