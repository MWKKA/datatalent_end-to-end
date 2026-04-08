"""
Service de matching entre offres France Travail et Sirene.

Ce module contient uniquement la logique metier du rapprochement.
Le script CLI `match_offres_sirene.py` reste un point d'entree orchestration.
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass

try:
    from scripts.core.common import normalize_spaces
except ModuleNotFoundError:  # execution directe: python scripts/...
    from core.common import normalize_spaces


FORMES_JURIDIQUES = (
    " s.a.s.",
    " sas",
    " s.a.",
    " sa",
    " s.e.",
    " se",
    " sarl",
    " eurl",
    " sci",
    " sasu",
    " sarlu",
    " snc",
    " scs",
    " gie",
    " association",
    " s.c.i.",
    " s.c.o.p.",
    " s.e.l.",
    " s.e.l.a.r.l.",
)


@dataclass(frozen=True)
class MatchResult:
    """Resultat de matching pour une offre."""

    siren: str | None
    denomination_sirene: str | None
    match_naf: int


def normalize_company_name(name: str) -> str:
    """Normalise un nom d'entreprise pour les comparaisons."""
    if not name:
        return ""
    return normalize_spaces(name).lower()


def normalize_company_name_base(name: str) -> str:
    """
    Normalise un nom d'entreprise et retire la forme juridique en suffixe.

    Example:
        >>> normalize_company_name_base("Exemple SAS")
        'exemple'
    """
    normalized = normalize_company_name(name)
    for legal_form in FORMES_JURIDIQUES:
        if normalized.endswith(legal_form):
            normalized = normalized[: -len(legal_form)].strip()
    return normalize_spaces(normalized)


def ensure_support_indexes(conn: sqlite3.Connection) -> None:
    """Cree les indexes utiles pour accelerer les recherches de noms."""
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_sirene_ul_denomination "
        "ON raw_sirene_unite_legale(denominationUniteLegale)"
    )
    conn.commit()


def create_result_table(conn: sqlite3.Connection) -> None:
    """Cree (ou vide) la table de resultat offres_avec_siren."""
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS offres_avec_siren (
            offre_id TEXT PRIMARY KEY,
            entreprise_nom TEXT,
            siren TEXT,
            denomination_sirene TEXT,
            match_naf INTEGER
        )
        """
    )
    conn.execute("DELETE FROM offres_avec_siren")
    conn.commit()


def fetch_offres(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    """Lit les offres a rapprocher."""
    return conn.execute(
        """
        SELECT id, entreprise_nom, code_naf
        FROM raw_france_travail_offres
        """
    ).fetchall()


def find_exact_candidate(conn: sqlite3.Connection, normalized_name: str) -> sqlite3.Row | None:
    """Cherche une unite legale avec un nom exactement equivalent."""
    return conn.execute(
        """
        SELECT siren, denominationUniteLegale, activitePrincipaleUniteLegale
        FROM raw_sirene_unite_legale
        WHERE LOWER(TRIM(denominationUniteLegale)) = ?
        LIMIT 1
        """,
        (normalized_name,),
    ).fetchone()


def find_prefix_candidates(conn: sqlite3.Connection, base_name: str) -> list[sqlite3.Row]:
    """Cherche des unites legales dont la denomination commence par base_name."""
    if not base_name:
        return []
    return conn.execute(
        """
        SELECT siren, denominationUniteLegale, activitePrincipaleUniteLegale
        FROM raw_sirene_unite_legale
        WHERE denominationUniteLegale IS NOT NULL
          AND denominationUniteLegale != ''
          AND LOWER(TRIM(denominationUniteLegale)) LIKE ?
        LIMIT 5
        """,
        (base_name + "%",),
    ).fetchall()


def choose_best_candidate(
    candidates: list[sqlite3.Row], offre_naf_code: str | None
) -> sqlite3.Row | None:
    """Choisit le meilleur candidat, en priorisant le NAF quand possible."""
    if not candidates:
        return None
    if offre_naf_code:
        for candidate in candidates:
            candidate_naf = (candidate["activitePrincipaleUniteLegale"] or "").strip()
            if candidate_naf == offre_naf_code:
                return candidate
    return candidates[0]


def compute_match(
    conn: sqlite3.Connection,
    entreprise_nom: str,
    offre_naf_code: str | None,
) -> MatchResult:
    """
    Retourne un resultat de matching pour une offre.

    Strategie:
    1) match exact sur nom normalise
    2) fallback prefixe sur nom sans forme juridique
    """
    if not entreprise_nom:
        return MatchResult(siren=None, denomination_sirene=None, match_naf=0)

    normalized_name = normalize_company_name(entreprise_nom)
    base_name = normalize_company_name_base(entreprise_nom)

    candidate = find_exact_candidate(conn, normalized_name)
    if not candidate:
        candidates = find_prefix_candidates(conn, base_name)
        candidate = choose_best_candidate(candidates, offre_naf_code)

    if not candidate:
        return MatchResult(siren=None, denomination_sirene=None, match_naf=0)

    candidate_naf = (candidate["activitePrincipaleUniteLegale"] or "").strip() or None
    naf_match = int(bool(offre_naf_code and candidate_naf and offre_naf_code == candidate_naf))

    return MatchResult(
        siren=candidate["siren"],
        denomination_sirene=candidate["denominationUniteLegale"],
        match_naf=naf_match,
    )


def insert_result_row(
    conn: sqlite3.Connection,
    offre_id: str,
    entreprise_nom: str | None,
    result: MatchResult,
) -> None:
    """Insere un resultat de matching dans offres_avec_siren."""
    conn.execute(
        """
        INSERT INTO offres_avec_siren (offre_id, entreprise_nom, siren, denomination_sirene, match_naf)
        VALUES (?, ?, ?, ?, ?)
        """,
        (offre_id, entreprise_nom, result.siren, result.denomination_sirene, result.match_naf),
    )

