"""Enrichit les offres team_working via API Sirene (approche equipe)."""

from __future__ import annotations

import json
import sqlite3
import time
import re
import unicodedata
from pathlib import Path
import argparse
from datetime import datetime, timezone

import requests
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from team_working.scripts.init_team_working_db import init_db  # noqa: E402

DB_PATH = Path(__file__).resolve().parents[1] / "db" / "team_working.db"

# Conserve volontairement la cle en dur (demande explicite projet scolaire)
API_KEY = ""
BASE_URL = "https://api.insee.fr/api-sirene/3.11"
DEFAULT_BATCH_SIZE = 20
FORMES_JURIDIQUES = (
    " SASU",
    " SAS",
    " SARL",
    " SA",
    " EURL",
    " SCI",
    " SNC",
    " SE",
    " GIE",
)

# Variantes ciblees pour quelques no-match frequents du run courant.
TARGETED_QUERY_ALIASES: dict[str, list[str]] = {
    "ADEQUAT INTERIM ET RECRUTEMENT": ["ADEQUAT", "ADEQUAT INTERIM"],
    "CEDEO CLIM CDL ELEC DISPART ENV": ["CEDEO", "CLIM", "DISPART"],
    "EUROTUNNEL SERVICES": ["EUROTUNNEL"],
}


def clean_company_for_matching(value: str | None) -> str:
    """Nettoie des motifs frequents bruyants avant normalisation."""
    if not value:
        return ""
    text = value
    # Ex: "3.EUROTUNNEL ..." -> "EUROTUNNEL ..."
    text = re.sub(r"^\s*\d+\s*[.\-]\s*", "", text)
    # Ex: "A - B - C" -> "A B C"
    text = text.replace("-", " ")
    text = re.sub(r"\s+", " ", text).strip()
    return text


def normalize_text(value: str | None) -> str:
    """Normalise un texte: uppercase, sans accents, sans ponctuation."""
    if not value:
        return ""
    text = unicodedata.normalize("NFKD", value)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = text.upper()
    text = re.sub(r"[^A-Z0-9 ]+", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def normalize_company_name(value: str | None) -> str:
    """Normalise un nom entreprise et retire les formes juridiques en suffixe."""
    text = normalize_text(clean_company_for_matching(value))
    for forme in FORMES_JURIDIQUES:
        if text.endswith(forme):
            text = text[: -len(forme)].strip()
    return text


def normalize_company_name_alpha_only(value: str | None) -> str:
    """Version normalisee sans chiffres (utile pour certains faux negatifs)."""
    text = normalize_company_name(value)
    text = re.sub(r"[0-9]+", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def compact_text(value: str | None) -> str:
    """Retire les espaces pour comparer des variantes COLLEES."""
    if not value:
        return ""
    return re.sub(r"\s+", "", value)


def normalize_naf(value: str | None) -> str:
    """Normalise un code NAF (garde alphanumerique, uppercase)."""
    if not value:
        return ""
    return re.sub(r"[^A-Z0-9]", "", value.upper())


def extract_naf_from_etab(etab: dict) -> str | None:
    """Extrait le NAF depuis le meilleur champ disponible dans la reponse Sirene."""
    naf_direct = etab.get("activitePrincipaleEtablissement")
    if naf_direct:
        return str(naf_direct)
    naf_unite_legale = (etab.get("uniteLegale") or {}).get("activitePrincipaleUniteLegale")
    if naf_unite_legale:
        return str(naf_unite_legale)
    periodes = etab.get("periodesEtablissement")
    if isinstance(periodes, list) and periodes:
        for periode in periodes:
            naf = (periode or {}).get("activitePrincipaleEtablissement")
            if naf:
                return str(naf)
    return None


def score_match(company_name: str, sirene_name: str, code_naf_offre: str | None, code_naf_sirene: str | None) -> tuple[int, str, int]:
    """
    Calcule un score de matching.

    Returns:
        (score, method, naf_match)
    """
    company_norm = normalize_company_name(company_name)
    sirene_norm = normalize_company_name(sirene_name)
    company_norm_alpha = normalize_company_name_alpha_only(company_name)
    sirene_norm_alpha = normalize_company_name_alpha_only(sirene_name)
    if not company_norm or not sirene_norm:
        return 0, "no_match", 0

    score = 0
    method = "no_match"
    if company_norm == sirene_norm:
        score = 100
        method = "exact_normalized"
    elif compact_text(company_norm) == compact_text(sirene_norm):
        score = 98
        method = "exact_compact"
    elif company_norm_alpha and sirene_norm_alpha and company_norm_alpha == sirene_norm_alpha:
        score = 96
        method = "exact_alpha_only"
    elif sirene_norm.startswith(company_norm) or company_norm.startswith(sirene_norm):
        score = 90
        method = "prefix_normalized"
    elif compact_text(sirene_norm).startswith(compact_text(company_norm)) or compact_text(company_norm).startswith(compact_text(sirene_norm)):
        score = 88
        method = "prefix_compact"
    elif company_norm in sirene_norm or sirene_norm in company_norm:
        score = 70
        method = "contains_normalized"
    elif company_norm_alpha and sirene_norm_alpha and (
        company_norm_alpha in sirene_norm_alpha or sirene_norm_alpha in company_norm_alpha
    ):
        score = 72
        method = "contains_alpha_only"

    naf_offre = normalize_naf(code_naf_offre)
    naf_sirene = normalize_naf(code_naf_sirene)
    naf_match = int(bool(naf_offre and naf_sirene and naf_offre == naf_sirene))
    if naf_match:
        score += 10
        if method == "contains_normalized":
            method = "contains_normalized_naf"
        elif method == "prefix_normalized":
            method = "prefix_normalized_naf"
        elif method == "exact_normalized":
            method = "exact_normalized_naf"
    return score, method, naf_match


def load_unique_company_names(conn: sqlite3.Connection) -> list[str]:
    rows = conn.execute(
        """
        SELECT DISTINCT company_name
        FROM raw_offres_team
        WHERE company_name IS NOT NULL AND TRIM(company_name) <> ''
        ORDER BY company_name
        """
    ).fetchall()
    return [r[0] for r in rows]

def load_unique_company_names_for_run(conn: sqlite3.Connection, run_ts: str) -> list[str]:
    """Retourne les entreprises uniques vues dans un run donne."""
    rows = conn.execute(
        """
        SELECT DISTINCT company_name
        FROM raw_offres_team
        WHERE company_name IS NOT NULL
          AND TRIM(company_name) <> ''
          AND last_seen_at = ?
        ORDER BY company_name
        """,
        (run_ts,),
    ).fetchall()
    return [r[0] for r in rows]


def load_unique_company_names_all(conn: sqlite3.Connection) -> list[str]:
    """Retourne toutes les entreprises uniques presentes en base."""
    rows = conn.execute(
        """
        SELECT DISTINCT company_name
        FROM raw_offres_team
        WHERE company_name IS NOT NULL
          AND TRIM(company_name) <> ''
        ORDER BY company_name
        """
    ).fetchall()
    return [r[0] for r in rows]


def _build_query_for_names(company_names: list[str]) -> str:
    """Construit la clause q avec echappement simple des guillemets."""
    expanded_names: list[str] = []
    for name in company_names:
        cleaned = clean_company_for_matching(name)
        normalized = normalize_company_name(cleaned)
        expanded_names.append(name)
        if normalized:
            expanded_names.append(normalized)
            for alias in TARGETED_QUERY_ALIASES.get(normalized, []):
                expanded_names.append(alias)

    unique_names: list[str] = []
    seen: set[str] = set()
    for item in expanded_names:
        key = normalize_text(item)
        if not key or key in seen:
            continue
        seen.add(key)
        unique_names.append(item)

    escaped = [name.replace('"', " ")[:120] for name in unique_names]
    return " OR ".join([f'denominationUniteLegale:"{name}"' for name in escaped if name])


def _search_single_batch(
    batch: list[str],
    headers: dict[str, str],
    resultats: dict[str, dict],
    batch_number: int,
) -> None:
    """Execute une recherche batch et stocke les meilleurs candidats."""
    q = _build_query_for_names(batch)
    params = {
        "q": q,
        "nombre": 1000,
        "champs": ",".join(
            [
                "siret",
                "siren",
                "nic",
                "etablissementSiege",
                "etatAdministratifEtablissement",
                "caractereEmployeurEtablissement",
                "trancheEffectifsEtablissement",
                "codePostalEtablissement",
                "libelleCommuneEtablissement",
                "codeCommuneEtablissement",
                "coordonneeLambertAbscisseEtablissement",
                "coordonneeLambertOrdonneeEtablissement",
                "activitePrincipaleEtablissement",
                "periodesEtablissement",
                "denominationUniteLegale",
                "activitePrincipaleUniteLegale",
                "categorieEntreprise",
                "categorieJuridiqueUniteLegale",
                "economieSocialeSolidaireUniteLegale",
                "trancheEffectifsUniteLegale",
                "dateCreationUniteLegale",
                "etatAdministratifUniteLegale",
                "statutDiffusionUniteLegale",
            ]
        ),
    }

    response = requests.get(f"{BASE_URL}/siret", headers=headers, params=params, timeout=60)
    if response.status_code == 400:
        # Fallback robuste: certains champs ne sont pas acceptes selon endpoint/version.
        params_without_fields = {"q": q, "nombre": 1000}
        response = requests.get(f"{BASE_URL}/siret", headers=headers, params=params_without_fields, timeout=60)
    if response.status_code == 414 and len(batch) > 1:
        mid = len(batch) // 2
        print(f"  -> batch trop long (414), split en {mid} + {len(batch) - mid}")
        _search_single_batch(batch[:mid], headers, resultats, batch_number)
        _search_single_batch(batch[mid:], headers, resultats, batch_number)
        return
    if response.status_code != 200:
        print(f"Erreur batch {batch_number}: {response.status_code}")
        return

    etablissements = response.json().get("etablissements", [])
    print(f"  -> {len(etablissements)} etablissements retournes")
    for etab in etablissements:
        nom_etab = etab.get("uniteLegale", {}).get("denominationUniteLegale")
        est_siege = etab.get("etablissementSiege")
        if nom_etab and (nom_etab not in resultats or est_siege):
            resultats[nom_etab] = etab


def search_sirene_batch(company_names: list[str], batch_size: int = DEFAULT_BATCH_SIZE) -> dict[str, dict]:
    """Recherche Sirene en batch et retourne une map nom_sirene -> etablissement."""
    headers = {"X-INSEE-Api-Key-Integration": API_KEY}
    resultats: dict[str, dict] = {}

    for i in range(0, len(company_names), batch_size):
        batch = company_names[i : i + batch_size]
        print(f"Batch {i // batch_size + 1} - {len(batch)} noms")

        _search_single_batch(batch, headers, resultats, i // batch_size + 1)

        time.sleep(2)

    return resultats


def persist_raw_sirene_results(conn: sqlite3.Connection, sirene_map: dict[str, dict]) -> None:
    for sirene_name, etab in sirene_map.items():
        conn.execute(
            """
            INSERT INTO raw_sirene_results_team
            (company_name, sirene_name, siren, siret, is_headquarter, payload_json, last_seen_at)
            VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(company_name) DO UPDATE SET
                sirene_name = COALESCE(NULLIF(excluded.sirene_name, ''), raw_sirene_results_team.sirene_name),
                siren = COALESCE(NULLIF(excluded.siren, ''), raw_sirene_results_team.siren),
                siret = COALESCE(NULLIF(excluded.siret, ''), raw_sirene_results_team.siret),
                is_headquarter = MAX(raw_sirene_results_team.is_headquarter, excluded.is_headquarter),
                payload_json = excluded.payload_json,
                last_seen_at = CURRENT_TIMESTAMP
            """,
            (
                sirene_name,
                sirene_name,
                etab.get("siren"),
                etab.get("siret"),
                int(bool(etab.get("etablissementSiege"))),
                json.dumps(etab, ensure_ascii=False),
            ),
        )


def load_cached_sirene_map(conn: sqlite3.Connection) -> dict[str, dict]:
    """Charge les candidats Sirene deja persistes dans la base locale."""
    rows = conn.execute(
        """
        SELECT sirene_name, payload_json
        FROM raw_sirene_results_team
        WHERE sirene_name IS NOT NULL
          AND TRIM(sirene_name) <> ''
          AND payload_json IS NOT NULL
        """
    ).fetchall()
    cache: dict[str, dict] = {}
    for sirene_name, payload_json in rows:
        try:
            payload = json.loads(payload_json)
        except Exception:
            continue
        if isinstance(payload, dict):
            cache[sirene_name] = payload
    return cache


def find_best_match(company_name: str, code_naf_offre: str | None, sirene_map: dict[str, dict]) -> tuple[dict | None, int, str, int]:
    """Retourne le meilleur match (et son score) pour une entreprise."""
    best_match = None
    best_score = 0
    best_method = "no_match"
    best_naf_match = 0

    for nom_sirene, etab in sirene_map.items():
        code_naf_sirene = extract_naf_from_etab(etab)
        score, method, naf_match = score_match(company_name, nom_sirene, code_naf_offre, code_naf_sirene)
        if score > best_score:
            best_score = score
            best_method = method
            best_naf_match = naf_match
            best_match = etab

    # Seuil mini pour eviter les faux positifs
    if best_score < 75:
        return None, 0, "no_match", 0
    return best_match, best_score, best_method, best_naf_match


def enrich_offres(conn: sqlite3.Connection, sirene_map: dict[str, dict], run_ts: str | None) -> tuple[int, int]:
    if run_ts is None:
        offers = conn.execute(
            """
            SELECT offer_id, company_name, code_naf_offre
            FROM raw_offres_team
            ORDER BY offer_id
            """
        ).fetchall()
    else:
        offers = conn.execute(
            """
            SELECT offer_id, company_name, code_naf_offre
            FROM raw_offres_team
            WHERE last_seen_at = ?
            ORDER BY offer_id
            """,
            (run_ts,),
        ).fetchall()
    matched = 0
    for offer_id, company_name, code_naf_offre in offers:
        match, match_score, match_method, naf_match = (
            find_best_match(company_name, code_naf_offre, sirene_map) if company_name else (None, 0, "no_match", 0)
        )
        if match:
            matched += 1
            conn.execute(
                """
                INSERT INTO offres_enriched_team
                (offer_id, company_name, sirene_matched, match_method, match_score, naf_match,
                 code_naf_offre, code_naf_sirene, sirene_name, siren, siret, sirene_payload_json)
                VALUES (?, ?, 1, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(offer_id) DO UPDATE SET
                    company_name = COALESCE(NULLIF(excluded.company_name, ''), offres_enriched_team.company_name),
                    sirene_matched = MAX(offres_enriched_team.sirene_matched, excluded.sirene_matched),
                    match_method = CASE
                        WHEN excluded.match_score >= COALESCE(offres_enriched_team.match_score, 0)
                        THEN excluded.match_method
                        ELSE offres_enriched_team.match_method
                    END,
                    match_score = CASE
                        WHEN excluded.match_score >= COALESCE(offres_enriched_team.match_score, 0)
                        THEN excluded.match_score
                        ELSE COALESCE(offres_enriched_team.match_score, 0)
                    END,
                    naf_match = MAX(COALESCE(offres_enriched_team.naf_match, 0), excluded.naf_match),
                    code_naf_offre = COALESCE(NULLIF(excluded.code_naf_offre, ''), offres_enriched_team.code_naf_offre),
                    code_naf_sirene = COALESCE(NULLIF(excluded.code_naf_sirene, ''), offres_enriched_team.code_naf_sirene),
                    sirene_name = CASE
                        WHEN excluded.match_score >= COALESCE(offres_enriched_team.match_score, 0)
                        THEN COALESCE(NULLIF(excluded.sirene_name, ''), offres_enriched_team.sirene_name)
                        ELSE offres_enriched_team.sirene_name
                    END,
                    siren = CASE
                        WHEN excluded.match_score >= COALESCE(offres_enriched_team.match_score, 0)
                        THEN COALESCE(NULLIF(excluded.siren, ''), offres_enriched_team.siren)
                        ELSE offres_enriched_team.siren
                    END,
                    siret = CASE
                        WHEN excluded.match_score >= COALESCE(offres_enriched_team.match_score, 0)
                        THEN COALESCE(NULLIF(excluded.siret, ''), offres_enriched_team.siret)
                        ELSE offres_enriched_team.siret
                    END,
                    sirene_payload_json = CASE
                        WHEN excluded.match_score >= COALESCE(offres_enriched_team.match_score, 0)
                        THEN COALESCE(excluded.sirene_payload_json, offres_enriched_team.sirene_payload_json)
                        ELSE offres_enriched_team.sirene_payload_json
                    END,
                    last_seen_at = CURRENT_TIMESTAMP
                """,
                (
                    offer_id,
                    company_name,
                    match_method,
                    match_score,
                    naf_match,
                    code_naf_offre,
                    extract_naf_from_etab(match),
                    match.get("uniteLegale", {}).get("denominationUniteLegale"),
                    match.get("siren"),
                    match.get("siret"),
                    json.dumps(match, ensure_ascii=False),
                ),
            )
        else:
            conn.execute(
                """
                INSERT INTO offres_enriched_team
                (offer_id, company_name, sirene_matched, match_method, match_score, naf_match,
                 code_naf_offre, code_naf_sirene, sirene_name, siren, siret, sirene_payload_json)
                VALUES (?, ?, 0, 'no_match', 0, 0, ?, NULL, NULL, NULL, NULL, NULL)
                ON CONFLICT(offer_id) DO UPDATE SET
                    company_name = COALESCE(NULLIF(excluded.company_name, ''), offres_enriched_team.company_name),
                    code_naf_offre = COALESCE(NULLIF(excluded.code_naf_offre, ''), offres_enriched_team.code_naf_offre),
                    last_seen_at = CURRENT_TIMESTAMP
                """,
                (offer_id, company_name, code_naf_offre),
            )
    return matched, len(offers)


def collect_global_metrics(conn: sqlite3.Connection) -> tuple[int, int]:
    """Retourne (matched_global, total_global) sur la table enrichie."""
    row = conn.execute(
        """
        SELECT
            COUNT(*) AS total_global,
            SUM(CASE WHEN sirene_matched = 1 THEN 1 ELSE 0 END) AS matched_global
        FROM offres_enriched_team
        """
    ).fetchone()
    total_global = int(row[0] or 0)
    matched_global = int(row[1] or 0)
    return matched_global, total_global


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Enrichissement Sirene team_working")
    parser.add_argument(
        "--run-ts",
        default=None,
        help="Run timestamp de l'ingestion offres (isoformat). Par defaut: dernier run disponible.",
    )
    parser.add_argument(
        "--scope",
        choices=["run", "all"],
        default="all",
        help="all (defaut): enrichit toutes les offres en base ; run: enrichit seulement le run cible.",
    )
    return parser.parse_args()


def resolve_run_ts(conn: sqlite3.Connection, run_ts_arg: str | None) -> str | None:
    if run_ts_arg:
        return run_ts_arg
    row = conn.execute("SELECT MAX(last_seen_at) FROM raw_offres_team").fetchone()
    return row[0] if row and row[0] else None


def main() -> None:
    args = parse_args()
    init_db()
    conn = sqlite3.connect(DB_PATH)
    run_ts = None
    if args.scope == "run":
        run_ts = resolve_run_ts(conn, args.run_ts)
        if not run_ts:
            print("Aucun run disponible dans raw_offres_team.")
            conn.close()
            return
        print(f"scope: run | run_ts cible: {run_ts}")
        company_names = load_unique_company_names_for_run(conn, run_ts)
        print(f"{len(company_names)} entreprises uniques a rechercher (run courant)")
    else:
        print("scope: all | enrichissement sur toutes les offres en base")
        company_names = load_unique_company_names_all(conn)
        print(f"{len(company_names)} entreprises uniques a rechercher (global)")
    sirene_map_api = search_sirene_batch(company_names)
    cache_map = load_cached_sirene_map(conn)
    # Union API + cache: reduit les faux negatifs de variantes de noms.
    sirene_map = {**cache_map, **sirene_map_api}
    print(f"{len(sirene_map_api)} entreprises Sirene trouvees via API")
    print(f"{len(cache_map)} entreprises Sirene en cache local")
    print(f"{len(sirene_map)} entreprises Sirene candidates (API + cache)")

    enrich_run_ts = datetime.now(timezone.utc).isoformat(timespec="seconds")
    persist_raw_sirene_results(conn, sirene_map_api)
    matched, total = enrich_offres(conn, sirene_map, run_ts)
    matched_global, total_global = collect_global_metrics(conn)
    conn.commit()
    conn.close()
    print(f"[run courant] {matched}/{total} offres enrichies avec Sirene")
    print(f"[global] {matched_global}/{total_global} offres enrichies avec Sirene")
    print(f"enrich_run_ts: {enrich_run_ts}")


if __name__ == "__main__":
    main()
