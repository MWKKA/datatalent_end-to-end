"""Cloud ingestion job for France Travail and Adzuna raw data."""

from __future__ import annotations

import csv
import io
import json
import os
import re
import time
import unicodedata
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

import requests
from google.cloud import bigquery, storage

from scripts.core.common import load_project_env
from scripts.ingestion.france_travail_auth import get_access_token

OFFRES_SEARCH_URL = "https://api.francetravail.io/partenaire/offresdemploi/v2/offres/search"
ADZUNA_SEARCH_URL_TEMPLATE = "https://api.adzuna.com/v1/api/jobs/fr/search/{page}"
REQUEST_TIMEOUT_SECONDS = 30
PAGE_SIZE = 150
ADZUNA_PAGE_SIZE = 50
DELAY_BETWEEN_REQUESTS_SEC = 0.2
GEO_COMMUNES_CSV_URL = "https://www.data.gouv.fr/api/1/datasets/r/f5df602b-3800-44d7-b2df-fa40a0350325"
SIRENE_BASE_URL = "https://api.insee.fr/api-sirene/3.11"


def _require_env(name: str) -> str:
    value = (os.environ.get(name) or "").strip()
    if not value:
        raise ValueError(f"Environment variable {name} is required.")
    return value


def _int_env(name: str, default: int) -> int:
    value = (os.environ.get(name) or "").strip()
    return int(value) if value else default


def fetch_france_travail_offers(mots_cles: str, max_results: int) -> list[dict[str, Any]]:
    token = get_access_token()
    offers: list[dict[str, Any]] = []
    range_start = 0

    while range_start < max_results:
        range_end = min(range_start + PAGE_SIZE - 1, max_results - 1)
        response = requests.get(
            OFFRES_SEARCH_URL,
            headers={"Authorization": f"Bearer {token}", "Range": f"offres={range_start}-{range_end}"},
            params={"motsCles": mots_cles},
            timeout=REQUEST_TIMEOUT_SECONDS,
        )
        if response.status_code == 416:
            break
        response.raise_for_status()
        payload = response.json()
        page_offers = payload.get("resultats") or payload.get("offres") or []
        if not page_offers:
            break
        offers.extend(page_offers)
        if len(page_offers) < PAGE_SIZE:
            break
        range_start = range_end + 1
        time.sleep(DELAY_BETWEEN_REQUESTS_SEC)
    return offers


def fetch_adzuna_offers(mots_cles: str, max_results: int) -> list[dict[str, Any]]:
    app_id = _require_env("ADZUNA_APP_ID")
    app_key = _require_env("ADZUNA_APP_KEY")
    offers: list[dict[str, Any]] = []
    page = 1

    while len(offers) < max_results:
        response = requests.get(
            ADZUNA_SEARCH_URL_TEMPLATE.format(page=page),
            params={
                "app_id": app_id,
                "app_key": app_key,
                "results_per_page": ADZUNA_PAGE_SIZE,
                "what": mots_cles,
                "content-type": "application/json",
            },
            timeout=REQUEST_TIMEOUT_SECONDS,
        )
        response.raise_for_status()
        payload = response.json()
        page_offers = payload.get("results") or []
        if not page_offers:
            break
        offers.extend(page_offers)
        if len(page_offers) < ADZUNA_PAGE_SIZE:
            break
        page += 1
        time.sleep(DELAY_BETWEEN_REQUESTS_SEC)
    return offers[:max_results]


def to_ndjson_records(records: list[dict[str, Any]], source_name: str, run_ts: str) -> list[str]:
    return [
        json.dumps(
            {
                "source_name": source_name,
                "run_ts": run_ts,
                "ingestion_timestamp": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                "raw_json": json.dumps(record, ensure_ascii=False),
            },
            ensure_ascii=False,
        )
        for record in records
    ]


def write_ndjson_file(lines: list[str], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("\n".join(lines) + ("\n" if lines else ""), encoding="utf-8")


def upload_to_gcs(bucket_name: str, local_path: Path, object_path: str) -> str:
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(object_path)
    blob.upload_from_filename(str(local_path))
    return f"gs://{bucket_name}/{object_path}"


def _communes_csv_header_to_bq_schema(header_row: list[str]) -> list[bigquery.SchemaField]:
    """Noms de colonnes BigQuery + tout en STRING (ex. code_insee 2A001 n'est pas un entier)."""
    seen: dict[str, int] = {}
    fields: list[bigquery.SchemaField] = []
    for i, raw in enumerate(header_row):
        name = (raw or "").strip() or f"field_{i}"
        safe = re.sub(r"[^0-9a-zA-Z_]", "_", name).strip("_") or f"field_{i}"
        if safe[0].isdigit():
            safe = f"_{safe}"
        count = seen.get(safe, 0)
        seen[safe] = count + 1
        fname = safe if count == 0 else f"{safe}_{count}"
        fields.append(bigquery.SchemaField(fname, "STRING"))
    return fields


def refresh_geo_communes_table(project_id: str, dataset_id: str, bucket_name: str, run_ts: str) -> str:
    response = requests.get(GEO_COMMUNES_CSV_URL, timeout=REQUEST_TIMEOUT_SECONDS)
    response.raise_for_status()

    local_path = Path("/tmp") / f"communes_raw_{run_ts}.csv"
    local_path.write_bytes(response.content)

    # Première ligne = en-tête (autodetect typait code_insee en INT64 → échec sur 2Axxx, 2Bxxx).
    first_line = local_path.read_bytes().split(b"\n", 1)[0].decode("utf-8-sig")
    header_row = next(csv.reader(io.StringIO(first_line)))
    communes_schema = _communes_csv_header_to_bq_schema(header_row)

    object_path = f"raw/geo/communes/run_ts={run_ts}/communes.csv"
    geo_uri = upload_to_gcs(bucket_name=bucket_name, local_path=local_path, object_path=object_path)

    client = bigquery.Client(project=project_id)
    destination = f"{project_id}.{dataset_id}.communes_raw"
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        autodetect=False,
        schema=communes_schema,
    )
    with local_path.open("rb") as csv_file:
        load_job = client.load_table_from_file(csv_file, destination, job_config=job_config)
    load_job.result()
    return geo_uri


def extract_company_name(record: dict[str, Any]) -> str | None:
    company = record.get("company")
    if isinstance(company, dict) and company.get("display_name"):
        return str(company["display_name"]).strip()
    entreprise = record.get("entreprise")
    if isinstance(entreprise, dict) and entreprise.get("nom"):
        return str(entreprise["nom"]).strip()
    return None


def extract_location_name(record: dict[str, Any]) -> str | None:
    location = record.get("location")
    if isinstance(location, dict) and location.get("display_name"):
        return str(location["display_name"]).strip()
    lieu = record.get("lieuTravail")
    if isinstance(lieu, dict) and lieu.get("libelle"):
        return str(lieu["libelle"]).strip()
    return None


def extract_intitule(record: dict[str, Any], source_name: str) -> str | None:
    if source_name == "adzuna":
        value = record.get("title")
    else:
        value = record.get("intitule")
    if not value:
        return None
    text = str(value).strip()
    return text or None


def extract_rome_code(record: dict[str, Any], source_name: str) -> str | None:
    if source_name == "adzuna":
        return None
    value = record.get("romeCode") or record.get("rome_code")
    if not value:
        return None
    text = str(value).strip()
    return text or None


def extract_code_naf(record: dict[str, Any], source_name: str) -> str | None:
    if source_name == "adzuna":
        return None
    value = record.get("codeNAF") or record.get("code_naf")
    if not value:
        return None
    text = str(value).strip()
    return text or None


def build_raw_offres_rows(records: list[dict[str, Any]], source_name: str, run_ts: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for record in records:
        if source_name == "adzuna":
            source_offer_id = str(record.get("id") or f"adzuna_generated_{uuid4().hex}")
            offer_id = f"adzuna_{source_offer_id}"
            is_france_travail = 0
            is_adzuna = 1
        else:
            source_offer_id = str(record.get("id") or f"ft_generated_{uuid4().hex}")
            offer_id = source_offer_id
            is_france_travail = 1
            is_adzuna = 0

        rows.append(
            {
                "offer_id": offer_id,
                "source_offer_id": source_offer_id,
                "source_name": source_name,
                "is_france_travail": is_france_travail,
                "is_adzuna": is_adzuna,
                "company_name": extract_company_name(record),
                "location_name": extract_location_name(record),
                "intitule": extract_intitule(record, source_name),
                "rome_code": extract_rome_code(record, source_name),
                "code_naf_offre": extract_code_naf(record, source_name),
                "payload_json": json.dumps(record, ensure_ascii=False),
                "last_seen_at": run_ts,
                "ingested_at": run_ts,
            }
        )
    return rows


def _dedupe_raw_offres_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Une seule ligne par offer_id (MERGE BigQuery exige au plus une ligne source par cible)."""
    by_offer_id: dict[str, dict[str, Any]] = {}
    for row in rows:
        oid = row.get("offer_id")
        if not oid:
            continue
        by_offer_id[str(oid)] = row
    return list(by_offer_id.values())


def upsert_raw_offres_rows(project_id: str, dataset_id: str, rows: list[dict[str, Any]]) -> int:
    """Charge le staging, MERGE vers raw_offres_team. Retourne le nombre de lignes uniques chargées."""
    if not rows:
        return 0

    n_before = len(rows)
    rows = _dedupe_raw_offres_rows(rows)
    if len(rows) < n_before:
        print(
            json.dumps(
                {
                    "raw_offres_dedupe": {
                        "rows_before": n_before,
                        "rows_after": len(rows),
                        "duplicate_offer_ids_removed": n_before - len(rows),
                    }
                },
                ensure_ascii=False,
            ),
            flush=True,
        )
    if not rows:
        return 0

    client = bigquery.Client(project=project_id)
    staging_table = f"{project_id}.{dataset_id}._raw_offres_team_stage"
    target_table = f"{project_id}.{dataset_id}.raw_offres_team"
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        autodetect=False,
        schema=[
            bigquery.SchemaField("offer_id", "STRING"),
            bigquery.SchemaField("source_offer_id", "STRING"),
            bigquery.SchemaField("source_name", "STRING"),
            bigquery.SchemaField("is_france_travail", "INTEGER"),
            bigquery.SchemaField("is_adzuna", "INTEGER"),
            bigquery.SchemaField("company_name", "STRING"),
            bigquery.SchemaField("location_name", "STRING"),
            bigquery.SchemaField("intitule", "STRING"),
            bigquery.SchemaField("rome_code", "STRING"),
            bigquery.SchemaField("code_naf_offre", "STRING"),
            bigquery.SchemaField("payload_json", "STRING"),
            bigquery.SchemaField("ingested_at", "TIMESTAMP"),
            bigquery.SchemaField("last_seen_at", "TIMESTAMP"),
        ],
    )
    load_job = client.load_table_from_json(rows, staging_table, job_config=job_config)
    load_job.result()

    # QUALIFY côté SQL : au cas où le staging aurait encore plusieurs lignes par offer_id.
    merge_query = f"""
    MERGE `{target_table}` T
    USING (
      SELECT * EXCEPT (dedupe_rn)
      FROM (
        SELECT
          *,
          ROW_NUMBER() OVER (
            PARTITION BY offer_id
            ORDER BY last_seen_at DESC, ingested_at DESC
          ) AS dedupe_rn
        FROM `{staging_table}`
      )
      WHERE dedupe_rn = 1
    ) S
    ON T.offer_id = S.offer_id
    WHEN MATCHED THEN
      UPDATE SET
        source_offer_id = S.source_offer_id,
        source_name = S.source_name,
        is_france_travail = S.is_france_travail,
        is_adzuna = S.is_adzuna,
        company_name = COALESCE(S.company_name, T.company_name),
        location_name = COALESCE(S.location_name, T.location_name),
        intitule = COALESCE(S.intitule, T.intitule),
        rome_code = COALESCE(S.rome_code, T.rome_code),
        code_naf_offre = COALESCE(S.code_naf_offre, T.code_naf_offre),
        payload_json = S.payload_json,
        last_seen_at = S.last_seen_at
    WHEN NOT MATCHED THEN
      INSERT (
        offer_id, source_offer_id, source_name, is_france_travail, is_adzuna,
        company_name, location_name, intitule, rome_code, code_naf_offre,
        payload_json, ingested_at, last_seen_at
      )
      VALUES (
        S.offer_id, S.source_offer_id, S.source_name, S.is_france_travail, S.is_adzuna,
        S.company_name, S.location_name, S.intitule, S.rome_code, S.code_naf_offre,
        S.payload_json, S.ingested_at, S.last_seen_at
      )
    """
    client.query(merge_query).result()
    return len(rows)


def normalize_text(value: str | None) -> str:
    if not value:
        return ""
    text = unicodedata.normalize("NFKD", value)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = text.upper()
    text = re.sub(r"[^A-Z0-9 ]+", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def normalize_company_name(value: str | None) -> str:
    if not value:
        return ""
    text = normalize_text(value.replace("-", " "))
    for suffix in (" SASU", " SAS", " SARL", " SA", " EURL", " SCI", " SNC", " SE", " GIE"):
        if text.endswith(suffix):
            text = text[: -len(suffix)].strip()
    return text


def compact_text(value: str | None) -> str:
    if not value:
        return ""
    return re.sub(r"\s+", "", value)


def normalize_naf(value: str | None) -> str:
    if not value:
        return ""
    return re.sub(r"[^A-Z0-9]", "", value.upper())


def extract_naf_from_sirene_payload(payload: dict[str, Any]) -> str | None:
    naf_direct = payload.get("activitePrincipaleEtablissement")
    if naf_direct:
        return str(naf_direct)
    unite_legale = payload.get("uniteLegale") or {}
    naf_ul = unite_legale.get("activitePrincipaleUniteLegale")
    if naf_ul:
        return str(naf_ul)
    return None


def _build_sirene_query(company_names: list[str]) -> str:
    escaped = [name.replace('"', " ")[:120] for name in company_names if name and name.strip()]
    return " OR ".join([f'denominationUniteLegale:"{name}"' for name in escaped])


def _search_sirene_batch(company_names: list[str], api_key: str) -> dict[str, dict[str, Any]]:
    if not company_names:
        return {}
    query = _build_sirene_query(company_names)
    if not query:
        return {}
    headers = {"X-INSEE-Api-Key-Integration": api_key}
    params = {"q": query, "nombre": 1000}
    response = requests.get(f"{SIRENE_BASE_URL}/siret", headers=headers, params=params, timeout=60)
    if response.status_code == 414 and len(company_names) > 1:
        mid = len(company_names) // 2
        left = _search_sirene_batch(company_names[:mid], api_key)
        right = _search_sirene_batch(company_names[mid:], api_key)
        return {**left, **right}
    response.raise_for_status()
    etablissements = response.json().get("etablissements", [])
    result: dict[str, dict[str, Any]] = {}
    for etab in etablissements:
        nom = (etab.get("uniteLegale") or {}).get("denominationUniteLegale")
        if nom:
            result[str(nom)] = etab
    return result


def refresh_sirene_raw_cache(project_id: str, dataset_id: str, run_ts: str, api_key: str) -> int:
    client = bigquery.Client(project=project_id)
    companies_query = f"""
    SELECT DISTINCT company_name
    FROM `{project_id}.{dataset_id}.raw_offres_team`
    WHERE last_seen_at = @run_ts
      AND company_name IS NOT NULL
      AND TRIM(company_name) != ''
    """
    run_dt = datetime.fromisoformat(run_ts)
    rows = client.query(
        companies_query,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("run_ts", "TIMESTAMP", run_dt)]
        ),
    ).result()
    company_names = [str(row.company_name) for row in rows]

    sirene_map: dict[str, dict[str, Any]] = {}
    batch_size = 20
    for i in range(0, len(company_names), batch_size):
        batch = company_names[i : i + batch_size]
        sirene_map.update(_search_sirene_batch(batch, api_key))
        time.sleep(0.2)

    if not sirene_map:
        return 0

    sirene_rows = []
    for sirene_name, payload in sirene_map.items():
        sirene_rows.append(
            {
                "company_name": sirene_name,
                "sirene_name": sirene_name,
                "siren": payload.get("siren"),
                "siret": payload.get("siret"),
                "is_headquarter": 1 if payload.get("etablissementSiege") else 0,
                "payload_json": json.dumps(payload, ensure_ascii=False),
                "fetched_at": run_ts,
                "last_seen_at": run_ts,
            }
        )

    staging_table = f"{project_id}.{dataset_id}._raw_sirene_results_stage"
    target_table = f"{project_id}.{dataset_id}.raw_sirene_results_team"
    load_job = client.load_table_from_json(
        sirene_rows,
        staging_table,
        job_config=bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            schema=[
                bigquery.SchemaField("company_name", "STRING"),
                bigquery.SchemaField("sirene_name", "STRING"),
                bigquery.SchemaField("siren", "INTEGER"),
                bigquery.SchemaField("siret", "INTEGER"),
                bigquery.SchemaField("is_headquarter", "INTEGER"),
                bigquery.SchemaField("payload_json", "STRING"),
                bigquery.SchemaField("fetched_at", "TIMESTAMP"),
                bigquery.SchemaField("last_seen_at", "TIMESTAMP"),
            ],
        ),
    )
    load_job.result()

    merge_query = f"""
    MERGE `{target_table}` T
    USING `{staging_table}` S
    ON T.company_name = S.company_name
    WHEN MATCHED THEN
      UPDATE SET
        sirene_name = COALESCE(S.sirene_name, T.sirene_name),
        siren = COALESCE(S.siren, T.siren),
        siret = COALESCE(S.siret, T.siret),
        is_headquarter = COALESCE(S.is_headquarter, T.is_headquarter),
        payload_json = COALESCE(S.payload_json, T.payload_json),
        last_seen_at = S.last_seen_at
    WHEN NOT MATCHED THEN
      INSERT (company_name, sirene_name, siren, siret, is_headquarter, payload_json, fetched_at, last_seen_at)
      VALUES (S.company_name, S.sirene_name, S.siren, S.siret, S.is_headquarter, S.payload_json, S.fetched_at, S.last_seen_at)
    """
    client.query(merge_query).result()
    return len(sirene_rows)


def score_match(company_name: str, sirene_name: str, code_naf_offre: str | None, code_naf_sirene: str | None) -> tuple[int, str, int]:
    company_norm = normalize_company_name(company_name)
    sirene_norm = normalize_company_name(sirene_name)
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
    elif sirene_norm.startswith(company_norm) or company_norm.startswith(sirene_norm):
        score = 90
        method = "prefix_normalized"
    elif company_norm in sirene_norm or sirene_norm in company_norm:
        score = 70
        method = "contains_normalized"

    naf_offre = normalize_naf(code_naf_offre)
    naf_sirene = normalize_naf(code_naf_sirene)
    naf_match = int(bool(naf_offre and naf_sirene and naf_offre == naf_sirene))
    if naf_match:
        score += 10
        method = f"{method}_naf" if method != "no_match" else "naf_only"
    return score, method, naf_match


def load_sirene_cache(project_id: str, dataset_id: str) -> list[dict[str, Any]]:
    client = bigquery.Client(project=project_id)
    query = f"""
    SELECT company_name, sirene_name, siren, siret, payload_json
    FROM `{project_id}.{dataset_id}.raw_sirene_results_team`
    WHERE company_name IS NOT NULL
      AND sirene_name IS NOT NULL
    """
    rows = client.query(query).result()
    cache: list[dict[str, Any]] = []
    for row in rows:
        payload = {}
        if row.payload_json:
            try:
                payload = json.loads(row.payload_json)
            except Exception:
                payload = {}
        cache.append(
            {
                "company_name": row.company_name,
                "sirene_name": row.sirene_name,
                "siren": row.siren,
                "siret": row.siret,
                "payload": payload,
            }
        )
    return cache


def enrich_offres_with_sirene_cache(project_id: str, dataset_id: str, run_ts: str) -> tuple[int, int]:
    client = bigquery.Client(project=project_id)
    offers_query = f"""
    SELECT offer_id, company_name, code_naf_offre
    FROM `{project_id}.{dataset_id}.raw_offres_team`
    WHERE last_seen_at = @run_ts
      AND company_name IS NOT NULL
      AND TRIM(company_name) != ''
    """
    run_dt = datetime.fromisoformat(run_ts)
    offers = client.query(
        offers_query,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("run_ts", "TIMESTAMP", run_dt)]
        ),
    ).result()

    sirene_cache = load_sirene_cache(project_id=project_id, dataset_id=dataset_id)
    enriched_rows: list[dict[str, Any]] = []
    matched_count = 0

    for offer in offers:
        best = None
        best_score = 0
        best_method = "no_match"
        best_naf_match = 0
        best_naf_sirene = None
        for candidate in sirene_cache:
            score, method, naf_match = score_match(
                company_name=offer.company_name,
                sirene_name=candidate["sirene_name"],
                code_naf_offre=offer.code_naf_offre,
                code_naf_sirene=extract_naf_from_sirene_payload(candidate["payload"]),
            )
            if score > best_score:
                best = candidate
                best_score = score
                best_method = method
                best_naf_match = naf_match
                best_naf_sirene = extract_naf_from_sirene_payload(candidate["payload"])

        if best is not None and best_score >= 75:
            matched_count += 1
            enriched_rows.append(
                {
                    "offer_id": offer.offer_id,
                    "company_name": offer.company_name,
                    "sirene_matched": 1,
                    "match_method": best_method,
                    "match_score": best_score,
                    "naf_match": best_naf_match,
                    "code_naf_offre": offer.code_naf_offre,
                    "code_naf_sirene": best_naf_sirene,
                    "sirene_name": best["sirene_name"],
                    "siren": int(best["siren"]) if best["siren"] is not None else None,
                    "siret": int(best["siret"]) if best["siret"] is not None else None,
                    "sirene_payload_json": json.dumps(best["payload"], ensure_ascii=False),
                    "enriched_at": run_ts,
                    "last_seen_at": run_ts,
                }
            )
        else:
            enriched_rows.append(
                {
                    "offer_id": offer.offer_id,
                    "company_name": offer.company_name,
                    "sirene_matched": 0,
                    "match_method": "no_match",
                    "match_score": 0,
                    "naf_match": 0,
                    "code_naf_offre": offer.code_naf_offre,
                    "code_naf_sirene": None,
                    "sirene_name": None,
                    "siren": None,
                    "siret": None,
                    "sirene_payload_json": None,
                    "enriched_at": run_ts,
                    "last_seen_at": run_ts,
                }
            )

    if not enriched_rows:
        return 0, 0

    staging_table = f"{project_id}.{dataset_id}._offres_enriched_team_stage"
    target_table = f"{project_id}.{dataset_id}.offres_enriched_team"
    load_job = client.load_table_from_json(
        enriched_rows,
        staging_table,
        job_config=bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            schema=[
                bigquery.SchemaField("offer_id", "STRING"),
                bigquery.SchemaField("company_name", "STRING"),
                bigquery.SchemaField("sirene_matched", "INTEGER"),
                bigquery.SchemaField("match_method", "STRING"),
                bigquery.SchemaField("match_score", "INTEGER"),
                bigquery.SchemaField("naf_match", "INTEGER"),
                bigquery.SchemaField("code_naf_offre", "STRING"),
                bigquery.SchemaField("code_naf_sirene", "STRING"),
                bigquery.SchemaField("sirene_name", "STRING"),
                bigquery.SchemaField("siren", "INTEGER"),
                bigquery.SchemaField("siret", "INTEGER"),
                bigquery.SchemaField("sirene_payload_json", "STRING"),
                bigquery.SchemaField("enriched_at", "TIMESTAMP"),
                bigquery.SchemaField("last_seen_at", "TIMESTAMP"),
            ],
        ),
    )
    load_job.result()

    merge_query = f"""
    MERGE `{target_table}` T
    USING `{staging_table}` S
    ON T.offer_id = S.offer_id
    WHEN MATCHED THEN
      UPDATE SET
        company_name = COALESCE(S.company_name, T.company_name),
        sirene_matched = S.sirene_matched,
        match_method = S.match_method,
        match_score = S.match_score,
        naf_match = S.naf_match,
        code_naf_offre = COALESCE(S.code_naf_offre, T.code_naf_offre),
        code_naf_sirene = COALESCE(S.code_naf_sirene, T.code_naf_sirene),
        sirene_name = COALESCE(S.sirene_name, T.sirene_name),
        siren = COALESCE(S.siren, T.siren),
        siret = COALESCE(S.siret, T.siret),
        sirene_payload_json = COALESCE(S.sirene_payload_json, T.sirene_payload_json),
        last_seen_at = S.last_seen_at
    WHEN NOT MATCHED THEN
      INSERT (
        offer_id, company_name, sirene_matched, match_method, match_score, naf_match,
        code_naf_offre, code_naf_sirene, sirene_name, siren, siret, sirene_payload_json,
        enriched_at, last_seen_at
      )
      VALUES (
        S.offer_id, S.company_name, S.sirene_matched, S.match_method, S.match_score, S.naf_match,
        S.code_naf_offre, S.code_naf_sirene, S.sirene_name, S.siren, S.siret, S.sirene_payload_json,
        S.enriched_at, S.last_seen_at
      )
    """
    client.query(merge_query).result()
    return matched_count, len(enriched_rows)


def main() -> None:
    load_project_env()

    bucket_name = _require_env("GCS_BUCKET_NAME")
    bq_project_id = _require_env("BQ_PROJECT_ID")
    bq_dataset_raw = (os.environ.get("BQ_DATASET_RAW") or "raw").strip()
    mots_cles = (os.environ.get("INGEST_MOTS_CLES") or "data engineer").strip()
    max_results = _int_env("INGEST_MAX_RESULTS", 3000)
    sirene_api_key = _require_env("SIRENE_API_KEY")
    run_ts = datetime.now(timezone.utc).isoformat(timespec="seconds")

    ft_records = fetch_france_travail_offers(mots_cles=mots_cles, max_results=max_results)
    adzuna_records = fetch_adzuna_offers(mots_cles=mots_cles, max_results=max_results)

    ft_file = Path("/tmp") / f"offres_france_travail_{run_ts}.ndjson"
    adzuna_file = Path("/tmp") / f"offres_adzuna_{run_ts}.ndjson"
    write_ndjson_file(to_ndjson_records(ft_records, "france_travail", run_ts), ft_file)
    write_ndjson_file(to_ndjson_records(adzuna_records, "adzuna", run_ts), adzuna_file)

    ft_object_path = f"raw/offres/france_travail/run_ts={run_ts}/offres.ndjson"
    adzuna_object_path = f"raw/offres/adzuna/run_ts={run_ts}/offres.ndjson"
    ft_uri = upload_to_gcs(bucket_name=bucket_name, local_path=ft_file, object_path=ft_object_path)
    adzuna_uri = upload_to_gcs(bucket_name=bucket_name, local_path=adzuna_file, object_path=adzuna_object_path)

    ft_rows = build_raw_offres_rows(ft_records, source_name="france_travail", run_ts=run_ts)
    adzuna_rows = build_raw_offres_rows(adzuna_records, source_name="adzuna", run_ts=run_ts)
    raw_offres_upsert_count = upsert_raw_offres_rows(
        project_id=bq_project_id, dataset_id=bq_dataset_raw, rows=ft_rows + adzuna_rows
    )
    geo_uri = refresh_geo_communes_table(
        project_id=bq_project_id, dataset_id=bq_dataset_raw, bucket_name=bucket_name, run_ts=run_ts
    )
    sirene_cached_rows = refresh_sirene_raw_cache(
        project_id=bq_project_id, dataset_id=bq_dataset_raw, run_ts=run_ts, api_key=sirene_api_key
    )
    sirene_matched, sirene_total = enrich_offres_with_sirene_cache(
        project_id=bq_project_id, dataset_id=bq_dataset_raw, run_ts=run_ts
    )

    print(
        json.dumps(
            {
                "run_ts": run_ts,
                "france_travail_records": len(ft_records),
                "adzuna_records": len(adzuna_records),
                "gcs_france_travail_uri": ft_uri,
                "gcs_adzuna_uri": adzuna_uri,
                "gcs_geo_uri": geo_uri,
                "bq_dataset_raw": bq_dataset_raw,
                "raw_offres_team_rows_upserted": raw_offres_upsert_count,
                "sirene_raw_cache_rows_upserted": sirene_cached_rows,
                "sirene_matched_rows": sirene_matched,
                "sirene_total_rows": sirene_total,
            },
            ensure_ascii=False,
        )
    )


if __name__ == "__main__":
    main()
