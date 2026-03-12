import os
import json
import time
import logging
from datetime import datetime, timezone
from typing import List, Dict, Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from dotenv import load_dotenv
from google.cloud import storage

load_dotenv()

# =========================
# CONFIG
# =========================
ADZUNA_APP_ID = os.getenv("ADZUNA_APP_ID")
ADZUNA_APP_KEY = os.getenv("ADZUNA_APP_KEY")
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME")

COUNTRY = "fr"
SEARCH_TERM = "data engineer"
RESULTS_PER_PAGE = 50
SLEEP_SECONDS = 3.0
MAX_PAGES_SAFETY = 80  # garde-fou
RAW_PREFIX = "raw-adzuna"

BASE_URL = f"https://api.adzuna.com/v1/api/jobs/{COUNTRY}/search"

# =========================
# LOGGING
# =========================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)


def validate_env() -> None:
    missing = []
    if not ADZUNA_APP_ID:
        missing.append("ADZUNA_APP_ID")
    if not ADZUNA_APP_KEY:
        missing.append("ADZUNA_APP_KEY")
    if not GCS_BUCKET_NAME:
        missing.append("GCS_BUCKET_NAME")

    if missing:
        raise ValueError(f"Variables d'environnement manquantes : {', '.join(missing)}")


def build_session() -> requests.Session:
    session = requests.Session()
    retries = Retry(
        total=4,
        backoff_factor=2,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retries)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def fetch_page(session: requests.Session, page: int) -> Dict[str, Any]:
    url = f"{BASE_URL}/{page}"
    params = {
        "app_id": ADZUNA_APP_ID,
        "app_key": ADZUNA_APP_KEY,
        "what": SEARCH_TERM,
        "results_per_page": RESULTS_PER_PAGE,
        "content-type": "application/json",
    }

    logger.info("Appel API Adzuna page=%s", page)
    response = session.get(url, params=params, timeout=30)

    if response.status_code != 200:
        logger.error("Erreur API page=%s status=%s body=%s", page, response.status_code, response.text[:500])
        response.raise_for_status()

    return response.json()


def upload_string_to_gcs(bucket_name: str, blob_name: str, content: str, content_type: str) -> None:
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.upload_from_string(content, content_type=content_type)
    logger.info("Upload terminé : gs://%s/%s", bucket_name, blob_name)


def ingest_adzuna_max_data_engineer() -> None:
    validate_env()
    session = build_session()

    page = 1
    total_expected = None
    all_jobs: List[Dict[str, Any]] = []
    seen_ids = set()

    while True:
        if page > MAX_PAGES_SAFETY:
            logger.warning("Arrêt sécurité : MAX_PAGES_SAFETY atteint (%s)", MAX_PAGES_SAFETY)
            break

        data = fetch_page(session, page)
        results = data.get("results", [])

        if total_expected is None:
            total_expected = data.get("count")
            logger.info("Total estimé par l'API : %s", total_expected)

        logger.info("Page %s : %s résultats", page, len(results))

        if not results:
            logger.info("Aucun résultat sur la page %s, arrêt.", page)
            break

        new_jobs_count = 0
        for job in results:
            job_id = job.get("id")
            if job_id and job_id not in seen_ids:
                seen_ids.add(job_id)
                all_jobs.append(job)
                new_jobs_count += 1

        logger.info("Page %s : %s nouvelles offres ajoutées", page, new_jobs_count)
        logger.info("Total cumulé : %s", len(all_jobs))

        # Condition de fin 1 : dernière page probable
        if len(results) < RESULTS_PER_PAGE:
            logger.info("Dernière page détectée (moins de résultats que RESULTS_PER_PAGE).")
            break

        # Condition de fin 2 : on a atteint le total annoncé
        if total_expected is not None and len(all_jobs) >= total_expected:
            logger.info("Tous les résultats annoncés semblent récupérés.")
            break

        page += 1
        time.sleep(SLEEP_SECONDS)

    collected_at = datetime.now(timezone.utc).isoformat()
    date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    timestamp_str = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

    payload = {
        "source": "adzuna",
        "country": COUNTRY,
        "search_term": SEARCH_TERM,
        "collected_at": collected_at,
        "results_per_page": RESULTS_PER_PAGE,
        "pages_processed": page,
        "max_pages_safety": MAX_PAGES_SAFETY,
        "total_expected": total_expected,
        "total_collected": len(all_jobs),
        "jobs": all_jobs,
    }

    blob_name = f"{RAW_PREFIX}/date={date_str}/adzuna_data_engineer_{timestamp_str}.json"

    upload_string_to_gcs(
        bucket_name=GCS_BUCKET_NAME,
        blob_name=blob_name,
        content=json.dumps(payload, ensure_ascii=False, indent=2),
        content_type="application/json",
    )

    logger.info("Ingestion terminée. %s offres stockées.", len(all_jobs))


if __name__ == "__main__":
    ingest_adzuna_max_data_engineer()