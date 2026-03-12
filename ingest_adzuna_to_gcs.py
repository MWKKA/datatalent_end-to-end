import os
import json
import time
from datetime import datetime
from dotenv import load_dotenv
import requests
from google.cloud import storage

load_dotenv()

APP_ID = os.getenv("ADZUNA_APP_ID")
APP_KEY = os.getenv("ADZUNA_APP_KEY")
BUCKET_NAME = os.getenv("GCS_BUCKET_NAME")

BASE_URL = "https://api.adzuna.com/v1/api/jobs/fr/search"
SEARCH_TERM = "data engineer"
RESULTS_PER_PAGE = 50
MAX_PAGES_SAFETY = 10
SLEEP_SECONDS = 3

if not APP_ID or not APP_KEY:
    raise ValueError("ADZUNA_APP_ID ou ADZUNA_APP_KEY manquant dans le .env")

if not BUCKET_NAME:
    raise ValueError("GCS_BUCKET_NAME manquant dans le .env")

all_jobs = []
seen_ids = set()

page = 1
total_expected = None

while page <= MAX_PAGES_SAFETY:
    print(f"\n--- PAGE {page} ---")

    url = f"{BASE_URL}/{page}"
    params = {
        "app_id": APP_ID,
        "app_key": APP_KEY,
        "what": SEARCH_TERM,
        "results_per_page": RESULTS_PER_PAGE,
        "content-type": "application/json"
    }

    response = requests.get(url, params=params, timeout=30)
    print("Status:", response.status_code)

    if response.status_code != 200:
        print("Erreur API :", response.text)
        break

    data = response.json()
    results = data.get("results", [])

    if total_expected is None:
        total_expected = data.get("count")
        print("Total estimé API :", total_expected)

    print("Nombre d'offres sur cette page :", len(results))

    if not results:
        print("Plus de résultats, arrêt.")
        break

    for job in results:
        job_id = job.get("id")
        if job_id not in seen_ids:
            seen_ids.add(job_id)
            all_jobs.append(job)

    print("Total cumulé :", len(all_jobs))

    if len(results) < RESULTS_PER_PAGE:
        print("Dernière page détectée.")
        break

    page += 1
    time.sleep(SLEEP_SECONDS)

# payload brut à stocker
payload = {
    "source": "adzuna",
    "search_term": SEARCH_TERM,
    "collected_at": datetime.utcnow().isoformat(),
    "total_expected": total_expected,
    "total_collected": len(all_jobs),
    "jobs": all_jobs
}

# 1) sauvegarde locale
date_str = datetime.utcnow().strftime("%Y%m%d")
local_filename = f"adzuna_raw_{date_str}.json"

with open(local_filename, "w", encoding="utf-8") as f:
    json.dump(payload, f, ensure_ascii=False, indent=2)

print(f"Fichier local sauvegardé : {local_filename}")

# 2) upload GCS
client = storage.Client()
bucket = client.bucket(BUCKET_NAME)

gcs_filename = f"raw-adzuna/adzuna_raw_{date_str}.json"
blob = bucket.blob(gcs_filename)
blob.upload_from_filename(local_filename)

print(f"Fichier uploadé dans gs://{BUCKET_NAME}/{gcs_filename}")