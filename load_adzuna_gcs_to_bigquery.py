import os
import json
import io
from datetime import datetime, timezone

from google.cloud import storage, bigquery
from dotenv import load_dotenv

load_dotenv()

BUCKET_NAME = os.getenv("GCS_BUCKET_NAME")
BQ_PROJECT_ID = os.getenv("BQ_PROJECT_ID")
BQ_DATASET = "raw"
BQ_TABLE = "adzuna_jobs"
RAW_PREFIX = "raw-adzuna/"

if not BUCKET_NAME or not BQ_PROJECT_ID:
    raise ValueError("GCS_BUCKET_NAME ou BQ_PROJECT_ID manquant dans le .env")

storage_client = storage.Client()
bq_client = bigquery.Client(project=BQ_PROJECT_ID)


def get_latest_blob(bucket_name: str, prefix: str):
    bucket = storage_client.bucket(bucket_name)
    blobs = list(storage_client.list_blobs(bucket, prefix=prefix))

    json_blobs = [b for b in blobs if b.name.endswith(".json")]
    if not json_blobs:
        raise FileNotFoundError(f"Aucun fichier JSON trouvé dans gs://{bucket_name}/{prefix}")

    latest_blob = max(json_blobs, key=lambda b: b.time_created)
    return latest_blob


# 1. Trouver automatiquement le dernier fichier JSON Adzuna
blob = get_latest_blob(BUCKET_NAME, RAW_PREFIX)
print(f"Fichier source détecté : gs://{BUCKET_NAME}/{blob.name}")

# 2. Lire le fichier brut depuis GCS
content = blob.download_as_text(encoding="utf-8")
payload = json.loads(content)

jobs = payload.get("jobs", [])
search_term = payload.get("search_term")
collected_at = payload.get("collected_at")

print(f"Nombre de jobs trouvés : {len(jobs)}")

rows = []
for job in jobs:
    row = {
        "job_id": str(job.get("id")) if job.get("id") is not None else None,
        "title": job.get("title"),
        "created": job.get("created"),
        "description": job.get("description"),
        "redirect_url": job.get("redirect_url"),
        "salary_min": job.get("salary_min"),
        "salary_max": job.get("salary_max"),
        "salary_is_predicted": int(job.get("salary_is_predicted")) if job.get("salary_is_predicted") is not None else None,
        "latitude": job.get("latitude"),
        "longitude": job.get("longitude"),
        "company_name": job.get("company", {}).get("display_name"),
        "location_display_name": job.get("location", {}).get("display_name"),
        "location_area": job.get("location", {}).get("area"),
        "search_term": search_term,
        "collected_at": collected_at,
        "ingestion_timestamp": datetime.now(timezone.utc).isoformat(),
        "raw_json": json.dumps(job, ensure_ascii=False)
    }
    rows.append(row)

if not rows:
    raise ValueError("Aucune ligne à charger dans BigQuery.")

table_id = f"{BQ_PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}"

job_config = bigquery.LoadJobConfig(
    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
    autodetect=True
)

ndjson_data = "\n".join(
    json.dumps(row, ensure_ascii=False) for row in rows
)

buffer = io.BytesIO(ndjson_data.encode("utf-8"))

load_job = bq_client.load_table_from_file(
    buffer,
    table_id,
    job_config=job_config
)

load_job.result()

print(f"Données chargées dans {table_id}")