import json
import time
from datetime import datetime, timezone
from google.cloud import storage, bigquery

BUCKET_NAME = "datatalent-raw-simplon"
PREFIX = "raw-offresemploi/offres/run_date=2026-03-11/"
PROJECT = "datatalent-simplon"
DATASET = "raw"
TABLE_ID = f"{PROJECT}.{DATASET}.offres_raw"


def create_table_if_not_exists(bq_client: bigquery.Client):
    schema = [
        bigquery.SchemaField("raw_json", "STRING"),
        bigquery.SchemaField("ingestion_timestamp", "TIMESTAMP"),
        bigquery.SchemaField("source_file", "STRING"),
    ]
    table = bigquery.Table(TABLE_ID, schema=schema)
    bq_client.create_table(table, exists_ok=True)
    print(f"Table '{TABLE_ID}' prête.")
    time.sleep(2)


def load_raw_offers_to_bigquery():
    storage_client = storage.Client()
    bq_client = bigquery.Client(project=PROJECT)

    create_table_if_not_exists(bq_client)

    blobs = list(storage_client.list_blobs(BUCKET_NAME, prefix=PREFIX))
    print("Nombre de blobs trouvés :", len(blobs))

    rows = []

    for blob in blobs:
        print("BLOB:", blob.name)
        if not blob.name.endswith(".json"):
            continue

        gcs_path = f"gs://{BUCKET_NAME}/{blob.name}"
        content = blob.download_as_text(encoding="utf-8")
        payload = json.loads(content)
        data = payload.get("data", {})
        resultats = data.get("resultats", [])
        print(f"{blob.name} -> {len(resultats)} offres")

        for offre in resultats:
            rows.append({
                "raw_json": json.dumps(offre, ensure_ascii=False),
                "ingestion_timestamp": datetime.now(timezone.utc).isoformat(),
                "source_file": gcs_path
            })

    print("Nombre total de lignes à insérer :", len(rows))
    if not rows:
        print("Aucune ligne à insérer.")
        return

    batch_size = 500
    for i in range(0, len(rows), batch_size):
        batch = rows[i:i + batch_size]
        errors = bq_client.insert_rows_json(TABLE_ID, batch)
        if errors:
            print(f"Erreurs sur le batch {i}-{i + len(batch) - 1}:")
            for error in errors:
                print(error)
        else:
            print(f"Batch {i}-{i + len(batch) - 1} inséré avec succès")


if __name__ == "__main__":
    load_raw_offers_to_bigquery()