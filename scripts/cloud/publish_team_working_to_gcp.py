"""Publie les tables team_working vers GCS (raw) puis BigQuery (raw)."""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
from pathlib import Path
from typing import Iterable
import sys

from google.cloud import bigquery, storage

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from scripts.core.common import load_project_env  # noqa: E402


PROJECT_ROOT = Path(__file__).resolve().parents[2]
TEAM_DB_PATH = PROJECT_ROOT / "team_working" / "db" / "team_working.db"
EXPORT_DIR = PROJECT_ROOT / "team_working" / "exports"
TABLES = ("raw_offres_team", "raw_sirene_results_team", "offres_enriched_team")

load_project_env()


def export_table_to_ndjson(db_path: Path, table_name: str, export_dir: Path) -> Path:
    """Exporte une table SQLite vers un fichier NDJSON."""
    export_dir.mkdir(parents=True, exist_ok=True)
    output_path = export_dir / f"{table_name}.ndjson"

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(f"SELECT * FROM {table_name}").fetchall()
    conn.close()

    with output_path.open("w", encoding="utf-8") as file:
        for row in rows:
            payload = {key: row[key] for key in row.keys()}
            file.write(json.dumps(payload, ensure_ascii=False) + "\n")
    return output_path


def upload_to_gcs(bucket_name: str, local_path: Path, blob_path: str) -> str:
    """Upload un fichier local vers GCS et retourne son URI gs://."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_path)
    blob.upload_from_filename(str(local_path))
    return f"gs://{bucket_name}/{blob_path}"


def load_ndjson_to_bq(
    project_id: str,
    dataset: str,
    table: str,
    gcs_uri: str,
    write_disposition: str,
) -> None:
    """Charge un fichier NDJSON GCS vers BigQuery (schema autodetect)."""
    bq_client = bigquery.Client(project=project_id)
    table_id = f"{project_id}.{dataset}.{table}"
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        autodetect=True,
        write_disposition=write_disposition,
    )
    load_job = bq_client.load_table_from_uri(gcs_uri, table_id, job_config=job_config)
    load_job.result()


def parse_args() -> argparse.Namespace:
    """Parse les arguments CLI."""
    parser = argparse.ArgumentParser(description="Publication team_working -> GCS -> BigQuery")
    parser.add_argument("--db-path", default=str(TEAM_DB_PATH), help="Chemin SQLite team_working.db")
    parser.add_argument("--bucket", default=os.environ.get("GCS_BUCKET_NAME"), help="Bucket GCS cible")
    parser.add_argument("--project-id", default=os.environ.get("BQ_PROJECT_ID"), help="Projet BigQuery cible")
    parser.add_argument("--dataset", default="raw", help="Dataset BigQuery cible")
    parser.add_argument(
        "--write-disposition",
        default="WRITE_TRUNCATE",
        choices=("WRITE_TRUNCATE", "WRITE_APPEND"),
        help="Mode de chargement BigQuery",
    )
    parser.add_argument(
        "--gcs-prefix",
        default="raw-sirene",
        help="Prefix GCS cible (ex: raw-sirene)",
    )
    return parser.parse_args()


def ensure_required_values(bucket: str | None, project_id: str | None) -> tuple[str, str]:
    """Valide les parametres obligatoires."""
    if not bucket:
        raise ValueError("Bucket manquant. Definir --bucket ou GCS_BUCKET_NAME.")
    if not project_id:
        raise ValueError("Project ID manquant. Definir --project-id ou BQ_PROJECT_ID.")
    return bucket, project_id


def iter_tables() -> Iterable[str]:
    """Retourne la liste des tables a publier."""
    return TABLES


def main() -> None:
    """Orchestre export SQLite, upload GCS et load BigQuery."""
    args = parse_args()
    bucket, project_id = ensure_required_values(args.bucket, args.project_id)
    db_path = Path(args.db_path).resolve()

    for table_name in iter_tables():
        print(f"[1/3] export SQLite -> NDJSON: {table_name}")
        ndjson_path = export_table_to_ndjson(db_path=db_path, table_name=table_name, export_dir=EXPORT_DIR)

        blob_path = f"{args.gcs_prefix}/{table_name}.ndjson"
        print(f"[2/3] upload GCS: {blob_path}")
        gcs_uri = upload_to_gcs(bucket_name=bucket, local_path=ndjson_path, blob_path=blob_path)

        print(f"[3/3] load BigQuery: {project_id}.{args.dataset}.{table_name}")
        load_ndjson_to_bq(
            project_id=project_id,
            dataset=args.dataset,
            table=table_name,
            gcs_uri=gcs_uri,
            write_disposition=args.write_disposition,
        )
        print(f"OK: {table_name}")

    print("Publication terminee.")


if __name__ == "__main__":
    main()

