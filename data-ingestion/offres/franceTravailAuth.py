import csv
import json
import time
from datetime import datetime

import requests
from google.cloud import storage


BUCKET_NAME = "datatalent-raw-simplon"
DEPARTEMENTS_FILE = "departements.csv"

TOKEN_URL = "https://authentification-partenaire.francetravail.io/connexion/oauth2/access_token?realm=/partenaire"
OFFRES_URL = "https://api.francetravail.io/partenaire/offresdemploi/v2/offres/search"


def get_france_travail_token() -> str:
    payload = {
        "client_id": "PAR_datatalent_5244322c2a3bd57a076545fa49c93c00fae2f3bf71f964ac47eb6009f482693f",
        "client_secret": "67bb02036ebfa0615381ee22be90e920085143771f4c0c28be26bcf28a8753b6",
        "grant_type": "client_credentials",
        "scope": "api_offresdemploiv2 o2dsoffre",
    }

    headers = {
        "Content-Type": "application/x-www-form-urlencoded"
    }

    response = requests.post(TOKEN_URL, data=payload, headers=headers, timeout=30)
    response.raise_for_status()

    return response.json()["access_token"]


def read_departements(csv_file: str) -> list[dict]:
    departements = []

    with open(csv_file, mode="r", encoding="utf-8") as file:
        reader = csv.DictReader(file)
        for row in reader:
            code_departement = row["code_departement"].strip()
            nom_departement = row["nom_departement"].strip()
            code_region = row["code_region"].strip()
            nom_region = row["nom_region"].strip()

            if code_departement:
                departements.append({
                    "code_departement": code_departement,
                    "nom_departement": nom_departement,
                    "code_region": code_region,
                    "nom_region": nom_region
                })

    return departements


def fetch_offres_by_department(token: str, departement: str, rome_code: str = "M1811", range_header: str = "0-49") -> dict:
    headers = {
        "Authorization": f"Bearer {token}",
        "Range": range_header
    }

    params = {
        "codeROME": rome_code,
        "departement": departement
    }

    response = requests.get(OFFRES_URL, headers=headers, params=params, timeout=30)
    response.raise_for_status()

    return response.json()


def upload_json_to_gcs(bucket_name: str, blob_path: str, data: dict) -> None:
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_path)

    blob.upload_from_string(
        json.dumps(data, ensure_ascii=False),
        content_type="application/json"
    )


def process_departements():
    token = get_france_travail_token()
    departements = read_departements(DEPARTEMENTS_FILE)

    run_date = datetime.utcnow().strftime("%Y-%m-%d")
    timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")

    for dep in departements:
        code_departement = dep["code_departement"]
        nom_departement = dep["nom_departement"]
        code_region = dep["code_region"]
        nom_region = dep["nom_region"]

        try:
            data = fetch_offres_by_department(
                token=token,
                departement=code_departement,
                rome_code="M1811",
                range_header="0-49"
            )

            # optionnel : enrichir le JSON avant upload
            payload = {
                "metadata": {
                    "run_date": run_date,
                    "timestamp_utc": timestamp,
                    "code_departement": code_departement,
                    "nom_departement": nom_departement,
                    "code_region": code_region,
                    "nom_region": nom_region,
                    "rome_code": "M1811",
                    "range": "0-49"
                },
                "data": data
            }

            blob_path = (
                f"raw-offresemploi/offres/"
                f"run_date={run_date}/"
                f"region={code_region}_{nom_region}/"
                f"departement={code_departement}_{nom_departement}/"
                f"offres_{timestamp}.json"
            )

            upload_json_to_gcs(
                bucket_name=BUCKET_NAME,
                blob_path=blob_path,
                data=payload
            )

            print(f"[{code_departement} - {nom_departement}] Upload OK -> gs://{BUCKET_NAME}/{blob_path}")

            time.sleep(1)

        except requests.HTTPError as e:
            print(f"[{code_departement} - {nom_departement}] Erreur HTTP : {e}")
        except Exception as e:
            print(f"[{code_departement} - {nom_departement}] Erreur : {e}")


if __name__ == "__main__":
    process_departements()