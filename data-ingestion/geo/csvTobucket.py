import requests
from google.cloud import storage

URL = "https://www.data.gouv.fr/api/1/datasets/r/f5df602b-3800-44d7-b2df-fa40a0350325"
BUCKET_NAME = "datatalent-raw-simplon"
DESTINATION_BLOB = "raw-geo/communes-france-2025.csv"


def upload_csv_to_gcs():
    print("Téléchargement du fichier...")
    response = requests.get(URL, stream=True)
    response.raise_for_status()

    storage_client = storage.Client()
    bucket = storage_client.bucket(BUCKET_NAME)
    blob = bucket.blob(DESTINATION_BLOB)

    print(f"Upload vers gs://{BUCKET_NAME}/{DESTINATION_BLOB}...")
    blob.upload_from_string(
        response.content,
        content_type="text/csv"
    )

    print(f"✅ Fichier uploadé avec succès : gs://{BUCKET_NAME}/{DESTINATION_BLOB}")
    print(f"   Taille : {blob.size / 1024 / 1024:.2f} MB")


if __name__ == "__main__":
    upload_csv_to_gcs()