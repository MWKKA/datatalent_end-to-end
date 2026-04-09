import time
from google.cloud import bigquery

PROJECT = "datatalent-simplon"
DATASET = "raw"
TABLE_ID = f"{PROJECT}.{DATASET}.communes_raw"
GCS_URI = "gs://datatalent-raw-simplon/raw-geo/communes-france-2025.csv"

SCHEMA = [
    bigquery.SchemaField("index", "INTEGER"),
    bigquery.SchemaField("code_insee", "STRING"),
    bigquery.SchemaField("nom_standard", "STRING"),
    bigquery.SchemaField("nom_sans_pronom", "STRING"),
    bigquery.SchemaField("nom_a", "STRING"),
    bigquery.SchemaField("nom_de", "STRING"),
    bigquery.SchemaField("nom_sans_accent", "STRING"),
    bigquery.SchemaField("nom_standard_majuscule", "STRING"),
    bigquery.SchemaField("typecom", "STRING"),
    bigquery.SchemaField("typecom_texte", "STRING"),
    bigquery.SchemaField("reg_code", "STRING"),
    bigquery.SchemaField("reg_nom", "STRING"),
    bigquery.SchemaField("dep_code", "STRING"),
    bigquery.SchemaField("dep_nom", "STRING"),
    bigquery.SchemaField("canton_code", "STRING"),
    bigquery.SchemaField("canton_nom", "STRING"),
    bigquery.SchemaField("epci_code", "STRING"),
    bigquery.SchemaField("epci_nom", "STRING"),
    bigquery.SchemaField("academie_code", "STRING"),
    bigquery.SchemaField("academie_nom", "STRING"),
    bigquery.SchemaField("code_postal", "STRING"),
    bigquery.SchemaField("codes_postaux", "STRING"),
    bigquery.SchemaField("zone_emploi", "STRING"),
    bigquery.SchemaField("code_insee_centre_zone_emploi", "STRING"),
    bigquery.SchemaField("code_unite_urbaine", "STRING"),
    bigquery.SchemaField("nom_unite_urbaine", "STRING"),
    bigquery.SchemaField("taille_unite_urbaine", "FLOAT64"),
    bigquery.SchemaField("type_commune_unite_urbaine", "STRING"),
    bigquery.SchemaField("statut_commune_unite_urbaine", "STRING"),
    bigquery.SchemaField("population", "FLOAT64"),
    bigquery.SchemaField("superficie_hectare", "FLOAT64"),
    bigquery.SchemaField("superficie_km2", "FLOAT64"),
    bigquery.SchemaField("densite", "FLOAT64"),
    bigquery.SchemaField("altitude_moyenne", "FLOAT64"),
    bigquery.SchemaField("altitude_minimale", "FLOAT64"),
    bigquery.SchemaField("altitude_maximale", "FLOAT64"),
    bigquery.SchemaField("latitude_mairie", "FLOAT64"),
    bigquery.SchemaField("longitude_mairie", "FLOAT64"),
    bigquery.SchemaField("latitude_centre", "FLOAT64"),
    bigquery.SchemaField("longitude_centre", "FLOAT64"),
    bigquery.SchemaField("grille_densite", "FLOAT64"),
    bigquery.SchemaField("grille_densite_texte", "STRING"),
    bigquery.SchemaField("niveau_equipements_services", "FLOAT64"),
    bigquery.SchemaField("niveau_equipements_services_texte", "STRING"),
    bigquery.SchemaField("gentile", "STRING"),
    bigquery.SchemaField("url_wikipedia", "STRING"),
    bigquery.SchemaField("url_villedereve", "STRING"),
]


def create_dataset_and_table(bq_client: bigquery.Client):
    # Création du dataset raw si absent
    dataset_ref = bigquery.Dataset(f"{PROJECT}.{DATASET}")
    dataset_ref.location = "EU"
    bq_client.create_dataset(dataset_ref, exists_ok=True)
    print(f"Dataset '{DATASET}' prêt.")

    # Création de la table si absente
    table = bigquery.Table(TABLE_ID, schema=SCHEMA)
    bq_client.create_table(table, exists_ok=True)
    print(f"Table '{TABLE_ID}' prête.")
    time.sleep(2)


def load_communes_csv_to_raw():
    bq_client = bigquery.Client(project=PROJECT)

    # Création dataset + table si absents
    create_dataset_and_table(bq_client)

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=2,
        allow_jagged_rows=True,
        allow_quoted_newlines=True,
        ignore_unknown_values=True,
        schema=SCHEMA,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    print(f"Chargement depuis {GCS_URI}...")
    load_job = bq_client.load_table_from_uri(GCS_URI, TABLE_ID, job_config=job_config)
    load_job.result()

    table = bq_client.get_table(TABLE_ID)
    print(f"✅ Table {TABLE_ID} chargée : {table.num_rows} lignes.")


if __name__ == "__main__":
    load_communes_csv_to_raw()