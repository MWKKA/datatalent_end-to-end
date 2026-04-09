import time
from google.cloud import bigquery

PROJECT = "datatalent-simplon"
RAW_TABLE_ID = f"{PROJECT}.raw.communes_raw"

STAGING_DATASET = f"{PROJECT}.staging"


def create_staging_tables(bq_client: bigquery.Client):
    # Création du dataset staging si absent
    dataset_ref = bigquery.Dataset(STAGING_DATASET)
    dataset_ref.location = "EU"
    bq_client.create_dataset(dataset_ref, exists_ok=True)
    print("Dataset 'staging' prêt.")

    # dim_region
    bq_client.create_table(bigquery.Table(
        f"{STAGING_DATASET}.dim_region",
        schema=[
            bigquery.SchemaField("reg_code", "INT64"),
            bigquery.SchemaField("reg_nom", "STRING"),
        ]
    ), exists_ok=True)

    # dim_departement
    bq_client.create_table(bigquery.Table(
        f"{STAGING_DATASET}.dim_departement",
        schema=[
            bigquery.SchemaField("dep_code", "STRING"),
            bigquery.SchemaField("dep_nom", "STRING"),
            bigquery.SchemaField("reg_code", "INT64"),
        ]
    ), exists_ok=True)

    # dim_commune
    bq_client.create_table(bigquery.Table(
        f"{STAGING_DATASET}.dim_commune",
        schema=[
            bigquery.SchemaField("commune_code_insee", "STRING"),
            bigquery.SchemaField("commune_name", "STRING"),
            bigquery.SchemaField("postal_code", "STRING"),
            bigquery.SchemaField("dep_code", "STRING"),
            bigquery.SchemaField("latitude", "FLOAT64"),
            bigquery.SchemaField("longitude", "FLOAT64"),
            bigquery.SchemaField("population", "INT64"),
            bigquery.SchemaField("densite", "FLOAT64"),
            bigquery.SchemaField("zone_emploi", "STRING"),
        ]
    ), exists_ok=True)

    print("Tables dim_region, dim_departement, dim_commune prêtes.")
    time.sleep(2)


def load_geo_staging(bq_client: bigquery.Client):

    # dim_region
    print("Chargement dim_region...")
    bq_client.query(f"""
        CREATE OR REPLACE TABLE `{STAGING_DATASET}.dim_region` AS
        SELECT DISTINCT
            SAFE_CAST(reg_code AS INT64) AS reg_code,
            reg_nom
        FROM `{RAW_TABLE_ID}`
        WHERE reg_code IS NOT NULL
        ORDER BY reg_code
    """).result()
    print("✅ dim_region chargée.")

    # dim_departement
    print("Chargement dim_departement...")
    bq_client.query(f"""
        CREATE OR REPLACE TABLE `{STAGING_DATASET}.dim_departement` AS
        SELECT DISTINCT
            dep_code,
            dep_nom,
            SAFE_CAST(reg_code AS INT64) AS reg_code
        FROM `{RAW_TABLE_ID}`
        WHERE dep_code IS NOT NULL
        ORDER BY dep_code
    """).result()
    print("✅ dim_departement chargée.")

    # dim_commune
    print("Chargement dim_commune...")
    bq_client.query(f"""
        CREATE OR REPLACE TABLE `{STAGING_DATASET}.dim_commune` AS
        SELECT DISTINCT
            code_insee                              AS commune_code_insee,
            nom_standard                            AS commune_name,
            code_postal                             AS postal_code,
            dep_code,
            SAFE_CAST(latitude_centre AS FLOAT64)   AS latitude,
            SAFE_CAST(longitude_centre AS FLOAT64)  AS longitude,
            SAFE_CAST(population AS INT64)          AS population,
            SAFE_CAST(densite AS FLOAT64)           AS densite,
            zone_emploi
        FROM `{RAW_TABLE_ID}`
        WHERE code_insee IS NOT NULL
        ORDER BY code_insee
    """).result()
    print("✅ dim_commune chargée.")


def main():
    bq_client = bigquery.Client(project=PROJECT)
    create_staging_tables(bq_client)
    load_geo_staging(bq_client)
    print("\n✅ Toutes les tables géographiques sont en staging.")


if __name__ == "__main__":
    main()