import json
import time
from datetime import datetime, timezone
from google.cloud import bigquery

PROJECT = "datatalent-simplon"
RAW_TABLE_ID = f"{PROJECT}.raw.offres_raw"
STAGING_TABLE_ID = f"{PROJECT}.staging.offres_staging"


def create_staging_if_not_exists(bq_client: bigquery.Client):
    # Création du dataset staging si absent
    dataset_ref = bigquery.Dataset(f"{PROJECT}.staging")
    dataset_ref.location = "EU"
    bq_client.create_dataset(dataset_ref, exists_ok=True)

    # Création de la table avec le schéma complet
    schema = [
        bigquery.SchemaField("id_offre", "STRING"),
        bigquery.SchemaField("intitule", "STRING"),
        bigquery.SchemaField("description", "STRING"),
        bigquery.SchemaField("date_creation", "TIMESTAMP"),
        bigquery.SchemaField("date_actualisation", "TIMESTAMP"),
        bigquery.SchemaField("lieu_libelle", "STRING"),
        bigquery.SchemaField("lieu_latitude", "FLOAT64"),
        bigquery.SchemaField("lieu_longitude", "FLOAT64"),
        bigquery.SchemaField("lieu_code_postal", "STRING"),
        bigquery.SchemaField("lieu_commune", "STRING"),
        bigquery.SchemaField("rome_code", "STRING"),
        bigquery.SchemaField("rome_libelle", "STRING"),
        bigquery.SchemaField("appellation_libelle", "STRING"),
        bigquery.SchemaField("entreprise_nom", "STRING"),
        bigquery.SchemaField("entreprise_description", "STRING"),
        bigquery.SchemaField("type_contrat", "STRING"),
        bigquery.SchemaField("type_contrat_libelle", "STRING"),
        bigquery.SchemaField("nature_contrat", "STRING"),
        bigquery.SchemaField("experience_exige", "STRING"),
        bigquery.SchemaField("experience_libelle", "STRING"),
        bigquery.SchemaField("duree_travail_libelle", "STRING"),
        bigquery.SchemaField("alternance", "BOOL"),
        bigquery.SchemaField("nombre_postes", "INT64"),
        bigquery.SchemaField("salaire_libelle", "STRING"),
        bigquery.SchemaField("code_naf", "STRING"),
        bigquery.SchemaField("secteur_activite", "STRING"),
        bigquery.SchemaField("secteur_activite_libelle", "STRING"),
        bigquery.SchemaField("url_origine", "STRING"),
        bigquery.SchemaField("url_postulation", "STRING"),
        bigquery.SchemaField("horaires", "STRING"),
        bigquery.SchemaField("entreprise_adaptee", "BOOL"),
        bigquery.SchemaField("employeur_handi_engage", "BOOL"),
        bigquery.SchemaField("ingestion_timestamp", "TIMESTAMP"),
        bigquery.SchemaField("source_file", "STRING"),
        bigquery.SchemaField("staging_timestamp", "TIMESTAMP"),
    ]

    table = bigquery.Table(STAGING_TABLE_ID, schema=schema)
    bq_client.create_table(table, exists_ok=True)
    print(f"Table '{STAGING_TABLE_ID}' prête.")
    time.sleep(2)


def parse_offre(raw_json: str, ingestion_timestamp: str, source_file: str) -> dict:
    offre = json.loads(raw_json)

    lieu = offre.get("lieuTravail") or {}
    entreprise = offre.get("entreprise") or {}
    salaire = offre.get("salaire") or {}
    origine = offre.get("origineOffre") or {}
    contexte = offre.get("contexteTravail") or {}

    return {
        "id_offre": str(offre.get("id")),
        "intitule": offre.get("intitule"),
        "description": offre.get("description"),
        "date_creation": offre.get("dateCreation"),
        "date_actualisation": offre.get("dateActualisation"),
        "lieu_libelle": lieu.get("libelle"),
        "lieu_latitude": lieu.get("latitude"),
        "lieu_longitude": lieu.get("longitude"),
        "lieu_code_postal": lieu.get("codePostal"),
        "lieu_commune": lieu.get("commune"),
        "rome_code": offre.get("romeCode"),
        "rome_libelle": offre.get("romeLibelle"),
        "appellation_libelle": offre.get("appellationlibelle"),
        "entreprise_nom": entreprise.get("nom"),
        "entreprise_description": entreprise.get("description"),
        "type_contrat": offre.get("typeContrat"),
        "type_contrat_libelle": offre.get("typeContratLibelle"),
        "nature_contrat": offre.get("natureContrat"),
        "experience_exige": offre.get("experienceExige"),
        "experience_libelle": offre.get("experienceLibelle"),
        "duree_travail_libelle": (offre.get("dureeTravailLibelle") or "").strip() or None,
        "alternance": offre.get("alternance"),
        "nombre_postes": offre.get("nombrePostes"),
        "salaire_libelle": salaire.get("libelle"),
        "code_naf": offre.get("codeNAF"),
        "secteur_activite": offre.get("secteurActivite"),
        "secteur_activite_libelle": offre.get("secteurActiviteLibelle"),
        "url_origine": origine.get("urlOrigine"),
        "url_postulation": (offre.get("contact") or {}).get("urlPostulation"),
        "horaires": ", ".join(
            h.strip() for h in contexte.get("horaires", []) if h.strip()
        ) or None,
        "entreprise_adaptee": offre.get("entrepriseAdaptee"),
        "employeur_handi_engage": offre.get("employeurHandiEngage"),
        "ingestion_timestamp": ingestion_timestamp,
        "source_file": source_file,
        "staging_timestamp": datetime.now(timezone.utc).isoformat(),
    }


def load_staging():
    bq_client = bigquery.Client(project=PROJECT)

    # Création du dataset + table si absents
    create_staging_if_not_exists(bq_client)

    query = f"SELECT raw_json, ingestion_timestamp, source_file FROM `{RAW_TABLE_ID}`"
    print("Lecture de la table raw...")
    raw_rows = list(bq_client.query(query).result())
    print(f"{len(raw_rows)} lignes récupérées depuis raw.")

    rows = []
    errors_parse = 0
    for row in raw_rows:
        try:
            parsed = parse_offre(
                row.raw_json,
                row.ingestion_timestamp.isoformat(),
                row.source_file
            )
            rows.append(parsed)
        except Exception as e:
            errors_parse += 1
            print(f"Erreur parsing : {e}")

    print(f"{len(rows)} lignes parsées ({errors_parse} erreurs de parsing).")

    if not rows:
        print("Aucune ligne à insérer.")
        return

    batch_size = 500
    total_errors = 0
    for i in range(0, len(rows), batch_size):
        batch = rows[i:i + batch_size]
        errors = bq_client.insert_rows_json(STAGING_TABLE_ID, batch)
        if errors:
            total_errors += len(errors)
            print(f"Erreurs batch {i}-{i + len(batch) - 1}:")
            for error in errors:
                print(error)
        else:
            print(f"Batch {i}-{i + len(batch) - 1} inséré avec succès")

    print(f"\nTerminé. {len(rows) - total_errors}/{len(rows)} lignes insérées.")


if __name__ == "__main__":
    load_staging()