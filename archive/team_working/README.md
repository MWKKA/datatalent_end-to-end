# Prototype SQLite — offres + Sirene (hors prod GCP)

Non utilisé par le **Cloud Run Job** ni par l’image Docker. Pour le pipeline BigQuery, voir `scripts/pipeline/run_ingestion_job.py` et `dbt/`.

## Fichiers

- `db/team_working.db` — base locale (générée, ne pas versionner les fichiers `.db` lourds).
- `scripts/init_team_working_db.py` — schéma SQLite.
- `scripts/ingest_offres_raw.py` — offres France Travail → SQLite.
- `scripts/enrich_companies_sirene.py` — appel API Sirene INSEE.
- `sql/` — requêtes de contrôle / jointures locales.

## Exécution (depuis la racine du dépôt)

Le package s’appelle toujours `team_working` mais vit sous **`archive/`** : il faut **`PYTHONPATH`**.

```bash
cd /chemin/vers/datatalent_end-to-end
export PYTHONPATH="$(pwd)/archive"

python -m team_working.scripts.init_team_working_db
python -m team_working.scripts.ingest_offres_raw --mots-cles "data engineer" --max-results 1150
python -m team_working.scripts.enrich_companies_sirene
```

Variables d’environnement : voir `.env.example` à la racine (clés FT, Adzuna, Sirene, etc.).
