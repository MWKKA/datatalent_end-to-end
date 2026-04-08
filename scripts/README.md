# Scripts du projet

- **core/common.py** : utilitaires partages (chargement `.env`, chemins projet, normalisation).
- **ingestion/download_geo.py** : telecharge regions/departements/communes dans `data/raw/geo/`.
- **ingestion/france_travail_auth.py** : auth OAuth2 Client Credentials (cache token).
- **ingestion/download_offres.py** : recupere les offres API dans `data/raw/france_travail/offres_<slug>.json`.
- **database/load_local_db.py** : charge les donnees raw en SQLite (`data/local.db` ou `DATABASE_PATH`).
- **matching/service.py** : logique metier de rapprochement offres ↔ Sirene.
- **matching/match_offres_sirene.py** : script CLI de matching qui alimente `offres_avec_siren`.
- **cloud/publish_team_working_to_gcp.py** : exporte `team_working.db` en NDJSON, envoie sur GCS raw, puis charge dans BigQuery raw.
- Les téléchargements Sirene se font manuellement depuis data.gouv.fr (voir `docs/SETUP_LOCAL.md`).

## Publication team_working -> GCP

Prerequis:
- `GOOGLE_APPLICATION_CREDENTIALS` pointe vers un service account ayant acces GCS + BigQuery.
- `.env` contient `GCS_BUCKET_NAME` et `BQ_PROJECT_ID`.

Commande:

```bash
uv run python scripts/cloud/publish_team_working_to_gcp.py --dataset raw --write-disposition WRITE_TRUNCATE
```

Tables publiees:
- `raw_offres_team`
- `raw_sirene_results_team`
- `offres_enriched_team`
