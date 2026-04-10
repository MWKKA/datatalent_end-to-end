# Archive — hors pipeline production

Ce dossier regroupe du **code et des notes hors chemin critique** (Cloud Run + BigQuery + dbt + Terraform).

| Élément | Contenu |
|---------|---------|
| **`team_working/`** | Prototype **SQLite** (ingestion offres FT, enrichissement Sirene API, exports NDJSON). Non utilisé par `scripts/pipeline/run_ingestion_job.py`. |
| **`dashboard/`** | **Streamlit** de visualisation locale sur les marts BQ (non livrable final). |
| **`sql_legacy/`** | Anciens scripts SQL de référence ; les transformations actives sont dans **`dbt/models/`**. |
| **`docs/`** | Notes de cadrage (`brief.md`, `etat_des_lieuxs.md`). |

## Réexécuter le prototype SQLite (`team_working`)

À lancer depuis la **racine du dépôt** :

```bash
export PYTHONPATH="$(pwd)/archive"
python -m team_working.scripts.init_team_working_db
python -m team_working.scripts.ingest_offres_raw --mots-cles "data engineer" --max-results 500
python -m team_working.scripts.enrich_companies_sirene
```

(Détails : `archive/team_working/README.md`.)

## Streamlit archivé

```bash
uv run streamlit run archive/dashboard/app.py
```

(Voir `archive/dashboard/README.md` — dépendances : `streamlit`, `altair`, client BQ, etc.)
