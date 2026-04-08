# Publication BigQuery - team_working

Ce document sert de validation equipe avant publication vers BigQuery.

## Contexte

- Source locale: `team_working/db/team_working.db`
- Bucket raw: `datatalent-raw-simplon`
- Projet BigQuery: `datatalent-simplon`
- Dataset cible: `raw`

## Script de publication

- Script: `scripts/cloud/publish_team_working_to_gcp.py`
- Flux: SQLite -> NDJSON -> GCS -> BigQuery
- Mode recommande: `WRITE_TRUNCATE` (snapshot complet)

Commande:

```bash
uv run python scripts/cloud/publish_team_working_to_gcp.py --dataset raw --write-disposition WRITE_TRUNCATE
```

## Objets GCS ecrits

- `gs://datatalent-raw-simplon/raw-sirene/raw_offres_team.ndjson`
- `gs://datatalent-raw-simplon/raw-sirene/raw_sirene_results_team.ndjson`
- `gs://datatalent-raw-simplon/raw-sirene/offres_enriched_team.ndjson`

## Tables BigQuery ecrites

- `datatalent-simplon.raw.raw_offres_team`
- `datatalent-simplon.raw.raw_sirene_results_team`
- `datatalent-simplon.raw.offres_enriched_team`

## Decomposition medallion BigQuery (apres publication raw)

Ordre d'execution recommande:

1. `sql/staging/stg_offres_ft_bq.sql`
2. `sql/staging/stg_offres_adzuna_bq.sql`
3. `sql/staging/stg_offres_all_bq.sql`
4. `sql/intermediate/slv_offres_sirene_bq.sql`
5. `sql/marts/mart_offres_matching_kpi_bq.sql`

Tables cibles:

- Staging:
  - `datatalent-simplon.staging.stg_offres_ft`
  - `datatalent-simplon.staging.stg_offres_adzuna`
  - `datatalent-simplon.staging.stg_offres_all`
- Silver / Intermediate:
  - `datatalent-simplon.intermediate.slv_offres_sirene`
- Marts:
  - `datatalent-simplon.marts.mart_offres_matching_kpi`

## Validation equipe (avant execution)

- [ ] Bucket et projet confirmes
- [ ] Permissions service account confirmees
- [ ] Mode de chargement confirme (`WRITE_TRUNCATE` ou `WRITE_APPEND`)
- [ ] Execution validee par l'equipe
