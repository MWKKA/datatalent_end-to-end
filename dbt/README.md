# dbt BigQuery - DataTalent

Aligné sur le schéma équipe :

- **Raw** : non géré par dbt (tables déjà en place dans BigQuery).
- **Staging** : consommé via `sources` (`offres_enriched_clean`, `offres_staging`, `adzuna_jobs_clean`).
- **Intermediate** : `int_adzuna_company_metrics`, `int_offres_all_sources_source`, snapshot SCD2, vue `int_offres_all_sources_current`.
- **Marts** : `mart_offres_clean` et tables dérivées (KPI, geo, etc.).

SCD2 (snapshot dbt) : colonne de versionnement **`update_date`**. Comme `os.date_actualisation` est souvent NULL quand la jointure `offres_staging` ne matche pas, `update_date` est calculée par `COALESCE(os.date_actualisation, oe.last_seen_at, oe.enriched_at)` pour que le snapshot garde une ligne par offre (sinon le mart se vide).

## Prérequis

- `uv` + dépendances du projet (`uv pip install dbt-bigquery` dans le venv du repo si besoin).
- Authentification GCP (ADC).

## Profil

Copier `profiles.yml.example` vers `~/.dbt/profiles.yml`.

## Ordre d’exécution (depuis `dbt/`)

```bash
uv run dbt debug
uv run dbt run --select int_adzuna_company_metrics int_offres_all_sources_source
uv run dbt snapshot --select snp_offres_all_sources_scd2
uv run dbt run --select int_offres_all_sources_current
uv run dbt run --select marts
uv run dbt test
```

## Fichiers clés

- `models/intermediate/int_adzuna_company_metrics.sql`
- `models/intermediate/int_offres_all_sources_source.sql`
- `snapshots/snp_offres_all_sources_scd2.sql`
- `models/intermediate/int_offres_all_sources_current.sql`
- `models/marts/mart_offres_clean.sql`
