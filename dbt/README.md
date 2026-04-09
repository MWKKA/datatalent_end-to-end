# dbt BigQuery - DataTalent

Projet dbt base sur le flux valide du groupe:
- `raw -> staging -> intermediate -> marts`
- SCD2 pilote par `delete_update` (snapshot dbt)
- sans dependance `adzuna_company_metrics`

## 1) Prerequis

- Installer dbt BigQuery:
  - `pip install dbt-bigquery`
- Authentification GCP active (ADC ou service account)

## 2) Profil dbt

Copier `profiles.yml.example` vers votre dossier profils dbt:

- Linux/WSL: `~/.dbt/profiles.yml`

## 3) Run ordre recommande

Depuis le dossier `dbt/`:

- `dbt debug`
- `dbt deps` (optionnel si packages plus tard)
- `dbt run --select stg_offres_enriched_clean int_offres_all_sources_source`
- `dbt snapshot --select snp_offres_all_sources_scd2`
- `dbt run --select int_offres_all_sources_current`
- `dbt run --select marts`
- `dbt test`

## 4) Modeles principaux

- `models/staging/stg_offres_enriched_clean.sql`
- `models/intermediate/int_offres_all_sources_source.sql`
- `snapshots/snp_offres_all_sources_scd2.sql`
- `models/intermediate/int_offres_all_sources_current.sql`
- `models/marts/mart_offres_clean.sql`
