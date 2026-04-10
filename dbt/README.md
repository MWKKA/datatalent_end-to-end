# dbt BigQuery - DataTalent

Aligné sur le schéma équipe :

- **Raw** : non géré par dbt (tables BigQuery alimentées par le job Cloud Run : `raw_offres_team`, `offres_enriched_team`, `communes_raw`, etc.).
- **Staging** : **dbt** matérialise dans le dataset `staging` les tables attendues en aval (alias BQ inchangés) :
  - `stg_offres_enriched_clean` → table **`offres_enriched_clean`** (jointure `offres_enriched_team` × `raw_offres_team`)
  - `stg_offres_staging` → **`offres_staging`** (lignes France Travail, champs depuis `payload_json`)
  - `stg_adzuna_jobs_clean` → **`adzuna_jobs_clean`** (lignes Adzuna pour `int_adzuna_company_metrics`)
  - **`stg_communes_france`** (typage depuis `raw.communes_raw`)
- **Intermediate** : `int_adzuna_company_metrics`, `int_offres_all_sources_source`, snapshot SCD2, vue `int_offres_all_sources_current`.
- **Marts** : `mart_offres_clean` et tables dérivées (KPI, geo, etc.).

SCD2 (snapshot dbt) : colonne de versionnement **`update_date`**. Comme `os.date_actualisation` est souvent NULL quand la jointure `offres_staging` ne matche pas, `update_date` est calculée par `COALESCE(os.date_actualisation, oe.last_seen_at, oe.enriched_at)` pour que le snapshot garde une ligne par offre (sinon le mart se vide).

## Carte BigQuery (pour s’y retrouver)

Règle simple : **dashboard / analyse métier → dataset `marts`** uniquement. Le reste est la « cuisine ».

| Dataset (BQ) | Rôle | Alimenté par |
|----------------|------|----------------|
| **`raw`** | Données brutes / MERGE (offres, communes, Sirene…) | Job Cloud Run (`run_ingestion_job`) |
| **`staging`** | Offres + communes **typées / jointes** (dbt) | **dbt** (`stg_*` avec alias `offres_enriched_clean`, `offres_staging`, `adzuna_jobs_clean`, `stg_communes_france`) |
| **`intermediate`** | Préparation + **historique SCD2** | **dbt** (`int_*`, `snp_*`, vue `int_offres_all_sources_current`) |
| **`marts`** | Tables prêtes consommation (KPI, geo, filtres DE…) | **dbt** |

Préfixes **dans les noms de tables** (repères, pas obligation BQ) :

- **`stg_`** : staging dbt (ex. communes typées depuis `raw`).
- **`int_`** : modèle intermédiaire (jointures / enrichissements avant mart).
- **`snp_`** : **snapshot** dbt = historique SCD2 (plusieurs lignes dans le temps pour une même offre).
- **`mart_`** : mart analytique.

Après chaque run d’ingestion **raw**, lancer dbt sur le staging (au minimum) pour rafraîchir `offres_enriched_clean`, `offres_staging` et `adzuna_jobs_clean` avant les `int_*` et le snapshot.

## Prérequis

- **`uv`** : ce dossier a son propre **`pyproject.toml`** + **`uv.lock`** (venv `dbt/.venv`). Comme ça tu gardes **`uv run dbt`** comme avant, sans mélanger avec le venv du projet racine (où `google-cloud-storage` et d’autres libs sont plus récentes et feraient échouer une install commune).
- Depuis **`dbt/`** : `uv sync` une fois, puis toutes les commandes en **`uv run dbt ...`**.
- Authentification GCP (ADC).

## Profil

Copier `profiles.yml.example` vers `~/.dbt/profiles.yml`.

## Ordre d’exécution (depuis `dbt/`)

```bash
cd dbt
uv sync
uv run dbt debug
# Staging offres (raw → staging) puis chaîne intermédiaire
uv run dbt run --select stg_offres_enriched_clean stg_offres_staging stg_adzuna_jobs_clean stg_communes_france
uv run dbt test --select stg_offres_enriched_clean stg_offres_staging stg_adzuna_jobs_clean stg_communes_france
uv run dbt run --select int_adzuna_company_metrics int_offres_all_sources_source
uv run dbt snapshot --select snp_offres_all_sources_scd2
uv run dbt run --select int_offres_all_sources_current
uv run dbt run --select marts
uv run dbt test
```

En une commande (graphe dbt) : `uv run dbt run` depuis `dbt/` exécute les dépendances dans le bon ordre.

## Fichiers clés

- `pyproject.toml` / `uv.lock` (environnement dbt isolé)
- `models/staging/stg_offres_enriched_clean.sql`, `stg_offres_staging.sql`, `stg_adzuna_jobs_clean.sql`
- `models/staging/stg_communes_france.sql`
- `models/intermediate/int_adzuna_company_metrics.sql`
- `models/intermediate/int_offres_all_sources_source.sql`
- `snapshots/snp_offres_all_sources_scd2.sql`
- `models/intermediate/int_offres_all_sources_current.sql`
- `models/marts/mart_offres_clean.sql`
