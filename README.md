# DataTalent — Pipeline data (GCP)

Réponse métier visée : **où recrute-t-on des Data Engineers en France, dans quelles entreprises et à quels salaires ?**

Stack : **Google Cloud** (Cloud Run Job, BigQuery, Cloud Storage, Secret Manager), **Python** (ingestion), **dbt** (transformations Medallion), **Terraform** (IaC).

**Sirene** : enrichissement via **API INSEE** (pas le stock Parquet national), pour limiter volume et coût sur GCP — choix validé avec le formateur.

---

## Structure du dépôt (prod)

| Dossier | Rôle |
|---------|------|
| **`scripts/`** | Ingestion : France Travail (OAuth2 + cache token), Adzuna, communes (CSV), Sirene API → **GCS** + **BigQuery `raw`**. Point d’entrée : `python -m scripts.pipeline.run_ingestion_job`. |
| **`dbt/`** | Modèles **staging / intermediate / marts**, snapshot SCD2, tests. Voir `dbt/README.md`. |
| **`terraform/`** | Bucket raw, datasets BQ, comptes de service, secrets, **Cloud Run Job** ; module **Cloud Scheduler** (désactivé par défaut dans `dev.tfvars`). Voir `terraform/README.md`. |
| **`Dockerfile`** / **`docker-compose.yml`** | Image minimale (`COPY scripts/` + `requirements.txt`) pour le job cloud et tests locaux. |
| **`dashboard/`** | **Streamlit** : lecture des tables **`marts`** sur **BigQuery** (GCP) via ADC — pas d’hébergement obligatoire, les données restent dans le cloud. |

Prototype SQLite et anciens SQL : **`archive/`** (voir `archive/README.md`). L’ancien dashboard archivé reste dans `archive/dashboard/` ; l’app active est **`dashboard/`**.

---

## Prérequis

- Python **3.11+**, **Docker** (optionnel), **gcloud** + **ADC** (`gcloud auth application-default login`).
- Compte / secrets : France Travail, Adzuna, clé **Sirene INSEE**, projet GCP.
- Copier **`.env.example`** → **`.env`** à la racine (non versionné).

Variables typiques : `FRANCE_TRAVAIL_*`, `ADZUNA_*`, `SIRENE_API_KEY`, `GCS_BUCKET_NAME`, `BQ_PROJECT_ID`.

---

## Ingestion locale (Docker)

```bash
docker compose build ingestion
docker compose run --rm ingestion
```

---

## Ingestion cloud

1. Construire et pousser l’image (ex. Artifact Registry).
2. Mettre à jour le **Cloud Run Job** avec cette image.
3. Exécuter :  
   `gcloud run jobs execute datatalent-ingestion --region=europe-west1 --project=VOTRE_PROJET --wait`

Le **planning quotidien** (Cloud Scheduler) est géré par Terraform mais **`create_scheduler = false`** dans `terraform/dev.tfvars.example` pour limiter les exécutions en phase projet ; passer à `true` + `terraform apply` pour l’activer.

---

## Transformations (dbt)

Environnement dédié dans **`dbt/`** (évite les conflits de deps avec le projet racine) :

```bash
cd dbt
uv sync
# Raw (job) → staging offres + communes, puis int / snapshot / marts (voir dbt/README.md)
uv run dbt run --select stg_offres_enriched_clean stg_offres_staging stg_adzuna_jobs_clean stg_communes_france
uv run dbt run --select int_adzuna_company_metrics int_offres_all_sources_source
uv run dbt snapshot --select snp_offres_all_sources_scd2
uv run dbt run --select int_offres_all_sources_current
uv run dbt run --select marts
uv run dbt test
```

Profil : copier `dbt/profiles.yml.example` vers `~/.dbt/profiles.yml` (voir `dbt/README.md`).

---

## Dashboard (Streamlit → BigQuery GCP)

L’UI tourne **en local** ; elle interroge **directement BigQuery** (dataset `marts` par défaut), comme le ferait Looker Studio ou un notebook avec le client Python.

```bash
# Même authentification que pour dbt / scripts (ADC)
gcloud auth application-default login

cd <racine-du-depot>
uv sync
uv run streamlit run dashboard/app.py
```

`BQ_PROJECT_ID` dans `.env` doit correspondre au projet où les marts ont été construits (`dbt build`).

---

## Terraform

```bash
cd terraform
terraform init
terraform plan -var-file=dev.tfvars
terraform apply -var-file=dev.tfvars
```

Utiliser **`dev.tfvars.example`** comme modèle ; les fichiers `*.tfvars` sensibles restent hors Git (voir `.gitignore`).

---

## Documentation complémentaire

- **`dbt/README.md`** — cartographie datasets BigQuery, préfixes `int_` / `snp_` / `mart_`, ordre des `dbt run`.
- **`dashboard/app.py`** — exploration visuelle des marts sur GCP (BigQuery).
- **`terraform/README.md`** — modules, variables, sorties.
- **`archive/README.md`** — contenu archivé (SQLite, Streamlit, SQL legacy, notes).
