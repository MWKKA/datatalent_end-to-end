# Terraform - DataTalent

Cette configuration Terraform provisionne une base d'infrastructure GCP pour le projet DataTalent :

- bucket GCS `raw`
- datasets BigQuery `raw`, `staging`, `intermediate`, `marts`
- service account pipeline + IAM
- secrets de base dans Secret Manager
- Cloud Run Job pour l'ingestion
- Cloud Scheduler (module présent ; désactivé par défaut dans `dev.tfvars` via `create_scheduler = false` — passer à `true` pour le planning quotidien)

## Prérequis

- Terraform >= 1.6
- `gcloud auth application-default login`
- droits suffisants sur le projet `datatalent-simplon`

## Utilisation

```bash
cd terraform
terraform init
terraform plan -var-file=dev.tfvars
terraform apply -var-file=dev.tfvars
```

## Variables importantes

- `project_id` : `datatalent-simplon`
- `project_name` : `datatalent`
- `region` : `europe-west1`
- `bq_location` : `EU`
- `pipeline_image` : image du job d'ingestion
- `ingest_mots_cles` : mots-cles utilises par le job
- `ingest_max_results` : volume max d'offres recuperees par source
- `pipeline_service_account_id` : service account dedie au pipeline
- `scheduler_service_account_id` : service account dedie au trigger scheduler
- `pipeline_service_account_roles` : roles IAM projet minimaux du runtime (jobUser + logWriter)
- `scheduler_service_account_roles` : roles IAM projet minimaux du scheduler (run.invoker)
- `manage_project_iam` : active la gestion IAM projet via Terraform
- `manage_dataset_iam` : attribue `bigquery.dataEditor` au niveau dataset
- `manage_bucket_iam` : attribue les roles GCS au niveau bucket

## Image Docker → Artifact Registry → Cloud Run Job

L’image d’ingestion est stockée dans **Artifact Registry** (format `LOCATION-docker.pkg.dev/PROJECT/REPO/IMAGE:TAG`), par ex. :

`europe-west1-docker.pkg.dev/datatalent-simplon/datatalent/pipeline:latest`

(alignée sur `pipeline_image` dans `dev.tfvars`.)

Depuis la **racine du dépôt**, avec **Docker** démarré et `gcloud` sur le projet :

```bash
./scripts/cloud/push_pipeline_image.sh
```

Variables optionnelles : `GCP_PROJECT_ID`, `GCP_REGION`, `IMAGE_TAG`, `CLOUD_RUN_JOB_NAME`.

Après un push, le script exécute `gcloud run jobs update` pour que le job utilise la nouvelle image.

**Cloud Build** (`gcloud builds submit --tag …`) est possible sans Docker local, mais le compte de build doit pouvoir lire/écrire le bucket source Cloud Build (sinon erreur `storage.objects.get` / 403) — à ajuster en IAM si besoin.

## Vérifier que le cron (Cloud Scheduler) est désactivé

Dans `dev.tfvars`, garder `create_scheduler = false` (défaut scolaire). Contrôle côté GCP :

```bash
gcloud scheduler jobs list --project=datatalent-simplon --location=europe-west1
```

Aucune ligne = pas de job planifié pour l’ingestion. Si un job apparaît encore après un ancien `apply`, le supprimer ou repasser `terraform apply` avec `create_scheduler = false`.

## Notes

- Les valeurs sensibles (contenu des secrets) ne sont pas versionnées ici.
- Les secrets sont créés, puis l'alimentation des versions peut se faire via CI/CD.
- Le job Cloud Run lit les secrets via variables d'environnement:
  `FRANCE_TRAVAIL_CLIENT_ID`, `FRANCE_TRAVAIL_CLIENT_SECRET`, `ADZUNA_APP_ID`, `ADZUNA_APP_KEY`, `SIRENE_API_KEY`.
