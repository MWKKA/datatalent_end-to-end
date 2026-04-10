locals {
  required_apis = toset([
    "artifactregistry.googleapis.com",
    "bigquery.googleapis.com",
    "cloudscheduler.googleapis.com",
    "iam.googleapis.com",
    "run.googleapis.com",
    "secretmanager.googleapis.com",
    "storage.googleapis.com"
  ])
}

resource "google_project_service" "required" {
  for_each           = local.required_apis
  project            = var.project_id
  service            = each.key
  disable_on_destroy = false
}

module "storage" {
  source = "./modules/storage"

  project_id      = var.project_id
  bucket_name     = "${var.project_name}-raw-${var.project_id}"
  bucket_location = var.bucket_location
  labels          = var.labels
  force_destroy   = false

  depends_on = [google_project_service.required]
}

module "bigquery" {
  source = "./modules/bigquery"

  project_id  = var.project_id
  location    = var.bq_location
  dataset_ids = ["raw", "staging", "intermediate", "marts"]
  labels      = var.manage_dataset_labels ? var.labels : {}

  depends_on = [google_project_service.required]
}

module "service_account" {
  source = "./modules/service_account"

  project_id   = var.project_id
  account_id   = var.pipeline_service_account_id
  display_name = var.pipeline_service_account_display_name
  project_roles = var.manage_project_iam ? var.pipeline_service_account_roles : []

  depends_on = [google_project_service.required]
}

module "scheduler_service_account" {
  source = "./modules/service_account"

  project_id   = var.project_id
  account_id   = var.scheduler_service_account_id
  display_name = var.scheduler_service_account_display_name
  project_roles = var.manage_project_iam ? var.scheduler_service_account_roles : []

  depends_on = [google_project_service.required]
}

resource "google_bigquery_dataset_iam_member" "pipeline_dataset_editor" {
  for_each = var.manage_dataset_iam ? toset(module.bigquery.dataset_ids) : toset([])

  project    = var.project_id
  dataset_id = each.value
  role       = var.pipeline_bigquery_dataset_role
  member     = "serviceAccount:${module.service_account.email}"
}

resource "google_storage_bucket_iam_member" "pipeline_bucket_roles" {
  for_each = var.manage_bucket_iam ? toset(var.pipeline_bucket_roles) : toset([])

  bucket = module.storage.bucket_name
  role   = each.value
  member = "serviceAccount:${module.service_account.email}"
}

module "secret_manager" {
  source = "./modules/secret_manager"

  project_id        = var.project_id
  secret_names      = var.secret_names
  labels            = var.labels
  accessor_member   = "serviceAccount:${module.service_account.email}"
  manage_secret_iam = var.manage_secret_iam

  depends_on = [google_project_service.required]
}

module "cloud_run_job" {
  count  = var.create_cloud_run_job ? 1 : 0
  source = "./modules/cloud_run_job"

  project_id            = var.project_id
  region                = var.region
  job_name              = "${var.project_name}-ingestion"
  image                 = var.pipeline_image
  service_account_email = module.service_account.email
  labels                = var.labels
  env_vars = {
    GCS_BUCKET_NAME    = module.storage.bucket_name
    BQ_PROJECT_ID      = var.project_id
    BQ_DATASET_RAW     = "raw"
    INGEST_MOTS_CLES   = var.ingest_mots_cles
    INGEST_MAX_RESULTS = tostring(var.ingest_max_results)
  }
  secret_env_vars = {
    FRANCE_TRAVAIL_CLIENT_ID     = "france-travail-client-id"
    FRANCE_TRAVAIL_CLIENT_SECRET = "france-travail-client-secret"
    ADZUNA_APP_ID                = "adzuna-app-id"
    ADZUNA_APP_KEY               = "adzuna-app-key"
    SIRENE_API_KEY               = "sirene-api-key"
  }

  depends_on = [google_project_service.required]
}

# Déclenchement quotidien du job d'ingestion (Cloud Scheduler → Cloud Run Job).
# Désactivable via var.create_scheduler (ex. tfvars scolaire) sans retirer ce code : la soutenance peut montrer le module complet.
module "scheduler" {
  count  = var.create_scheduler && var.create_cloud_run_job ? 1 : 0
  source = "./modules/scheduler"

  project_id                = var.project_id
  region                    = var.region
  job_name                  = "${var.project_name}-daily-ingestion"
  schedule                  = var.scheduler_cron
  time_zone                 = var.scheduler_timezone
  target_uri                = module.cloud_run_job[0].run_uri
  scheduler_service_account = module.scheduler_service_account.email

  depends_on = [google_project_service.required]
}
