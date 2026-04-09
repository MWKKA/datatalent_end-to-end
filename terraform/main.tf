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
  dataset_ids = ["staging", "intermediate", "marts"]
  labels      = var.manage_dataset_labels ? var.labels : {}

  depends_on = [google_project_service.required]
}

module "service_account" {
  source = "./modules/service_account"

  project_id   = var.project_id
  account_id   = "${var.project_name}-pipeline"
  display_name = "DataTalent Pipeline Service Account"
  project_roles = var.manage_project_iam ? [
    "roles/bigquery.dataEditor",
    "roles/bigquery.jobUser",
    "roles/logging.logWriter",
    "roles/run.developer",
    "roles/run.invoker",
    "roles/secretmanager.secretAccessor",
    "roles/storage.objectAdmin"
  ] : []

  depends_on = [google_project_service.required]
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

  depends_on = [google_project_service.required]
}

module "scheduler" {
  count  = var.create_scheduler && var.create_cloud_run_job ? 1 : 0
  source = "./modules/scheduler"

  project_id                = var.project_id
  region                    = var.region
  job_name                  = "${var.project_name}-daily-ingestion"
  schedule                  = var.scheduler_cron
  time_zone                 = var.scheduler_timezone
  target_uri                = module.cloud_run_job[0].run_uri
  scheduler_service_account = module.service_account.email

  depends_on = [google_project_service.required]
}
