variable "project_id" {
  description = "GCP project ID"
  type        = string
  default     = "datatalent-simplon"
}

variable "project_name" {
  description = "Project short name used in resource names"
  type        = string
  default     = "datatalent"
}

variable "pipeline_service_account_id" {
  description = "Service account ID dedicated to the data pipeline"
  type        = string
  default     = "datatalent-pipeline"
}

variable "pipeline_service_account_display_name" {
  description = "Display name for the dedicated pipeline service account"
  type        = string
  default     = "DataTalent Pipeline Service Account"
}

variable "scheduler_service_account_id" {
  description = "Service account ID dedicated to Cloud Scheduler trigger"
  type        = string
  default     = "datatalent-scheduler"
}

variable "scheduler_service_account_display_name" {
  description = "Display name for the dedicated scheduler service account"
  type        = string
  default     = "DataTalent Scheduler Service Account"
}

variable "region" {
  description = "Primary GCP region for serverless resources"
  type        = string
  default     = "europe-west1"
}

variable "bq_location" {
  description = "BigQuery dataset location"
  type        = string
  default     = "EU"
}

variable "bucket_location" {
  description = "GCS bucket location"
  type        = string
  default     = "EU"
}

variable "labels" {
  description = "Common labels applied to resources"
  type        = map(string)
  default = {
    app         = "datatalent"
    managed_by  = "terraform"
    environment = "dev"
  }
}

variable "pipeline_image" {
  description = "Container image used by Cloud Run Job"
  type        = string
  default     = "europe-west1-docker.pkg.dev/cloudrun/container/job:latest"
}

variable "scheduler_cron" {
  description = "Cron schedule for ingestion trigger"
  type        = string
  default     = "0 6 * * *"
}

variable "scheduler_timezone" {
  description = "Timezone used by Cloud Scheduler"
  type        = string
  default     = "Europe/Paris"
}

variable "secret_names" {
  description = "Secret names managed in Secret Manager"
  type        = list(string)
  default = [
    "france-travail-client-id",
    "france-travail-client-secret",
    "adzuna-app-id",
    "adzuna-app-key",
    "sirene-api-key"
  ]
}

variable "manage_project_iam" {
  description = "If true, Terraform manages project-level IAM role bindings"
  type        = bool
  default     = false
}

variable "pipeline_service_account_roles" {
  description = "Project IAM roles assigned to the dedicated pipeline service account"
  type        = list(string)
  default = [
    "roles/bigquery.jobUser",
    "roles/logging.logWriter"
  ]
}

variable "scheduler_service_account_roles" {
  description = "Project IAM roles assigned to the scheduler service account"
  type        = list(string)
  default = [
    "roles/run.invoker"
  ]
}

variable "manage_secret_iam" {
  description = "If true, Terraform manages IAM bindings on secrets"
  type        = bool
  default     = false
}

variable "manage_dataset_iam" {
  description = "If true, Terraform manages dataset-level IAM for the pipeline service account"
  type        = bool
  default     = false
}

variable "pipeline_bigquery_dataset_role" {
  description = "Dataset-level BigQuery role granted to the pipeline service account"
  type        = string
  default     = "roles/bigquery.dataEditor"
}

variable "manage_bucket_iam" {
  description = "If true, Terraform manages bucket-level IAM for the pipeline service account"
  type        = bool
  default     = false
}

variable "pipeline_bucket_roles" {
  description = "Bucket-level Storage roles granted to the pipeline service account"
  type        = list(string)
  default = [
    "roles/storage.objectViewer",
    "roles/storage.objectCreator"
  ]
}

variable "manage_dataset_labels" {
  description = "If true, Terraform updates labels on imported BigQuery datasets"
  type        = bool
  default     = false
}

variable "create_cloud_run_job" {
  description = "If true, Terraform creates Cloud Run job"
  type        = bool
  default     = false
}

variable "create_scheduler" {
  description = "If true, Terraform creates Cloud Scheduler trigger"
  type        = bool
  default     = false
}

variable "ingest_mots_cles" {
  description = "Keywords used by the ingestion job"
  type        = string
  default     = "data engineer"
}

variable "ingest_max_results" {
  description = "Maximum number of offers fetched per source"
  type        = number
  default     = 3000
}
