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
    "france-travail-client-secret"
  ]
}

variable "manage_project_iam" {
  description = "If true, Terraform manages project-level IAM role bindings"
  type        = bool
  default     = false
}

variable "manage_secret_iam" {
  description = "If true, Terraform manages IAM bindings on secrets"
  type        = bool
  default     = false
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
