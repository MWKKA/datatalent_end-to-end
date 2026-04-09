resource "google_cloud_run_v2_job" "ingestion" {
  name     = var.job_name
  location = var.region
  project  = var.project_id
  labels   = var.labels

  template {
    task_count  = 1
    parallelism = 1

    template {
      service_account = var.service_account_email
      max_retries     = 1
      timeout         = "3600s"

      containers {
        image = var.image

        resources {
          limits = {
            cpu    = "1"
            memory = "512Mi"
          }
        }
      }
    }
  }
}
