resource "google_cloud_scheduler_job" "trigger_job" {
  project   = var.project_id
  region    = var.region
  name      = var.job_name
  schedule  = var.schedule
  time_zone = var.time_zone

  http_target {
    uri         = var.target_uri
    http_method = "POST"

    oauth_token {
      service_account_email = var.scheduler_service_account
      scope                 = "https://www.googleapis.com/auth/cloud-platform"
    }
  }
}
