output "raw_bucket_name" {
  description = "Raw ingestion bucket name"
  value       = module.storage.bucket_name
}

output "pipeline_service_account" {
  description = "Service account email used by Cloud Run pipeline job"
  value       = module.service_account.email
}

output "scheduler_service_account" {
  description = "Service account email used by Cloud Scheduler trigger"
  value       = module.scheduler_service_account.email
}

output "cloud_run_job_name" {
  description = "Cloud Run Job created by Terraform"
  value       = try(module.cloud_run_job[0].job_name, null)
}

output "scheduler_job_name" {
  description = "Cloud Scheduler job name"
  value       = try(module.scheduler[0].job_name, null)
}
