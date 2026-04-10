variable "project_id" {
  type = string
}

variable "region" {
  type = string
}

variable "job_name" {
  type = string
}

variable "image" {
  type = string
}

variable "service_account_email" {
  type = string
}

variable "labels" {
  type    = map(string)
  default = {}
}

variable "env_vars" {
  description = "Plain environment variables for the Cloud Run job container"
  type        = map(string)
  default     = {}
}

variable "secret_env_vars" {
  description = "Secret-backed env vars: map env var name -> Secret Manager secret id"
  type        = map(string)
  default     = {}
}
