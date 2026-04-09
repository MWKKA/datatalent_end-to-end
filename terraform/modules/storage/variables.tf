variable "project_id" {
  type = string
}

variable "bucket_name" {
  type = string
}

variable "bucket_location" {
  type = string
}

variable "labels" {
  type    = map(string)
  default = {}
}

variable "force_destroy" {
  type    = bool
  default = false
}
