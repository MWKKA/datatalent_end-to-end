variable "project_id" {
  type = string
}

variable "secret_names" {
  type = list(string)
}

variable "labels" {
  type    = map(string)
  default = {}
}

variable "accessor_member" {
  type = string
}

variable "manage_secret_iam" {
  type    = bool
  default = false
}
