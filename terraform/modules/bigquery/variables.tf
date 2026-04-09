variable "project_id" {
  type = string
}

variable "location" {
  type = string
}

variable "dataset_ids" {
  type = list(string)
}

variable "labels" {
  type    = map(string)
  default = {}
}
