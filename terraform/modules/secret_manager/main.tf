resource "google_secret_manager_secret" "secrets" {
  for_each = toset(var.secret_names)

  project   = var.project_id
  secret_id = each.value
  labels    = var.labels

  replication {
    auto {}
  }
}

resource "google_secret_manager_secret_iam_member" "accessor" {
  for_each = var.manage_secret_iam ? toset(var.secret_names) : toset([])

  project   = var.project_id
  secret_id = google_secret_manager_secret.secrets[each.value].secret_id
  role      = "roles/secretmanager.secretAccessor"
  member    = var.accessor_member
}
