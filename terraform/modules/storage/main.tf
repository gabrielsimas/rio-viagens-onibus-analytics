variable "project_id" {}
variable "airflow_sa_email" {}
variable "dremio_sa_email" {}

locals {
  buckets = {
    landing = "mvp-rio-transportes-landing"
    bronze  = "mvp-rio-transportes-bronze"
    silver  = "mvp-rio-transportes-silver"
    gold    = "mvp-rio-transportes-gold"
  }
}

resource "random_id" "bucket_suffix" { byte_length = 2 }

resource "google_storage_bucket" "data_lake" {
  for_each = local.buckets
  name = "${each.value}-${random_id.bucket_suffix.hex}"
  location = "US"
  force_destroy = true
  uniform_bucket_level_access = true
}

# --- PERMISSÕES ---
# Airflow -> Landing e Bronze
resource "google_storage_bucket_iam_member" "airflow_access" {
  for_each = toset(["landing","bronze"])
  bucket = google_storage_bucket.data_lake[each.key].name
  role = "roles/storage.objectAdmin"
  member = "serviceAccount:${var.airflow_sa_email}"
}

# Dremio -> Bronze, Silver e Gold
resource "google_storage_bucket_iam_member" "dremio_access" {
  for_each = toset(["bronze","silver","gold"])

  bucket = google_storage_bucket.data_lake[each.key].name
  role = "roles/storage.objectAdmin"
  member = "serviceAccount:${var.dremio_sa_email}"
}

output "bucket_names" {
  value = { for k, v in google_storage_bucket.data_lake : k => v.name }
}