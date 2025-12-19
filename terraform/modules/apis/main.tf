variable "project_id" {}

resource "google_project_service" "enabled_apis" {
  for_each = toset([
    "iam.googleapis.com",
    "storage.googleapis.com",
    "drive.googleapis.com",
    "secretmanager.googleapis.com",
    "orgpolicy.googleapis.com"
  ])
  service = each.key
  disable_on_destroy = false
}