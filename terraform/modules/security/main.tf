variable "project_id" {}

resource "google_project_organization_policy" "allow_service_account_key_creation" {
  project = var.project_id
  constraint = "constraints/iam.disableServiceAccountKeyCreation"

  boolean_policy {
    enforced = false
  }
}

# --- SERVICE ACCOUNTS
resource "google_service_account" "airflow_sa" {
  account_id = "sa-airflow-etl"
  display_name = "Airflow ETL Service Account"
}

resource "google_service_account" "dremio_sa" {
  account_id = "sa-dremio-lake"
  display_name = "Dremio Lakehouse Service Account"
}

# --- CHAVES DE ACESSO (Geração das chaves)
resource "google_service_account_key" "airflow_key" {
    service_account_id = google_service_account.airflow_sa.name
    depends_on = [
      google_project_organization_policy.allow_service_account_key_creation
    ]
}

resource "google_service_account_key" "dremio_key" {
  service_account_id = google_service_account.dremio_sa.name
  depends_on = [
    google_project_organization_policy.allow_service_account_key_creation
  ]
}

# --- SECRET MANAGER (O Cofre)
# 1. Cria o "Envelope" da Secret
resource "google_secret_manager_secret" "airflow_secret" {
  secret_id = "airflow-sa-key"
  replication {
    auto { }
  }
}

resource "google_secret_manager_secret" "dremio_secret" {
  secret_id = "dremio-sa-key"
  replication {
    auto { }
  }
}

# 2. Guarda o valor (JSON da chave) dentro do Cofre
resource "google_secret_manager_secret_version" "airflow_key_version" {
  secret = google_secret_manager_secret.airflow_secret.id
  secret_data = google_service_account_key.airflow_key.private_key
}

resource "google_secret_manager_secret_version" "dremio_key_version" {
    secret = google_secret_manager_secret.dremio_secret.id
    secret_data = google_service_account_key.dremio_key.private_key
}

# OUTPUTS
output "airflow_email" { value = google_service_account.airflow_sa.email }
output "dremio_email" { value = google_service_account.dremio_sa.email }