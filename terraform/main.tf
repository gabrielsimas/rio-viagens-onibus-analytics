# 1. Ativa as APIs
module "gcp_apis" {
  source = "./modules/apis"
  project_id = var.project_id
}

# 2. Cria Segurança (Contas e Segredos)
module "security" {
  source = "./modules/security"
  project_id = var.project_id

  depends_on = [ module.gcp_apis ] # Só roda depois das APIs ligadas
}

# 3. Cria Storage e aplica permissões
module "data_lake" {
  source = "./modules/storage"
  project_id = var.project_id
  airflow_sa_email = module.security.airflow_email
  dremio_sa_email = module.security.dremio_email
}

