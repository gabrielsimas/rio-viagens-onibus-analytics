variable "project_id" {
  description = "O ID do seu projeto no GCP"
  type = string
}

variable "region" {
  description = "Região padrão"
  type = string
  default = "us-east1"
}