# !/bin/bash
# Encerra o script se qualquer comando falhar
set -e

# --- CONFIGURAÇÕES ---
\# Substitua pelo ID do seu projeto
PROJECT_ID="mvp-engenharia-dados-479719"
# Região onde o bucket de estado será criado
REGION="us-central1"
# Nome do bucket para guardar o estado do Terraform (deve ser único globalmente)
TF_STATE_BUCKET_NAME="mvp-rio-transportes-tf-state"

echo "🚀 Iniciando Bootstrap do GCP para o projeto: $PROJECT_ID"

# 1. Configurar o projeto localmente para garantir que estamos no lugar certo
echo "📋 Definindo projeto ativo..."
gcloud config set project $PROJECT_ID

# 2. Habilitar as APIs Críticas (Gatekeepers)
# Sem a 'cloudresourcemanager', o Terraform não pode verificar nada.
# Sem a 'serviceusage', ele não consegue ativar outras APIs.
echo "🔌 Ativando APIs essenciais (isso pode levar alguns segundos)..."
gcloud resource-manager org-policies disable-enforce iam.disableServiceAccountKeyCreation --project=mvp-engenharia-dados-479719
gcloud services enable cloudresourcemanager.googleapis.com
gcloud services enable serviceusage.googleapis.com
gcloud services enable iam.googleapis.com

# 3. Criar o Bucket para o Backend do Terraform (se não existir)
echo "📦 Verificando bucket de estado ($TF_STATE_BUCKET_NAME)..."

if ! gcloud storage buckets describe gs://$TF_STATE_BUCKET_NAME &> /dev/null; then
  echo "   Bucket não existe. Criando..."
  gcloud storage buckets create gs://$TF_STATE_BUCKET_NAME \
    --project=$PROJECT_ID \
    --location=$REGION \
    --uniform-bucket-level-access

  # Ativar versionamento (Boas práticas de DevOps: se o estado corromper, você tem backup)
  gcloud storage buckets update gs://$TF_STATE_BUCKET_NAME --versioning
  echo "✅ Bucket criado com sucesso!"
else
  echo "✅ O bucket já existe."
fi

echo "--------------------------------------------------------"
echo "🎉 Bootstrap concluído!"
echo "Agora você pode rodar 'terraform init' e 'terraform apply'."
echo "--------------------------------------------------------"