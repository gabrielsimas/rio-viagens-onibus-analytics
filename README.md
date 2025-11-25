# 🚌 MVP Engenharia de Dados - O Rio de Janeiro e os Ônibus: Entendendo o Caos!

## ***Nome:*** **Luís Gabriel Nascimento Simas**

## ***Matrícula:*** **4052025000943**

![Python](https://img.shields.io/badge/Python-3.9+-blue?style=for-the-badge&logo=python&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-Container-2496ED?style=for-the-badge&logo=docker&logoColor=white)
![Airflow](https://img.shields.io/badge/Airflow-Orchestration-017CEE?style=for-the-badge&logo=apache-airflow&logoColor=white)
![dbt](https://img.shields.io/badge/dbt-Transformation-FF694B?style=for-the-badge&logo=dbt&logoColor=white)
![GCP](https://img.shields.io/badge/GCP-Cloud_Storage-4285F4?style=for-the-badge&logo=google-cloud&logoColor=white)

Este projeto constitui o MVP para a conclusão da Pós-Graduação em Engenharia de Dados. O objetivo é construir uma plataforma de dados robusta (*"Modern Data Stack in a Box"*) para ingerir, processar e analisar dados de GPS, bilhetagem e frota do transporte público da cidade do Rio de Janeiro.

---

## 🏛️ Arquitetura da Solução

A solução foi desenhada seguindo o paradigma de **Data Lakehouse**, unificando a flexibilidade do Data Lake com a gestão de dados do Data Warehouse. Toda a infraestrutura é containerizada via Docker, garantindo reprodutibilidade e isolamento.

![Diagrama de Arquitetura](docs/diagrama_arquitetura.png)

### Stack Tecnológica & Decisões Arquiteturais

Abaixo detalho as ferramentas escolhidas e a justificativa técnica para cada componente:

| Componente | Ferramenta Escolhida | Justificativa Técnica (O "Porquê") |
| :--- | :--- | :--- |
| **Orquestração** | **Apache Airflow** | Padrão de mercado para gerenciamento de dependências complexas. Permite *backfilling*, retentativas automáticas e monitoramento visual dos pipelines. |
| **Compute (Ingestão)** | **DuckDB** | Motor SQL OLAP in-process. Escolhido para substituir o Pandas na etapa de Bronze, permitindo processamento via *streaming* (baixo consumo de RAM) e conversão performática de CSV para Parquet. |
| **Storage** | **Google Cloud Storage** | Armazenamento de objetos escalável e de baixo custo. Implementado com segregação física de Buckets (Landing/Bronze) para garantir políticas de segurança e ciclo de vida distintos. |
| **Transformação** | **dbt (Data Build Tool)** | Implementa a filosofia de *Analytics Engineering*. Responsável pela limpeza, testes de qualidade (Data Quality) e documentação da linhagem dos dados. |
| **Query Engine** | **Dremio** | Atua como a camada de Lakehouse, permitindo consultas SQL de baixa latência diretamente sobre os arquivos no Data Lake, eliminando a necessidade de cópia para um Data Warehouse proprietário. |
| **Formato de Arquivo** | **Parquet / Iceberg** | Formatos colunares com compressão (Snappy/Zstd), otimizados para leitura analítica e suporte a *schema evolution*. |

---

## 🔄 Fluxo de Dados (Pipeline)

O pipeline segue a **Arquitetura Medalhão** (*Medallion Architecture*) para garantir a qualidade progressiva dos dados:

![Arquitetura Medalhão](docs/medallion_architecture.png)

### 1. Camada Landing (Triagem)
* **Origem:** Arquivos CSV brutos extraídos manualmente do BigQuery (Data Rio) e armazenados no Google Drive.
* **Processo:** Airflow orquestra o download em *chunks* para memória local temporária.
* **Destino:** Bucket GCS `mvp-transporte-landing`.
* **Objetivo:** Cópia fiel da origem (*Raw*), servindo como backup imutável.

### 2. Camada Bronze
* **Processo:** DuckDB lê os CSVs da Landing, infere schema e converte para Parquet.
* **Destino:** Bucket GCS `mvp-transporte-bronze`.
* **Objetivo:** Otimização de armazenamento (compressão) e performance de leitura, mantendo os dados brutos históricos.

### 3. Camada Silver (Refinada)
* **Processo:** dbt executa transformações SQL via Dremio.
* **Ações:** Limpeza de nulos, tipagem de dados (`String` -> `Timestamp`), renomeação de colunas para padrão de negócio e desduplicação.
* **Objetivo:** Dados confiáveis e limpos (*"Single Source of Truth"*).

### 4. Camada Gold (Agregada)
* **Processo:** dbt modela os dados em **Esquema Estrela** (*Star Schema*).
* **Modelagem:**
    * Tabelas Fato: `fct_viagens`, `fct_telemetria`.
    * Tabelas Dimensão: `dim_veiculo`, `dim_calendario`.
* **Objetivo:** Dados prontos para consumo por ferramentas de BI e resposta às perguntas de negócio.

---

## 🧪 Qualidade de Dados

A qualidade é garantida via testes automatizados no **dbt**:
* **Unicidade:** Chaves primárias (`id_viagem`) verificadas para evitar duplicatas.
* **Nulidade:** Campos críticos não podem ser nulos.
* **Regras de Negócio:** Validação de intervalos de datas e consistência de tempos de viagem (ex: tempo de viagem não pode ser negativo).