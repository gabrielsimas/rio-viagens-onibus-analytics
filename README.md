# Rio Viagens Ônibus Analytics 🚌🌧️

![Python](https://img.shields.io/badge/python-3670A0?style=for-the-badge&logo=python&logoColor=ffdd54)
![Google Cloud](https://img.shields.io/badge/GoogleCloud-%234285F4.svg?style=for-the-badge&logo=google-cloud&logoColor=white)
![Dremio](https://img.shields.io/badge/Dremio-724D99?style=for-the-badge&logo=dremio&logoColor=white)
![Apache Iceberg](https://img.shields.io/badge/Apache%20Iceberg-blue?style=for-the-badge&logo=apache&logoColor=white)

[![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/gabrielsimas/rio-viagens-onibus-analytics/blob/master/ARTIGO.ipynb)

Este repositório contém a documentação técnica e os resultados analíticos do projeto de monitoramento de transporte público e impacto climático no Rio de Janeiro. Através de uma arquitetura de **Data Lakehouse**, integramos milhões de registros de telemetria GPS a dados pluviométricos para gerar insights sobre a eficiência operacional da frota.

---

## 🚀 Visualizar o Artigo no Google Colab

# 🔴🔴🔴 ABRA O ARTIGO COMPLETO AQUI! 🔴🔴🔴
# 🔻🔻🔻🔻🔻🔻🔻🔻🔻🔻🔻🔻🔻🔻🔻🔻🔻🔻

[![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/gabrielsimas/rio-viagens-onibus-analytics/blob/master/ARTIGO.ipynb)

---

## 🏗️ Arquitetura do Projeto (Medalhão)

A solução foi estruturada utilizando a metodologia de camadas para garantir a qualidade e a rastreabilidade do dado:

* **Camada Bronze**: Armazenamento dos dados brutos em formato Parquet no Google Cloud Storage.
* **Camada Prata**: Processo de saneamento, limpeza de *outliers* e o *Temporal Join* entre telemetria e meteorologia.
* **Camada Ouro**: Modelagem dimensional (*Star Schema*) persistida em **Apache Iceberg**, facilitando a consulta das métricas finais.

## 🔍 Principais Descobertas

O projeto responde a 6 perguntas críticas de negócio, destacando-se:
1.  A identificação de linhas com alto índice de **Imprevisibilidade** (Desvio Padrão elevado).
2.  O impacto severo de chuvas fortes na oferta e no tempo médio de viagem.

**Entretanto**, devido a limitações computacionais de processamento, a análise atual foca no recorte temporal de 2024 para garantir a precisão estatística dos resultados apresentados.

## 🛠️ Stack Tecnológica

* **Engine de Dados**: Dremio (SQL Lakehouse).
* **Armazenamento**: Google Cloud Storage & Apache Iceberg.

---
Projeto desenvolvido por [Gabriel Simas](https://github.com/gabrielsimas) para a pós-graduação da **PUC-Rio** 🎓.