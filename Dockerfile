FROM apache/airflow:2.8.1-python3.11

USER root

# Instala compiladores (gcc), headers de banco e ferramentas essenciais
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    git \
    unixodbc \
    libpq-dev \
    unixodbc-dev \
    g++ \
    pkg-config && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

COPY ./drivers/dremio-odbc-LATEST.x86_64.deb /tmp/
RUN dpkg -i /tmp/dremio-odbc-LATEST.x86_64.deb
USER airflow

USER airflow

COPY requirements.txt /requirements.txt

RUN pip install --no-cache-dir -r /requirements.txt