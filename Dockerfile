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
    alien \
    g++ \
    pkg-config && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

COPY ./drivers/dremio-flight-odbc.rpm /tmp/
RUN alien -i /tmp/dremio-flight-odbc.rpm

RUN printf "[Dremio Flight]\n\
Description=Dremio Arrow Flight SQL ODBC Driver\n\
Driver=/opt/arrow-flight-sql-odbc-driver/lib64/libarrow-odbc.so.0.9.1.168\n\
Setup=/opt/arrow-flight-sql-odbc-driver/lib64/libarrow-odbc.so.0.9.1.168\n\
UsageCount=1" > /etc/odbcinst.ini

USER airflow

COPY requirements.txt /requirements.txt

RUN pip install --no-cache-dir -r /requirements.txt