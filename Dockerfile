FROM apache/airflow:slim-2.10.0

USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    git \
    libpq-dev && \
    apt-get clean && \
    rm -rf /var/lib/api/lists/*

USER airflow

COPY requirements.txt /requirements.txt

RUN pip install --no-cache-dir -r /requirements.txt