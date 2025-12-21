import pyodbc
import logging
from airflow.providers.odbc.hooks.odbc import OdbcHook

class DataOpsManager:
    def __init__(self, conn_id="dremio_odbc", storage_source="gcs_bronze") -> None:
        self._conn_id = conn_id
        self._source = storage_source
        self._hook = OdbcHook(odbc_conn_id=conn_id)

    def _execute_sql(self, sql: str):
        conn_str = self._hook.odbc_connection_string
        with pyodbc.connect(conn_str, autocommit=True) as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql)

    def ensure_table_exists(self, dataset_name, bucket_name, schema_string=None):
        """Cria a tabela Iceberg diretamente no storage do GCS via Dremio."""
        # Sem Nessie, usamos o namespace do Source diretamente
        table_path = f"{self._source}.{dataset_name}"

        sql_create = f"""
            CREATE TABLE IF NOT EXISTS {table_path}
            AS SELECT * FROM TABLE(gcs_bronze."{bucket_name}"."{dataset_name}" (type => 'parquet'))
        """

        try:
            logging.info(f"Registrando tabela Iceberg: {table_path}")
            self._execute_sql(sql_create)
            logging.info(f"Sucesso: {dataset_name} está disponível no Dremio.")
        except Exception as e:
            if "already exists" in str(e).lower():
                logging.warning(f"Tabela {dataset_name} já existe. Rodando Refresh.")
                self._execute_sql(f"ALTER TABLE {table_path} REFRESH METADATA")
            else:
                raise e