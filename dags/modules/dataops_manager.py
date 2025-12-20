import pyodbc
import logging
from airflow.providers.odbc.hooks.odbc import OdbcHook


class DataOpsManager:
    def __init__(self, conn_id="dremio_odbc", catalog="nessie_catalog") -> None:
        self._conn_id = conn_id
        self._catalog = catalog
        self._hook = OdbcHook(odbc_conn_id=conn_id)

    def _execute_sql_direct(self, sql: str):
        """
        Bypass Spartan: Força o autocommit=True em todas as execuções.
        Impede que o Airflow tente desabilitar o autocommit (causador do HYC00).
        """
        conn_str = self._hook.odbc_connection_string
        logging.info(f"Dremio SQL Exec: {sql}")

        with pyodbc.connect(conn_str, autocommit=True) as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql)

    def create_branch(self, branch_name: str, source_ref="main"):
        sql = f'CREATE BRANCH "{branch_name}" IN {self._catalog}'
        try:
            self._execute_sql_direct(sql)
            logging.info(f"Branch '{branch_name}' pronta.")
        except Exception as e:
            if "already exists" in str(e).lower():
                logging.warning(f"Branch '{branch_name}' já existe.")
            else:
                raise e

    def ensure_table_exists(
        self, dataset_name: str, branch_name: str, bucket_name: str
    ):
        """
        Registra a tabela Iceberg no Nessie via Dremio.
        """
        # 1. Muda o contexto para a branch correta
        use_ref_sql = f'USE REFERENCE "{branch_name}" IN {self._catalog}'

        # 2. SQL de criação da tabela (ajuste conforme sua estrutura de pastas no GCS)
        create_table_sql = f"""
            CREATE TABLE IF NOT EXISTS {self._catalog}.{dataset_name}
            AS SELECT * FROM TABLE(gcs_bronze."{bucket_name}"."{dataset_name}" (type => 'parquet'))
        """

        try:
            logging.info(f"Registrando tabela {dataset_name} na branch {branch_name}")
            self._execute_sql_direct(use_ref_sql)
            self._execute_sql_direct(create_table_sql)
            logging.info(f"Tabela {dataset_name} registrada com sucesso!")
        except Exception as e:
            logging.error(f"Erro ao registrar tabela no Dremio: {e}")
            raise e

    def merge_branch(self, branch_name: str, target_ref="main"):
        """
        Publica os dados da branch de dev para a main.
        """
        sql = f'MERGE BRANCH "{branch_name}" INTO "{target_ref}" IN {self._catalog}'
        try:
            logging.info(f"Fazendo merge de {branch_name} para {target_ref}")
            self._execute_sql_direct(sql)
            logging.info("Merge realizado com sucesso!")
        except Exception as e:
            logging.error(f"Erro no merge: {e}")
            raise e
