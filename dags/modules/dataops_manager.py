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
        self,
        dataset_name: str,
        branch_name: str,
        bucket_name: str,
        schema_string: str = None,
    ):
        """
        Cria a tabela no Nessie usando CAST para garantir os tipos do catálogo.
        """
        use_ref = f'USE REFERENCE "{branch_name}" IN {self._catalog}'

        if schema_string:
            # Transforma "col TIPO" em "CAST(col AS TIPO) as col"
            cols_with_types = [c.strip() for c in schema_string.split(",")]
            cast_list = []
            for item in cols_with_types:
                parts = item.split()
                if len(parts) >= 2:
                    col_name = parts[0]
                    col_type = parts[1]
                    cast_list.append(f"CAST({col_name} AS {col_type}) as {col_name}")

            select_clause = ", ".join(cast_list)
        else:
            select_clause = "*"

        # Criamos via CTAS. O Dremio gerencia o LOCATION automaticamente no Nessie.
        sql_create = f"""
            CREATE TABLE IF NOT EXISTS {self._catalog}.{dataset_name}
            AS SELECT {select_clause}
            FROM TABLE(gcs_bronze."{bucket_name}"."{dataset_name}" (type => 'parquet'))
        """

        try:
            logging.info(f"Registrando {dataset_name} com tipagem explícita...")
            self._execute_sql_direct(use_ref)
            self._execute_sql_direct(sql_create)
            logging.info(f"Tabela {dataset_name} criada com sucesso.")
        except Exception as e:
            if "already exists" in str(e).lower():
                logging.warning(f"Tabela {dataset_name} já existe na branch.")
            else:
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
