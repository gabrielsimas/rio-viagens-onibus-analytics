import pyodbc
import logging
from airflow.providers.odbc.hooks.odbc import OdbcHook


class DataOpsManager:
    def __init__(self, conn_id="dremio_odbc", catalog="nessie_catalog") -> None:
        self._conn_id = conn_id
        self._catalog = catalog
        self._hook = OdbcHook(odbc_conn_id=conn_id)

    def _get_connection(self):
        """Retorna uma conexão com autocommit habilitado para evitar erros HYC00."""
        conn_str = self._hook.odbc_connection_string
        return pyodbc.connect(conn_str, autocommit=True)

    def create_branch(self, branch_name: str, source_ref="main"):
        sql = f'CREATE BRANCH "{branch_name}" IN {self._catalog}'
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(sql)
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
        Cria a tabela no Nessie garantindo que o USE REFERENCE e o CREATE
        ocorram na MESMA sessão.
        """
        use_ref = f'USE REFERENCE "{branch_name}" IN {self._catalog}'

        if schema_string:
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

        sql_create = f"""
            CREATE TABLE IF NOT EXISTS {self._catalog}.{dataset_name}
            AS SELECT {select_clause}
            FROM TABLE(gcs_bronze."{bucket_name}"."{dataset_name}" (type => 'parquet'))
        """

        try:
            # FATO: Abrimos uma ÚNICA conexão para manter o estado da branch
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    logging.info(f"Setando contexto para branch: {branch_name}")
                    cursor.execute(use_ref)

                    logging.info(f"Registrando {dataset_name} com tipagem explícita...")
                    cursor.execute(sql_create)

            logging.info(
                f"Tabela {dataset_name} criada com sucesso na branch {branch_name}."
            )
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
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    logging.info(f"Publicando dados: {branch_name} -> {target_ref}")
                    cursor.execute(sql)
            logging.info("Merge concluído!")
        except Exception as e:
            raise e