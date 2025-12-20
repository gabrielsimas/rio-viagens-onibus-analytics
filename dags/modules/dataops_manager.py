import os
import logging
from airflow.providers.odbc.hooks.odbc import OdbcHook


class DataOpsManager:
    """
    Gerencia operações de DataOps no Dremio (Nessie/Project Nessie)
    utilizando ODBC para garantir performance e compatibilidade com o ecossistema Python.
    """

    def __init__(self, conn_id="dremio_odbc", catalog="nessie_catalog") -> None:
        self._conn_id = conn_id
        self._catalog = catalog
        # O OdbcHook gerencia a conexão via pyodbc internamente
        self._hook = OdbcHook(odbc_conn_id=conn_id)

    def create_branch(self, branch_name: str, source_ref="main"):
        """Cria uma branch isolada no Nessie para isolamento do ETL (WAP pattern)."""
        # Syntax Dremio: CREATE BRANCH "nome" IN catalog FROM "origem"
        sql = f'CREATE BRANCH "{branch_name}" IN {self._catalog} FROM "{source_ref}"'
        try:
            logging.info(
                f"Dremio DataOps: Tentando criar a branch '{branch_name}' no catálogo '{self._catalog}'"
            )
            self._hook.run(sql)
            logging.info(f"Sucesso: Branch '{branch_name}' criada.")
        except Exception as e:
            # Tratamento para evitar que a DAG quebre se a branch já existir (reprocessamento)
            if "already exists" in str(e).lower():
                logging.warning(
                    f"Atenção: A branch '{branch_name}' já existe. Prosseguindo com a ingestão..."
                )
            else:
                logging.error(f"Erro fatal ao criar branch no Dremio: {e}")
                raise e

    def merge_branch(self, branch_name: str, target_ref="main"):
        """Faz o merge atômico da branch de trabalho para a Main."""
        # Syntax Dremio: MERGE BRANCH "origem" INTO catalog INTO "destino"
        sql = f'MERGE BRANCH "{branch_name}" INTO {self._catalog} INTO "{target_ref}"'
        try:
            logging.info(
                f"Dremio DataOps: Executando MERGE atômico: {branch_name} -> {target_ref}"
            )
            self._hook.run(sql)
            logging.info(f"Sucesso: Merge realizado e publicado na '{target_ref}'.")
        except Exception as e:
            logging.error(f"Erro ao realizar merge no Dremio: {e}")
            raise e

    def refresh_table(self, table_name: str, branch_name: str):
        """Força o Dremio a reconhecer os novos arquivos Parquet na branch específica

        Args:
            table_name (str): Nome da Tabela
            branch_name (str): Nome da Branch
        """
        # Define o contexto da branch para a sessão atual
        use_sql = f'USE REFERENCE "{branch_name}" IN {self._catalog}'
        # Comando para atualizar metadados (ajuste o path conforme sua fonte no Dremio)
        refresh_sql = f"ALTER TABLE {self._catalog}.{table_name} REFRESH METADATA"

        try:
            logging.info(
                f"Dremio DataOps: Atualizando metadados de '{table_name}' na branch '{branch_name}'"
            )
            self._hook.run(use_sql)
            self._hook.run(refresh_sql)
        except Exception as e:
            logging.error(f"Erro ao atualizar metadados: {e}")
            raise e

    def ensure_table_exists(
        self, dataset_name: str, branch_name: str, bucket_name: str = None
    ):
        """
        Garante que a tabela Iceberg existe no Dremio/Nessie.
        Se não existir, cria. Se existir, apenas garante o contexto da branch.
        """
        bucket = bucket_name or os.getenv(
            "BUCKET_BRONZE", "mvp-rio-transportes-bronze-f7c4"
        )
        table_path = f"{self._catalog}.bronze.{dataset_name}"
        # O path aponta para a raiz do dataset no bucket bronze que limpamos
        storage_location = f"gs://{bucket}/{dataset_name}"

        # SQL para criar tabela Iceberg se não existir
        # Nota: O Dremio/Nessie identifica as partições automaticamente via metadados Iceberg
        create_sql = f"""
            CREATE TABLE IF NOT EXISTS {table_path}
            AT BRANCH "{branch_name}"
            LOCATION '{storage_location}'
        """

        try:
            # Garante que estamos operando na branch correta antes de criar/atualizar
            self._hook.run(f'USE REFERENCE "{branch_name}" IN {self._catalog}')

            logging.info(
                f"Dremio DataOps: Verificando/Criando tabela {table_path} na branch {branch_name}"
            )
            self._hook.run(create_sql)

            # Se for uma tabela de arquivos brutos (não-Iceberg), precisaríamos do REFRESH METADATA
            # Mas como estamos focando em Iceberg/Nessie, o metadado é gerenciado pelo commit.
            logging.info(f"Tabela {dataset_name} pronta para consumo.")
        except Exception as e:
            logging.error(f"Erro ao registrar tabela no Dremio: {e}")
            raise e
