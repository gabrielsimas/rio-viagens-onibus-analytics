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
