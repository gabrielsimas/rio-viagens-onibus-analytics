import logging
from airflow.providers.common.sql.hooks.sql import DbHook


class DataOpsManager:
    def __init__(self, conn_id="dremio_default", catalog="nessie_catalog") -> None:
        self._conn_id = conn_id
        self._catalog = catalog
        self._hook = DbApiHook(conn_id=conn_id)

    def create_branch(self, branch_name: str, source_ref="main"):
        """Cria uma branch isolada para o ETL."""
        sql = f'CREATE BRANCH "{branch_name}" IN {self._catalog} FROM "{source_ref}"'
        try:
            logging.info(f"Tentando criar a branch: {branch_name}")
            self._hook.run(sql)
            logging.info(f"Branch {branch_name} criada com sucesso!")
        except Exception as e:
            if "already exists" in str(e):
                logging.warning(f"Branch {branch_name} já existe. Seguindo...")
            else:
                raise e

    def merge_branch(self, branch_name: str, target_ref="main"):
        """Faz o merge atômic (transacional) para a Main."""
        sql = f'MERGE BRANCH "{branch_name}" INTO {self._catalog} INTO "{target_ref}"'
        logging.info(f"Executando o Merge: {branch_name} -> {target_ref}")
        self.hook.run(sql)
        logging.info(
            f"Merge entre {branch_name} -> {target_ref} realizado com sucesso!"
        )
