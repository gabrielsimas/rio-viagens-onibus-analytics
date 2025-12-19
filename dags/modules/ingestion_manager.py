import os
import duckdb
import logging
import tempfile
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.suite.hooks.drive import GoogleDriveHook


class IngestionManager:
    """
    Gerencia a ingestão de múltiplos datasets do Google Drive para
    a camada de Triagem (Landing) e Bronze no Google Cloud Storage.
    """

    def __init__(self, gcp_conn_id="google_cloud_default") -> None:
        self._gcp_conn = gcp_conn_id
        # Hooks instanciados no __init__ para reuso dentro da mesma task
        self._drive_hook = GoogleDriveHook(gcp_conn_id=gcp_conn_id)
        self._gcs_hook = GCSHook(gcp_conn_id=gcp_conn_id)

    def process_file_to_bronze(
        self,
        folder_id: str,
        bucket_landing: str,
        bucket_bronze: str,
        dataset_name: str,
        data_referencia: str,
    ):
        """
        Orquestra o fluxo para uma pasta específica com particionamento Hive:
        1. Lista CSVs no Drive.
        2. Define estrutura Hive (ano/mes/dia) baseada na data_referencia.
        3. Converte CSV para Parquet via DuckDB.
        4. Sobe para GCS seguindo a estrutura de pastas Hive.

        Args:
            folder_id (str): ID da pasta no Google Drive.
            bucket_landing (str): Nome do bucket de Triagem.
            bucket_bronze (str): Nome do bucket bronze.
            dataset_name (str): Nome do contexto (ex: 'viagens_onibus').
            data_referencia (str): Data no formato YYYY-MM-DD (via Airflow {{ ds }}).
        """
        logging.info(
            f"--- Iniciando Processamento: Dataset {dataset_name} | Referência: {data_referencia} ---"
        )

        # 1. Construção da Estrutura Hive-Style (Caminho lógico)
        try:
            ano, mes, dia = data_referencia.split("-")
            hive_path = f"ano={ano}/mes={mes}/dia={dia}"
        except ValueError:
            logging.error(
                f"Formato de data_referencia inválido: {data_referencia}. Esperado: YYYY-MM-DD"
            )
            raise

        # 2. Listar arquivos na pasta do Drive
        query = f"'{folder_id}' in parents and trashed = false and name contains '.csv'"

        try:
            files = (
                self._drive_hook.get_conn()
                .files()
                .list(q=query, fields="files(id, name)")
                .execute()
                .get("files", [])
            )
        except Exception as e:
            logging.error(f"Erro ao listar arquivos no Drive para {dataset_name}: {e}")
            raise e

        if not files:
            logging.warning(
                f"[{dataset_name}] Nenhum arquivo .csv encontrado na pasta."
            )
            return

        # Processamento em diretório temporário local (VPS)
        with tempfile.TemporaryDirectory() as tmp_dir:
            for file in files:
                file_name = file["name"]
                file_id = file["id"]

                local_csv_path = os.path.join(tmp_dir, file_name)
                parquet_name = file_name.replace(".csv", ".parquet")
                local_parquet_path = os.path.join(tmp_dir, parquet_name)

                # Definição dos caminhos remotos com estrutura HIVE
                remote_object_raw = f"raw/{dataset_name}/{hive_path}/{file_name}"
                remote_object_bronze = (
                    f"bronze/{dataset_name}/{hive_path}/{parquet_name}"
                )

                try:
                    logging.info(f"[{dataset_name}] --- Processando: {file_name} ---")

                    # 3. Download do Drive
                    request = (
                        self._drive_hook.get_conn().files().get_media(fileId=file_id)
                    )
                    with open(local_csv_path, "wb") as f:
                        f.write(request.execute())

                    # 4. Upload para Landing (CSV Original)
                    if not self._gcs_hook.exists(
                        bucket_name=bucket_landing, object_name=remote_object_raw
                    ):
                        logging.info(f"Subindo CSV original para Landing...")
                        self._gcs_hook.upload(
                            bucket_name=bucket_landing,
                            object_name=remote_object_raw,
                            filename=local_csv_path,
                            timeout=600,
                        )

                    # 5. Conversão CSV -> Parquet via DuckDB (Streaming local)
                    logging.info("Convertendo para Parquet com DuckDB...")
                    query_duck = f"""
                        COPY (
                            SELECT * FROM read_csv_auto('{local_csv_path}', auto_detect=TRUE, all_varchar=FALSE)
                            )
                        TO '{local_parquet_path}' (FORMAT 'PARQUET', CODEC 'SNAPPY');
                    """
                    duckdb.query(query_duck)

                    # 6. Upload para Bronze (Parquet Particionado)
                    logging.info(
                        f"Subindo Parquet para Bronze no caminho: {remote_object_bronze}"
                    )
                    self._gcs_hook.upload(
                        bucket_name=bucket_bronze,
                        object_name=remote_object_bronze,
                        filename=local_parquet_path,
                        timeout=600,
                    )

                except Exception as e:
                    logging.error(
                        f"Falha crítica em {file_name} no dataset {dataset_name}: {e}"
                    )
                    raise e

        logging.info(f"--- Ingestão Bronze finalizada para {dataset_name} ---")
