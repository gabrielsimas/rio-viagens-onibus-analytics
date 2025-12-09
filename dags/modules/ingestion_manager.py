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
        self._drive_hook = GoogleDriveHook(gcp_conn_id=gcp_conn_id)
        self._gcs_hook = GCSHook(gcp_conn_id=gcp_conn_id)

    # TODO: Criar uma função mais genérica
    def process_file_to_bronze(
        self, folder_id: str, bucket_landing: str, bucket_bronze: str, dataset_name
    ):
        """
        Orquestra o fluxo para uma pasta específica:
        1. Lista CSVs na pasta do Drive.
        2. Baixa (Stream) para disco temporário.
        3. Salva na Landing (Raw CSV) em subpasta organizada.
        4. Converte para Parquet (DuckDB).
        5. Salva na Bronze (Parquet Otimizado)

        Args:
            folder_id (str): ID da pasta no Google Drive.
            bucket_landing (str): Nome do buclet temporário (Triagem)
            bucket_bronze (str): Nome do bucket bronze (ex: mvp-bronze).
            dataset_name (str): Nome do contexto (ex: 'viagens_onibus). Usado para subpastas.
        """
        logging.info(f"--- Iniciando Processamento: Dataset {dataset_name} (Folder ID: {folder_id}) ---")

        # 1. Listar arquivos na pasta do Drive
        # A Query garante que pegamos apenas CSVs que não estão no lixo
        query = f"{folder_id} in parents and trashed = false and name contains '.csv'"

        try:
            # TODO: Adicionar à Classe estática GDriveHelper pra limpar esse arquivo
            files = self._drive_hook.get_conn().files().list(
                q=query,
                fields="files(id, name)"
            ).execute().get('files',[])
        except Exception as e:
            logging.error(f"Erro ao listar arquivos no Drive para {dataset_name}: {e}")
            raise e

        if not files:
            logging.warning(f"[{dataset_name}] Nenhuma arquivo .csv encontrada na pasta.")
            return

        # Cria diretório temporário no SSD na VPS (/tmp) para processamento rápido
        with tempfile.TemporaryDirectory() as tmp_dir:
            for file in files:
                file_name = file['name']
                file_id = file['id']

                # Define caminhos locais e remotos
                local_csv_path = os.path.join(tmp_dir, file_name)
                parquet_name = file_name.replace('.csv', '.parquet')
                local_parquet_path = os.path.join(tmp_dir, parquet_name)

                remote_object_raw = f"raw/{dataset_name}/{file_name}"
                remote_object_bronze = f"bronze/{dataset_name}/{parquet_name}"

                try:
                    logging.info(f"[{dataset_name}] --- Processando: {file_name} ---")

                    # 2. Download Otimizado (Stream) do Drive para o Disco Local
                    # TODO: Adicionar à Classe estática GDriveHelper pra limpar esse arquivo
                    logging.info(f"Baixando {file_name} do Drive {folder_id} para {tmp_dir}...")
                    request = self._drive_hook.get_conn().files().get_media(fileId=file_id)
                    with open(local_csv_path, 'wb') as f:
                        f.write(request.execute())

                    # 3. Upload para Landing (CSV Puro - Backup)
                    # Verifica se já existe para evitar re-upload desnecessário
                    if not self._gcs_hook.exists(bucket_name=bucket_landing, object_name=remote_object_raw):
                        logging.info("Upload para Landing (CSV)...")
                        self._gcs_hook.upload(
                            bucket_name=bucket_landing,
                            object_name=remote_object_raw,
                            filename=local_csv_path,
                            timeout=600 #10 Minutos para arquivos grandes
                        )
                    else:
                        logging.info(f"Arquivo {file_name} já existe na Landing {dataset_name}. Pulando arquivo... ")

                    # 4. Conversão CSV -> Parquet via DuckDB (Zero Copy / Streaming)
                    logging.info("Convertendo para Parquet com DuckDB...")

                    # Magic Query (Query Mágica) do DuckDB:
                    # auto_detect=TRUE: Tenta adivinhar tipos
                    # all_varchar=FALSE: Tenta convencer números/datas (mas se falhar, o script pode parar.
                    # Se os dados forem muito sujos, mude para TRUE para garantir a ingestão e limpe na Silver.
                    query = f"""
                        COPY (
                            SELECT * FROM read_csv_auto('{local_csv_path}', auto_detect=TRUE, all_varchar=FALSE)
                            )
                        TO '{local_csv_path}' (FORMAT 'PARQUET', CODEC 'SNAPPY');
                    """
                    duckdb.query(query)

                    # 5. Upload para a Camada Bronze
                    logging.info(f"Upload para a Camada Bronze (Parquet)...")
                    self._gcs_hook.upload(
                        bucket_name=bucket_bronze,
                        object_name=remote_object_bronze,
                        filename=local_parquet_path,
                        timeout=600
                    )

                    logging.info(f"Sucesso Total: {parquet_name}")

                except Exception as e:
                    logging.error(f"Falha crítica em {file_name} no dataset {dataset_name}: {e}")
                    # Aqui decidimos se quebramos o ciclo com raise, falhando a DAG, ou 'continue' para
                    # tentar o próximo arquivo!
                    # Como todos os arquivos são importantes, vamos quebrar o processo.
                    #continue
                    raise e

        logging.info("Ingestão da camada Bronze finalizada")
