import os
import io
import duckdb
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from airflow.providers.google.cloud.hooks.gcs import GCSHook


class IngestionManager:
    """
    Classe responsável pela ingestão de dados.
    Utiliza bypass direto via Google Discovery API para evitar problemas de
    escopo e permissões do Airflow Connection padrão.
    """

    def __init__(self) -> None:
        # Caminho onde o arquivo JSON está mapeado no seu Docker
        self._key_path = "/opt/airflow/keys/sa-airflow.json"

        # Escopo de somente leitura que validamos no Colab
        self._scopes = ['https://www.googleapis.com/auth/drive.readonly']

        # Para o GCS, mantemos o Hook se a conexão 'google_cloud_default' estiver ok.
        # Caso o GCS também dê erro de 403, podemos fazer bypass nele depois.
        self._gcs_hook = GCSHook(gcp_conn_id="google_cloud_default")

    def _get_drive_service(self):
        """Cria o serviço do Drive usando a chave diretamente do disco"""
        if not os.path.exists(self._key_path):
            raise FileNotFoundError(f"Chave não encontrada em: {self._key_path}")

        creds = service_account.Credentials.from_service_account_file(
            self._key_path, scopes=self._scopes
        )
        return build('drive', 'v3', credentials=creds)

    def _validate_not_empty(self, local_parquet):
        """Apenas garante que o arquivo Parquet gerado tem conteúdo."""
        con = duckdb.connect()
        count = con.execute(
            f"SELECT COUNT(*) FROM read_parquet('{local_parquet}')"
        ).fetchone()[0]
        if count == 0:
            raise ValueError("DQ Fail: Arquivo gerado está vazio.")
        print(f"Check Bronze: {count} registros encontrados.")

    def process_file_to_bronze(
        self,
        folder_id,
        bucket_landing,
        bucket_bronze,
        dataset_name,
        data_referencia,
        expected_columns,
    ):
        print(f"--- Início do Processamento: {dataset_name} (Modo Bypass Ativo) ---")

        try:
            # Autenticação direta (Igual ao que fizemos no Colab)
            service = self._get_drive_service()

            # 1. Lista arquivos na pasta do Drive
            query = f"'{folder_id}' in parents and trashed = false"
            results = service.files().list(q=query, fields="files(id, name)").execute()
            items = results.get('files', [])

            if not items:
                print(f"Aviso: Nenhuma arquivo encontrado para o ID: {folder_id}")
                return

            for item in items:
                file_id = item['id']
                file_name = item['name']

                # Filtro para processar apenas CSVs
                if not file_name.lower().endswith('.csv'):
                    continue

                print(f"Baixando e convertendo: {file_name}...")

                # 2. Download via Stream (Direto para a memória temporária)
                request = service.files().get_media(fileId=file_id)
                fh = io.BytesIO()
                downloader = MediaIoBaseDownload(fh, request)

                done = False
                while not done:
                    status, done = downloader.next_chunk()

                # 3. Salva temporariamente para processamento do DuckDB
                local_csv = f"/tmp/{file_name}"
                with open(local_csv, "wb") as f:
                    f.write(fh.getbuffer())

                # 4. Transformação Bronze (Parquet + Hive Partitioning)
                ano, mes, dia = data_referencia.split('-')
                output_name = file_name.lower().replace('.csv', '.parquet')
                local_parquet = f"/tmp/{output_name}"

                # DuckDB lê o CSV e já gera o Parquet otimizado
                con = duckdb.connect()
                con.execute(f"COPY (SELECT * FROM read_csv_auto('{local_csv}')) TO '{local_parquet}' (FORMAT 'PARQUET')")

                self._validate_not_empty(local_parquet)

                # 5. Upload para GCS (Landing e Bronze)
                # O GCSHook costuma ser mais estável, mas o bypass é o plano B
                path_landing = f"{dataset_name}/{ano}/{mes}/{dia}/{file_name}"
                path_bronze = f"{dataset_name}/ano={ano}/mes={mes}/dia={dia}/{output_name}"

                self._gcs_hook.upload(bucket_name=bucket_landing, object_name=path_landing, filename=local_csv)
                self._gcs_hook.upload(bucket_name=bucket_bronze, object_name=path_bronze, filename=local_parquet)

                # Limpeza de arquivos temporários da VPS
                os.remove(local_csv)
                os.remove(local_parquet)
                print(f"Sucesso: {file_name} ingerido na camada Bronze.")

        except Exception as e:
            print(f"Falha na ingestão de {dataset_name}: {str(e)}")
            raise e
