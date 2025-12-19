import os
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from modules.ingestion_manager import IngestionManager


class PipelineOrchestrator:
    """
    Classe responsável por definir e construir a DAG do pipeline de ingestão.
    Encapsula a configuração e a definição das tarefas.
    """

    def __init__(self) -> None:
        # 1. Carrega Configurações (Environment Variables)
        # TODO: Adicionar no docker-compose.yml e no .env cada um dos IDs das pastas do Drive aqui!
        self._bucket_landing = os.getenv('BUCKET_LANDING')
        self._bucket_bronze = os.getenv('BUCKET_BRONZE')

        # 2. Configuração Central dos Datasets (Mapeamento: Contexto -> ID da Pasta no Drive)
        # Estes IDs devem estar no .env ou docker-compose.yml
        # A chave do dicionário será o nome da subpasta no GCS
        self._datasets_config = {
            'viagens_onibus': os.getenv('FOLDER_ID_ONIBUS'),
            'licenciamento_frota': os.getenv('FOLDER_ID_FROTA'),
            'clima_pluviometria': os.getenv('FOLDER_ID_CLIMA'),
            'estacoes_clima': os.getenv('FOLDER_ID_ESTACOES'),
            'reclamacoes_1746': os.getenv('FOLDER_ID_1746')
        }
        self._ingestor = IngestionManager()

        # 3. Define Argumentos Padrão do Airflow
        self._default_args = {
            'owner': 'Gabriel Simas',
            'depends_on_past': False,
            'email_on_failure': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
            'start_date': datetime(2024, 1, 1),
        }

    def create_dag(self, dag_id: str, schedule_interval):
        """
        Cria e retorna o objeto DAG com todas as tasks conectadas.
        """
        with DAG(
            dag_id=dag_id,
            default_args=self._default_args,
            description="Pipeline de Ingestão: Drive -> Landing -> Bronze (Parquet)",
            schedule_interval=schedule_interval,
            catchup=False,
            tags=['ingestao', 'bronze', 'duckdb', 'poo']
        ) as dag:

            # Iteração Dinâmica: Cria uma Task para cada pasta configurada
            for dataset_name, folder_id in self._datasets_config.items():

                if not folder_id:
                    print(f"AVISO: Variável de ambiente para '{dataset_name}' não encontrada. Pulando.")
                    continue

                PythonOperator(
                        task_id=f'ingest_{dataset_name}',
                        python_callable=self._ingestor.process_file_to_bronze,
                        op_kwargs={
                            'folder_id': folder_id,
                            'bucket_landing': self._bucket_landing,
                            'bucket_bronze': self._bucket_bronze,
                            'dataset_name': dataset_name  # Passa o contexto para organizar subpastas
                        }
                    )

            # Aqui você pode adicionar mais tasks facilmente
            # ex: task_ingest_full >> self.create_dbt_task(dag)

            return dag
