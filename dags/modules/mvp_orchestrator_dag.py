import os
from airflow import DAG
from datetime import datetime, timedelta
from modules.dataops_manager import DataOpsManager
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

        self._dataops = DataOpsManager()

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

            # 1. Cria Branch de Trabalho (Isolamento)
            # Usamos a data da execução para garantir unicidade: ex: dev_2025_12_20
            branch_name = "dev_{{ ds_nodash }}"

            create_branch_task = PythonOperator(
                task_id="create_nessie_branch",
                python_callable=self._dataops.create_branch,
                op_kwargs={"branch_name": branch_name},
            )

            # 2. Ingestão e Registro (Tasks Dinâmicas)
            # Criamos uma lista para armazenar as últimas tarefas de cada dataset (os registros)
            # para que o Merge espere por elas.
            last_tasks_before_merge = []
            for dataset_name, folder_id in self._datasets_config.items():

                if not folder_id:
                    print(f"AVISO: Variável de ambiente para '{dataset_name}' não encontrada. Pulando.")
                    continue

                # 2.1 Task de Ingestão (Drive -> GCS)
                ingest_task = PythonOperator(
                    task_id=f"ingest_{dataset_name}",
                    python_callable=self._ingestor.process_file_to_bronze,
                    op_kwargs={
                        "folder_id": folder_id,
                        "bucket_landing": self._bucket_landing,
                        "bucket_bronze": self._bucket_bronze,
                        "dataset_name": dataset_name,
                        "data_referencia": "{{ ds }}",
                    },
                )

                register_task = PythonOperator(
                    task_id=f"register_{dataset_name}_on_dremio",
                    python_callable=self._dataops.ensure_table_exists,
                    op_kwargs={
                        "dataset_name": dataset_name,
                        "branch_name": branch_name,
                        "bucket_name": self._bucket_bronze # Adicionado para bater com o DataOpsManager
                    },
                )

                # Define o fluxo para este dataset específico:
                # Cria Branch -> Ingestão -> Registro no Dremio
                create_branch_task >> ingest_task >> register_task

                # Adicionamos o register_task na lista. O Merge dependerá dele.
                last_tasks_before_merge.append(register_task)

            # 3. Merge (Publicação Atômica)
            # Só acontece se todas as ingestões terminarem com sucesso
            publish_data = PythonOperator(
                task_id="merge_to_main",
                python_callable=self._dataops.merge_branch,
                op_kwargs={"branch_name": branch_name, "target_ref": "main"},
            )

            last_tasks_before_merge >> publish_data

            return dag
