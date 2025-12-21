import os
from airflow import DAG
from datetime import datetime, timedelta
from modules.dataops_manager import DataOpsManager
from airflow.operators.python import PythonOperator
from modules.ingestion_manager import IngestionManager

# Em modules/mvp_orchestrator_dag.py

SCHEMAS = {
    "clima_pluviometria": """
        primary_key VARCHAR, id_estacao VARCHAR, acumulado_chuva_15_min DOUBLE,
        acumulado_chuva_1_h DOUBLE, acumulado_chuva_4_h DOUBLE, acumulado_chuva_24_h DOUBLE,
        acumulado_chuva_96_h DOUBLE, horario TIME, data_particao DATE
    """,
    "estacoes_clima": """
        id_estacao VARCHAR, estacao VARCHAR, latitude DOUBLE, longitude DOUBLE,
        cota DOUBLE, x DOUBLE, y DOUBLE, endereco VARCHAR, situacao VARCHAR,
        data_inicio_operacao TIMESTAMP, data_fim_operacao TIMESTAMP, data_atualizacao TIMESTAMP
    """,
    "licenciamento_frota": """
        carroceria VARCHAR, id_chassi BIGINT, id_fabricante_chassi BIGINT, nome_chassi VARCHAR,
        id_planta BIGINT, tipo_veiculo VARCHAR, status VARCHAR, data_inicio_vinculo DATE,
        data_ultima_vistoria DATE, ano_ultima_vistoria BIGINT, ultima_situacao VARCHAR,
        tecnologia VARCHAR, quantidade_lotacao_pe BIGINT, quantidade_lotacao_sentado BIGINT,
        tipo_combustivel VARCHAR, indicador_ar_condicionado BOOLEAN, indicador_elevador BOOLEAN,
        indicador_usb BOOLEAN, indicador_wifi BOOLEAN, indicador_veiculo_lacrado BOOLEAN,
        indicador_data_ultima_vistoria_tratada BOOLEAN, data_arquivo_fonte DATE,
        versao VARCHAR, datetime_ultima_atualizacao TIMESTAMP, id_execucao_dbt VARCHAR,
        ano_ultima_vistoria_atualizado BIGINT
    """,
    "reclamacoes_1746": """
        id_unidade_organizacional VARCHAR, nome_unidade_organizacional VARCHAR,
        id_unidade_organizacional_mae VARCHAR, unidade_organizacional_ouvidoria VARCHAR,
        categoria VARCHAR, id_tipo VARCHAR, tipo VARCHAR, id_subtipo VARCHAR,
        subtipo VARCHAR, status VARCHAR, longitude DOUBLE, latitude DOUBLE,
        data_alvo_finalizacao TIMESTAMP, data_alvo_diagnostico TIMESTAMP,
        data_real_diagnostico TIMESTAMP, tempo_prazo BIGINT, prazo_unidade VARCHAR,
        prazo_tipo VARCHAR, dentro_prazo VARCHAR, situacao VARCHAR, tipo_situacao VARCHAR,
        justificativa_status VARCHAR, reclamacoes BIGINT, extracted_at TIMESTAMP,
        updated_at VARCHAR, data_particao DATE
    """,
    "viagens_onibus": """
        data DATE, consorcio VARCHAR, tipo_dia VARCHAR, id_empresa VARCHAR,
        id_veiculo VARCHAR, id_viagem VARCHAR, servico VARCHAR, shape_id VARCHAR,
        sentido VARCHAR, datetime_partida TIMESTAMP, datetime_chegada TIMESTAMP,
        tempo_viagem BIGINT, distancia_planejada DOUBLE, perc_conformidade_shape DOUBLE,
        perc_conformidade_registros DOUBLE, versao_modelo VARCHAR
    """,
}


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
            'clima_pluviometria': os.getenv('FOLDER_ID_CLIMA'),
            'estacoes_clima': os.getenv('FOLDER_ID_ESTACOES')
        }
        self._ingestor = IngestionManager()

        # 3. Define Argumentos Padrão do Airflow
        self._default_args = {
            "owner": "Gabriel Simas",
            "depends_on_past": False,
            "email_on_failure": False,
            "retries": 1,
            "retry_delay": timedelta(seconds=10),
            "start_date": datetime(2024, 1, 1),
        }

        self._dataops = DataOpsManager()

    def create_dag(self, dag_id: str, schedule_interval):

        with DAG(
            dag_id=dag_id,
            default_args=self._default_args,
            description="Pipeline de Ingestão: Drive -> Landing -> Bronze (Parquet)",
            schedule_interval=schedule_interval,
            catchup=False,
            tags=["ingestao", "bronze", "duckdb", "poo"],
        ) as dag:

            # Loop de Datasets
            for dataset_name, folder_id in self._datasets_config.items():

                if not folder_id:
                    continue

                # 1. Ingestão (Drive -> GCS)
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

                # 2. Registro (Dremio) com Tipagem Forte
                register_task = PythonOperator(
                    task_id=f"register_{dataset_name}_on_dremio",
                    python_callable=self._dataops.ensure_table_exists,
                    op_kwargs={
                        "dataset_name": dataset_name,
                        "bucket_name": self._bucket_bronze,
                        "schema_string": SCHEMAS.get(dataset_name),
                    },
                )

                # Define o fluxo isolado: Branch -> Ingest -> Register -> Merge
                ingest_task >> register_task

            return dag
