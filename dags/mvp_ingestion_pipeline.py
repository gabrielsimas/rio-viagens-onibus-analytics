from modules.mvp_orchestrator_dag import PipelineOrchestrator

orchestrator = PipelineOrchestrator()
dag = orchestrator.create_dag(
    dag_id='mvp_ingestion_pipeline_v2',
    schedule_interval='@daily'
)