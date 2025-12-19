import os
import pytest
from unittest.mock import MagicMock, patch, ANY

# Ajuste a importação conforme a estrutura da sua pasta.
# Se ingestion_manager.py estiver dentro de modules/, use
# from modules.ingestion_manager import IngestionManager
from modules.ingestion_manager import IngestionManager


@pytest.fixture
def mock_gcp_hooks():
    """Fixture para mockar todas as dependências externas de nuvem."""
    with patch("modules.ingestion_manager.GoogleDriveHook") as mock_drive_hook, patch(
        "modules.ingestion_manager.GCSHook"
    ) as mock_gcs_hook, patch("modules.ingestion_manager.duckdb") as mock_duckdb:

        # Configura os retornos dos mocks para não darem erro de atributo
        mock_drive_instance = mock_drive_hook.return_value
        mock_gcs_instance = mock_gcs_hook.return_value

        yield {
            "drive": mock_drive_instance,
            "gcs": mock_gcs_instance,
            "duckdb": mock_duckdb,
        }

class TestIngestionManager:

    def test_process_file_success_flow(self, mock_gcp_hooks):
        """
        Cenário feliz
        - Lista 1 arquivos no Drive.
        - Baixa, Converte e Sobe para Landing e Bronze.
        """

        # 1. SETUP
        manager = IngestionManager()
        mocks = mock_gcp_hooks

        # Simula a resposta do Drive
        mock_file = {'id': '12345', 'name': 'dados_brutos.csv'}
        mocks['drive'].get_conn().files.return_value.list.return_value.execute.return_value.get.return_value = [mock_file]

        # Simula o conteúdo baixado (bytes)
        mocks[
            "drive"
        ].get_conn.return_value.files.return_value.get_media.return_value.execute.return_value = b"col1,col2\nval1,val2"

        # Simula que o arquivo NÃO existe na Landing (para forçar o upload)
        mocks['gcs'].exists.return_value = False

        # 2. EXECUÇÃO
        manager.process_file_to_bronze(
            folder_id='folder_xyz',
            bucket_landing='landing_bucket',
            bucket_bronze='bronze_bucket',
            dataset_name='teste_unitario'
        )

        # 3. ASSERÇÕES

        # Verificou se chamou o download do Drive
        mocks['drive'].get_conn.return_value.files.return_value.get_media.assert_called_with(fileId='12345')
