import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
from services.get_order_proof_data import get_order_proof_data


class TestGetOrderProofData(unittest.TestCase):
    """
    Classe de testes para a função get_order_proof_data, responsável por validar o comportamento da função
    ao processar arquivos de imagem armazenados em um bucket do Google Cloud Storage (GCS).
    """

    @patch("services.get_order_proof_data.get_storage_client")
    def test_get_order_proof_data(self, mock_get_storage_client):
        """
        Testa a função get_order_proof_data com arquivos válidos de extensão .jpg.
        O objetivo é garantir que a função retorne os dados corretos para os arquivos presentes no bucket.
        """
        # Mock do cliente do Google Cloud Storage e do bucket
        mock_client = MagicMock()
        mock_bucket = MagicMock()
        mock_blob1 = MagicMock()
        mock_blob2 = MagicMock()

        # Definindo os valores de retorno para os blobs
        mock_blob1.name = "evidencias_atendimentos/6400342.jpg"
        mock_blob2.name = "evidencias_atendimentos/6400649.jpg"

        # Configura o retorno de list_blobs para incluir apenas os blobs mockados
        mock_bucket.list_blobs.return_value = [mock_blob1, mock_blob2]
        mock_client.bucket.return_value = mock_bucket

        # Configura o retorno de get_storage_client para retornar o cliente mockado
        mock_get_storage_client.return_value = mock_client

        # Chama a função com os dados mockados
        bucket_name = "desafio-eng-dados"
        prefix = "evidencias_atendimentos/"
        result = get_order_proof_data(bucket_name, prefix)

        # Cria o DataFrame esperado
        expected_df = pd.DataFrame(
            {
                "order_number": ["6400342", "6400649"],
                "gcs_path": [
                    "gs://desafio-eng-dados/evidencias_atendimentos/6400342.jpg",
                    "gs://desafio-eng-dados/evidencias_atendimentos/6400649.jpg",
                ],
            }
        )

        # Verifica se o resultado corresponde ao DataFrame esperado
        pd.testing.assert_frame_equal(result["order_proofs"], expected_df)

        # Verifica se os métodos mockados foram chamados com os parâmetros corretos
        mock_client.bucket.assert_called_once_with(bucket_name)
        mock_bucket.list_blobs.assert_called_once_with(prefix=prefix)

    @patch("services.get_order_proof_data.get_storage_client")
    def test_ignore_non_jpeg_files(self, mock_get_storage_client):
        """
        Testa a função get_order_proof_data para garantir que arquivos que não possuem a extensão .jpg ou .jpeg
        sejam ignorados durante o processamento.
        """
        # Mock do cliente do Google Cloud Storage e do bucket
        mock_client = MagicMock()
        mock_bucket = MagicMock()
        mock_blob = MagicMock()

        # Arquivo com extensão diferente
        mock_blob.name = "evidencias_atendimentos/6400342.png"

        mock_bucket.list_blobs.return_value = [mock_blob]
        mock_client.bucket.return_value = mock_bucket
        mock_get_storage_client.return_value = mock_client

        bucket_name = "desafio-eng-dados"
        prefix = "evidencias_atendimentos/"
        result = get_order_proof_data(bucket_name, prefix)

        # DataFrame esperado deve estar vazio, pois nenhum arquivo .jpg ou .jpeg foi encontrado
        expected_df = pd.DataFrame(columns=["order_number", "gcs_path"])

        pd.testing.assert_frame_equal(result["order_proofs"], expected_df)


if __name__ == "__main__":

    runner = unittest.TextTestRunner(verbosity=2)  # verbosity=2 for detailed output
    unittest.main(testRunner=runner)
