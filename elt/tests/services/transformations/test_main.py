import unittest
import pandas as pd
from services.transformations.main import transform_data


class TestTransformData(unittest.TestCase):
    """
    Classe de testes para a função transform_data, responsável por validar a transformação dos dados em três tabelas:
    'customers', 'orders' e 'terminals'.
    """

    def setUp(self):
        """
        Configura os dados de exemplo que serão usados nos testes. Este método é executado antes de cada teste.
        """
        # Criação de um DataFrame de exemplo para uso nos testes
        self.sample_data = {
            "terminal_serial_number": ["SN123", "SN456"],
            "terminal_model": ["ModelX", "ModelY"],
            "terminal_type": ["TypeA", "TypeB"],
            "order_number": [1001, 1002],
            "customer_id": [1, 2],
            "technician_email": ["tech1@example.com", "tech2@example.com"],
            "arrival_date": ["2024-01-01", "2024-01-02"],
            "deadline_date": ["2024-02-01", "2024-02-02"],
            "cancellation_reason": [None, "Customer request"],
            "city": ["CityA", "CityB"],
            "country": ["CountryA", "CountryB"],
            "country_state": ["StateA", "StateB"],
            "zip_code": ["12345", "67890"],
            "street_name": ["StreetA", "StreetB"],
            "neighborhood": ["NeighborhoodA", "NeighborhoodB"],
            "complement": ["Apt 1", "Apt 2"],
            "provider": ["ProviderA", "ProviderB"],
            "customer_phone": ["555-1234", "555-5678"],
        }
        self.df = pd.DataFrame(self.sample_data)

        # DataFrames esperados para cada tabela
        self.expected_terminals_df = pd.DataFrame(
            {
                "terminal_serial_number": ["SN123", "SN456"],
                "terminal_model": ["ModelX", "ModelY"],
                "terminal_type": ["TypeA", "TypeB"],
            }
        )

        self.expected_orders_df = pd.DataFrame(
            {
                "order_number": [1001, 1002],
                "terminal_serial_number": ["SN123", "SN456"],
                "customer_id": [1, 2],
                "technician_email": ["tech1@example.com", "tech2@example.com"],
                "arrival_date": ["2024-01-01", "2024-01-02"],
                "deadline_date": ["2024-02-01", "2024-02-02"],
                "cancellation_reason": [None, "Customer request"],
                "city": ["CityA", "CityB"],
                "country": ["CountryA", "CountryB"],
                "country_state": ["StateA", "StateB"],
                "zip_code": ["12345", "67890"],
                "street_name": ["StreetA", "StreetB"],
                "neighborhood": ["NeighborhoodA", "NeighborhoodB"],
                "complement": ["Apt 1", "Apt 2"],
                "provider": ["ProviderA", "ProviderB"],
            }
        )

        self.expected_customers_df = pd.DataFrame(
            {
                "customer_id": [1, 2],
                "customer_phone": ["555-1234", "555-5678"],
            }
        )

    def test_tables_created(self):
        """
        Testa se as três tabelas esperadas ('customers', 'orders', 'terminals') são criadas pela função transform_data.
        """
        # Chama a função com o DataFrame de exemplo
        result_tables = transform_data(self.df)

        # Verifica se as três tabelas esperadas foram criadas
        self.assertIn("customers", result_tables)
        self.assertIn("orders", result_tables)
        self.assertIn("terminals", result_tables)

    def test_data_integrity(self):
        """
        Testa a integridade dos dados, verificando se os DataFrames resultantes contêm os dados corretos.
        """
        # Chama a função com o DataFrame de exemplo
        result_tables = transform_data(self.df)

        # Verifica se os dados dentro dos DataFrames resultantes correspondem aos DataFrames esperados
        pd.testing.assert_frame_equal(
            result_tables["terminals"], self.expected_terminals_df
        )
        pd.testing.assert_frame_equal(result_tables["orders"], self.expected_orders_df)
        pd.testing.assert_frame_equal(
            result_tables["customers"], self.expected_customers_df
        )


if __name__ == "__main__":
    # Executa os testes com saída detalhada
    runner = unittest.TextTestRunner(verbosity=2)
    unittest.main(testRunner=runner)
