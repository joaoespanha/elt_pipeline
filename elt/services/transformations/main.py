import logging
import pandas as pd
from pandas.tseries.offsets import BDay
from model.pg_connections.dev_main import get_full_pg_data

# Nome da tabela no PostgreSQL que contém os dados brutos extraídos do CSV
table_name = "raw_csv_orders"  # Substitua pelo nome desejado da tabela


# Caminho do arquivo de log para registrar as operações de transformação
log_file_path = (
    "elt/services/transformations/tansformations.log"  # Caminho do arquivo de log
)

# Configuração do logging para registrar as mensagens de informação no arquivo de log e no console
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(log_file_path),  # Registro em arquivo
        logging.StreamHandler(),  # Registro no console (terminal)
    ],
)


def transform_data(df):
    """
    Transforma os dados extraídos do PostgreSQL em três tabelas separadas: Terminals, Orders e Customers.

    :param df: DataFrame com os dados extraídos da tabela "raw_data_orders".
    :return: Dicionário contendo os DataFrames transformados para Terminals, Orders e Customers.
    """

    # Tabela de Terminais
    terminals_df = df[
        [
            "terminal_serial_number",
            "terminal_model",
            "terminal_type",
        ]
    ]

    # Tabela de Pedidos (Orders) com a coluna adicional "is_business_day"
    orders_df = df[
        [
            "order_number",
            "terminal_serial_number",
            "customer_id",
            "technician_email",
            "arrival_date",
            "deadline_date",
            "cancellation_reason",
            "city",
            "country",
            "country_state",
            "zip_code",
            "street_name",
            "neighborhood",
            "complement",
            "provider",
        ]
    ]

    # Adiciona a coluna "is_business_day" que verifica se a data de chegada é um dia útil
    orders_df["is_business_day"] = pd.to_datetime(orders_df["arrival_date"]).apply(
        lambda x: x == x + BDay(0)
    )

    # Tabela de Clientes (Customers)
    customers_df = df[
        [
            "customer_id",
            "customer_phone",
        ]
    ]

    # Dicionário que agrupa as tabelas resultantes da transformação
    result_tables = {
        "customers": customers_df,
        "orders": orders_df,
        "terminals": terminals_df,
    }

    print("Tabelas criadas com sucesso")
    return result_tables


# Exemplo de uso
if __name__ == "__main__":
    # Extrai os dados completos da tabela PostgreSQL usando a função get_full_pg_data
    df = get_full_pg_data()

    # Realiza a transformação dos dados
    result_tables = transform_data(df)

    # Exibe as tabelas resultantes da transformação
    print(result_tables)

    # Registra no log que a transformação dos dados foi concluída com sucesso
    logging.info("Transformação dos dados concluída com sucesso.")
