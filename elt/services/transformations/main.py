import logging
import pandas as pd
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

    df["arrival_date"] = pd.to_datetime(df["arrival_date"], errors="coerce")
    df["deadline_date"] = pd.to_datetime(df["deadline_date"], errors="coerce")

    # Tabela de Terminais
    terminals_df = df[
        [
            "terminal_serial_number",
            "terminal_model",
            "terminal_type",
        ]
    ]
    terminals_df = terminals_df.drop_duplicates(subset="terminal_serial_number")

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
    orders_df["is_business_day"] = orders_df["arrival_date"].apply(
        lambda x: pd.Timestamp(x).dayofweek < 5 and not pd.isnull(x)
    )

    # Tabela de Clientes (Customers)
    customers_df = df[
        [
            "customer_id",
            "customer_phone",
        ]
    ]
    customers_df = customers_df.drop_duplicates(subset="customer_id")

    # Dicionário que agrupa as tabelas resultantes da transformação
    result_tables = {
        "customers": customers_df,
        "orders": orders_df,
        "terminals": terminals_df,
    }

    print("Tabelas criadas com sucesso")
    return result_tables
