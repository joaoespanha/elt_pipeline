from google.cloud import bigquery
import os
import pandas_gbq
from dotenv import load_dotenv
import logging

load_dotenv()

# Configuração do logging
log_file_path = "elt/model/google_connections/bigquery_process.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(log_file_path),  # Logging em arquivo
        logging.StreamHandler(),  # Logging no console (terminal)
    ],
)

# Caminho para o arquivo JSON da chave de conta de serviço
json_key_path = os.path.expanduser("~/bigquery_gcp_key.json")

# Inicializa um cliente BigQuery
client = bigquery.Client.from_service_account_json(json_key_path)

# Especifica o projeto e o dataset do BigQuery
project_id = os.getenv("BIGQUERY_PROJECT_ID")
dataset_name = os.getenv("BIGQUERY_DATASET_NAME")


def table_exists_in_bigquery(table_name):
    """
    Verifica se uma tabela existe no dataset do BigQuery.

    :param table_name: Nome da tabela a ser verificada.
    :return: True se a tabela existir, False caso contrário.
    """
    dataset_ref = client.dataset(dataset_name)
    table_ref = dataset_ref.table(table_name)
    try:
        client.get_table(table_ref)
        logging.info(f"Tabela {table_name} existe no BigQuery.")
        return True
    except Exception as e:
        logging.warning(f"Tabela {table_name} não existe no BigQuery: {e}")
        return False


def get_existing_data(table_name, column_name):
    """
    Recupera os dados existentes de uma coluna específica de uma tabela no BigQuery.

    :param table_name: Nome da tabela.
    :param column_name: Nome da coluna a ser recuperada.
    :return: Lista de valores existentes nessa coluna.
    """
    if table_exists_in_bigquery(table_name):
        query = f"SELECT {column_name} FROM `{project_id}.{dataset_name}.{table_name}`"
        df = client.query(query).result().to_dataframe()
        logging.info(
            f"Dados existentes recuperados da tabela {table_name} no BigQuery."
        )
        return df[column_name].tolist()
    else:
        logging.warning(
            f"Tabela {table_name} não existe. Nenhum dado existente para recuperar."
        )
        return []


def load_dataframes_to_bigquery(dfs):
    """
    Carrega múltiplos DataFrames do Pandas para suas respectivas tabelas no BigQuery,
    garantindo que nenhum registro duplicado seja inserido.

    :param dfs: Dicionário onde as chaves são os nomes das tabelas e os valores são DataFrames do Pandas a serem carregados.
    """
    for table_name, df in dfs.items():
        if table_name == "orders":
            existing_order_numbers = get_existing_data("orders", "order_number")
            df = df[~df["order_number"].isin(existing_order_numbers)]

        elif table_name == "terminals":
            existing_terminal_serial_numbers = get_existing_data(
                "terminals", "terminal_serial_number"
            )
            df = df[
                ~df["terminal_serial_number"].isin(existing_terminal_serial_numbers)
            ]

        elif table_name == "customers":
            existing_customer_ids = get_existing_data("customers", "customer_id")
            df = df[~df["customer_id"].isin(existing_customer_ids)]

        elif table_name == "order_proofs":
            existing_order_numbers = get_existing_data("order_proofs", "order_number")
            df = df[~df["order_number"].isin(existing_order_numbers)]

        # Carrega o DataFrame filtrado para o BigQuery usando pandas_gbq
        if not df.empty:
            pandas_gbq.to_gbq(
                df,
                destination_table=f"{dataset_name}.{table_name}",
                project_id=client.project,
                if_exists="append",
            )
            logging.info(
                f"Dados carregados com sucesso em {dataset_name}.{table_name}."
            )
        else:
            logging.info(
                f"Não há novos dados para carregar em {dataset_name}.{table_name}."
            )
