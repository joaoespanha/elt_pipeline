import pandas as pd
from sqlalchemy import create_engine, inspect
from services.utils import move_csv_file, check_for_files
import logging
import time
from dotenv import load_dotenv
import os

load_dotenv()

# Definir caminhos e detalhes de conexão
directory_path = "elt/data/unprocessed_csvs"  # Diretório para verificar arquivos CSV
destination_dir = (
    "elt/data/processed_csvs"  # Diretório de destino para arquivos processados
)
table_name = "raw_csv_orders"  # Substitua pelo nome da tabela desejada
batch_size = 1000  # Definir o tamanho do lote para carregamento
log_file_path = "elt/model/pg_connections/csv_process.log"  # Caminho do arquivo de log

postgres_user = os.getenv("POSTGRES_USER")
postgres_password = os.getenv("POSTGRES_PASSWORD")
postgres_host = os.getenv("POSTGRES_HOST")
postgres_port = os.getenv("POSTGRES_PORT")
postgres_db = os.getenv("POSTGRES_DB")

# Configuração do logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(log_file_path),  # Logging em arquivo
        logging.StreamHandler(),  # Logging no console (terminal)
    ],
)


def get_full_pg_data():
    """
    Obtenha todos os dados da tabela PostgreSQL.

    :return: DataFrame com todos os dados da tabela PostgreSQL.
    """
    try:
        logging.info("Criando conexão com o banco de dados PostgreSQL")
        engine = create_engine(
            f"postgresql://{postgres_user}:{postgres_password}@{postgres_host}:{postgres_port}/{postgres_db}"
        )

        logging.info(f"Lendo dados da tabela PostgreSQL: {table_name}")
        query = f"SELECT * FROM {table_name}"
        df = pd.read_sql_query(query, engine)

        logging.info(
            f"Leitura bem-sucedida de {len(df)} registros da tabela PostgreSQL"
        )
        return df

    except Exception as e:
        logging.error(f"Ocorreu um erro durante a recuperação de dados: {e}")
        raise  # Re-lançar a exceção para tratá-la posteriormente, se necessário


def table_exists(engine, table_name):
    """
    Verifica se uma tabela existe no banco de dados PostgreSQL.

    :param engine: Objeto engine do SQLAlchemy.
    :param table_name: Nome da tabela a verificar.
    :return: True se a tabela existir, False caso contrário.
    """
    inspector = inspect(engine)
    return inspector.has_table(table_name)


def load_existing_data_from_pg(engine):
    """
    Carrega os números de pedido existentes da tabela PostgreSQL, se existir.

    :param engine: Objeto engine do SQLAlchemy.
    :return: DataFrame com os números de pedido existentes.
    """
    if table_exists(engine, table_name):
        query = f"SELECT order_number FROM {table_name}"
        existing_df = pd.read_sql_query(query, engine)
        return existing_df
    else:
        logging.info(
            f"Tabela {table_name} não existe. Pulando carregamento de dados existentes."
        )
        return pd.DataFrame(columns=["order_number"])


def filter_new_data(df, existing_df):
    """
    Filtra as linhas no CSV que já existem no PostgreSQL.

    :param df: DataFrame com os dados do CSV.
    :param existing_df: DataFrame com os dados existentes no PostgreSQL.
    :return: DataFrame com os novos dados que não estão no PostgreSQL.
    """
    new_data = df[~df["order_number"].isin(existing_df["order_number"])]
    return new_data


def process_csv_to_postgres(csv_file, table_name, batch_size):
    """
    Processa o arquivo CSV e carrega os dados no PostgreSQL em lotes.

    :param csv_file: Caminho do arquivo CSV.
    :param table_name: Nome da tabela no PostgreSQL.
    :param batch_size: Tamanho do lote para inserção dos dados.
    """
    try:
        logging.info(
            "Iniciando o processo para carregar dados do CSV para o PostgreSQL"
        )

        # Criar uma conexão com o banco de dados PostgreSQL
        logging.info(
            f"Criando conexão com o banco de dados PostgreSQL em {postgres_host}:{postgres_port}"
        )
        engine = create_engine(
            f"postgresql://{postgres_user}:{postgres_password}@{postgres_host}:{postgres_port}/{postgres_db}"
        )

        # Ler o arquivo CSV em um DataFrame
        logging.info(f"Lendo o arquivo CSV de {csv_file}")
        df = pd.read_csv(csv_file)
        logging.info(f"Arquivo CSV lido com sucesso com {len(df)} registros")

        # Carregar dados existentes do PostgreSQL
        logging.info(
            f"Verificando se a tabela {table_name} existe e carregando dados existentes, se disponíveis"
        )
        existing_df = load_existing_data_from_pg(engine)

        # Filtrar os dados que já existem na tabela PostgreSQL
        logging.info("Filtrando registros já presentes na tabela PostgreSQL")
        new_data = filter_new_data(df, existing_df)

        if not new_data.empty:
            # Carregamento em lotes na tabela PostgreSQL dentro de uma transação
            logging.info(f"Iniciando inserção em lotes com tamanho de {batch_size}")
            with engine.begin() as connection:  # Gerenciamento explícito de transação
                for i in range(0, len(new_data), batch_size):
                    batch_df = new_data[i : i + batch_size]
                    logging.info(
                        f"Inserindo registros de {i} a {i + len(batch_df)} na tabela PostgreSQL: {table_name}"
                    )
                    batch_df.to_sql(
                        table_name,
                        connection,
                        if_exists="append",
                        index=False,
                        method="multi",
                    )

            logging.info(
                f"Dados carregados com sucesso na tabela {table_name} no PostgreSQL"
            )
        else:
            logging.info("Nenhum dado novo para carregar.")

    except Exception as e:
        logging.error(f"Ocorreu um erro durante o processamento: {e}")
        raise  # Re-lançar a exceção para tratá-la posteriormente, se necessário


if __name__ == "__main__":
    try:
        # Registrar o horário de início do script
        start_time = time.time()

        # Verificar se há arquivos no diretório e obter o caminho do primeiro arquivo
        csv_file_path = check_for_files(directory_path)

        if csv_file_path:
            # Processar o arquivo CSV e carregá-lo no PostgreSQL
            process_csv_to_postgres(csv_file_path, table_name, batch_size)

            # Renomear e mover o arquivo CSV processado
            move_csv_file(csv_file_path, destination_dir)
        else:
            logging.info("Nenhum arquivo encontrado para processar.")

    finally:
        # Registrar o horário de término e calcular a duração total
        end_time = time.time()
        total_time = end_time - start_time
        logging.info(f"Processo concluído em {total_time:.2f} segundos")
