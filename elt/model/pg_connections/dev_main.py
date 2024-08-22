import pandas as pd
from sqlalchemy import create_engine, inspect
import logging
from dotenv import load_dotenv
import os

load_dotenv()

# Definir caminhos e detalhes de conexão
directory_path = (
    "elt/data/unprocessed_parquets"  # Diretório para verificar arquivos parquet
)
destination_dir = (
    "elt/data/processed_parquets"  # Diretório de destino para arquivos processados
)
table_name = "raw_parquet_orders"  # Substitua pelo nome da tabela desejada
batch_size = 1000  # Definir o tamanho do lote para carregamento
log_file_path = (
    "elt/model/pg_connections/parquet_process.log"  # Caminho do arquivo de log
)

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
    Filtra as linhas no parquet que já existem no PostgreSQL.

    :param df: DataFrame com os dados do parquet.
    :param existing_df: DataFrame com os dados existentes no PostgreSQL.
    :return: DataFrame com os novos dados que não estão no PostgreSQL.
    """
    new_data = df[~df["order_number"].isin(existing_df["order_number"])]
    return new_data


def process_parquet_to_postgres(parquet_file, table_name, batch_size):
    """
    Processa o arquivo parquet e carrega os dados no PostgreSQL em lotes.

    :param parquet_file: Caminho do arquivo parquet.
    :param table_name: Nome da tabela no PostgreSQL.
    :param batch_size: Tamanho do lote para inserção dos dados.
    """
    try:
        logging.info(
            "Iniciando o processo para carregar dados do parquet para o PostgreSQL"
        )

        # Criar uma conexão com o banco de dados PostgreSQL
        logging.info(
            f"Criando conexão com o banco de dados PostgreSQL em {postgres_host}:{postgres_port}"
        )
        engine = create_engine(
            f"postgresql://{postgres_user}:{postgres_password}@{postgres_host}:{postgres_port}/{postgres_db}"
        )

        # Ler o arquivo parquet em um DataFrame
        logging.info(f"Lendo o arquivo parquet de {parquet_file}")
        df = pd.read_parquet(parquet_file)
        logging.info(f"Arquivo parquet lido com sucesso com {len(df)} registros")

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
