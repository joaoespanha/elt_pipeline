import logging
from model.pg_connections.dev_main import process_parquet_to_postgres
from services.utils import check_for_files
from services.transformations.main import transform_data
from model.google_connections.bigquery import load_dataframes_to_bigquery
from services.get_order_proof_data import get_order_proof_data

directory_path = "elt/data/unprocessed_parquets"  # Directory to check for parquet files
destination_dir = (
    "elt/data/processed_parquets"  # Destination directory for processed files
)
table_name = "raw_parquet_orders"  # Replace with your desired table name
batch_size = 25000  # Define the batch size for loading
log_file_path = "elt/parquet_to_pg/parquet_process.log"  # Log file path

log_file_path = "elt/main.log"

# Configuração do logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(log_file_path),  # Logging em arquivo
        logging.StreamHandler(),  # Logging no console (terminal)
    ],
)

# Record the start time of the script
# Step 1: Check if there are files in the directory and get the first file path
parquet_file_paths = check_for_files(directory_path)
logging.info(f"Parquet file paths: {parquet_file_paths}")

order_proof_data = get_order_proof_data(
    "desafio-eng-dados", prefix="evidencias_atendimentos/"
)

load_dataframes_to_bigquery(order_proof_data)

for parquet_file_path in parquet_file_paths:
    logging.info(f"Running for {parquet_file_path}")

    # Step 2: Process the parquet file and load it into PostgreSQL
    new_data_df = process_parquet_to_postgres(parquet_file_path, table_name, batch_size)

    if not new_data_df.empty:
        logging.info("parquet processing completed successfully.")

        load_data = transform_data(new_data_df)

        logging.info("Data transformation completed successfully.")

        # Step 3: Load the transformed data into BigQuery
        logging.info("Starting data loading to BigQuery...")
        load_dataframes_to_bigquery(load_data, batch_size)
        logging.info("Data loading to BigQuery completed successfully.")
    else:
        logging.error("parquet processing skipped.")
