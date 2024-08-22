from model.pg_connections.dev_main import process_parquet_to_postgres
from services.utils import check_for_files
from model.pg_connections.dev_main import get_full_pg_data
from services.transformations.main import transform_data
from model.google_connections.bigquery import load_dataframes_to_bigquery
from services.get_order_proof_data import get_order_proof_data

directory_path = "elt/data/unprocessed_parquets"  # Directory to check for parquet files
destination_dir = (
    "elt/data/processed_parquets"  # Destination directory for processed files
)
table_name = "raw_parquet_orders"  # Replace with your desired table name
batch_size = 10000  # Define the batch size for loading
log_file_path = "elt/parquet_to_pg/parquet_process.log"  # Log file path


# Record the start time of the script
# Step 1: Check if there are files in the directory and get the first file path
parquet_file_paths = check_for_files(directory_path)

order_proof_data = get_order_proof_data(
    "desafio-eng-dados", prefix="evidencias_atendimentos/"
)

load_dataframes_to_bigquery(order_proof_data)

for parquet_file_path in parquet_file_paths:
    # Step 2: Process the parquet file and load it into PostgreSQL
    process_parquet_to_postgres(parquet_file_path, table_name, batch_size)

    print("parquet processing completed successfully.")

    print("Starting data transformation...")
    pg_data = get_full_pg_data()

    load_data = transform_data(pg_data)

    print("Data transformation completed successfully.")
    print(transform_data)

    # Step 3: Load the transformed data into BigQuery
    print("Starting data loading to BigQuery...")
    load_dataframes_to_bigquery(load_data)
    print("Data loading to BigQuery completed successfully.")
