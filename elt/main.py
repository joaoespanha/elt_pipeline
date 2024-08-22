from model.pg_connections.dev_main import process_csv_to_postgres
from services.utils import move_csv_file, check_for_files
from model.pg_connections.dev_main import get_full_pg_data
from services.transformations.main import transform_data
from model.google_connections.bigquery import load_dataframes_to_bigquery
from services.get_order_proof_data import get_order_proof_data

directory_path = "elt/data/unprocessed_csvs"  # Directory to check for CSV files
destination_dir = "elt/data/processed_csvs"  # Destination directory for processed files
table_name = "raw_csv_orders"  # Replace with your desired table name
batch_size = 1000  # Define the batch size for loading
log_file_path = "elt/parquet_to_pg/csv_process.log"  # Log file path


# Record the start time of the script
# Step 1: Check if there are files in the directory and get the first file path
csv_file_path = check_for_files(directory_path)

if csv_file_path:
    # Step 2: Process the CSV file and load it into PostgreSQL
    process_csv_to_postgres(csv_file_path, table_name, batch_size)
    # Step 3: Rename and move the processed CSV file
    # move_csv_file(csv_file_path, destination_dir)

print("CSV processing completed successfully.")


print("Starting data transformation...")
pg_data = get_full_pg_data()

load_data = transform_data(pg_data)

order_proof_data = get_order_proof_data(
    "desafio-eng-dados", prefix="evidencias_atendimentos/"
)
print("Data transformation completed successfully.")
print(transform_data)
load_data["order_proofs"] = order_proof_data["order_proofs"]

# Step 4: Load the transformed data into BigQuery
print("Starting data loading to BigQuery...")
load_dataframes_to_bigquery(load_data)
print("Data loading to BigQuery completed successfully.")
