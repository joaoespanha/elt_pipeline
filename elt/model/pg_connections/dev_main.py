import pandas as pd
from sqlalchemy import create_engine, inspect
from services.utils import move_csv_file, check_for_files
import logging
import time
from dotenv import load_dotenv
import os

load_dotenv()


# Define paths and connection details
directory_path = "elt/data/unprocessed_csvs"  # Directory to check for CSV files
destination_dir = "elt/data/processed_csvs"  # Destination directory for processed files
table_name = "raw_csv_orders"  # Replace with your desired table name
postgres_password = "secret"
batch_size = 1000  # Define the batch size for loading
log_file_path = "elt/model/pg_connections/csv_process.log"  # Log file path

postgres_user = os.getenv("POSTGRES_USER")
postgres_password = os.getenv("POSTGRES_PASSWORD")
postgres_host = os.getenv("POSTGRES_HOST")
postgres_port = os.getenv("POSTGRES_PORT")
postgres_db = os.getenv("POSTGRES_DB")


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(log_file_path),  # File logging
        logging.StreamHandler(),  # Console (terminal) logging
    ],
)


def get_full_pg_data():
    """
    Get the full data from the PostgreSQL table.

    :return: DataFrame with all the data from the PostgreSQL table.
    """
    try:
        logging.info("Creating connection to PostgreSQL database")
        engine = create_engine(
            f"postgresql://{postgres_user}:{postgres_password}@{postgres_host}:{postgres_port}/{postgres_db}"
        )

        logging.info(f"Reading data from the PostgreSQL table: {table_name}")
        query = f"SELECT * FROM {table_name}"
        df = pd.read_sql_query(query, engine)

        logging.info(f"Successfully read {len(df)} records from the PostgreSQL table")
        return df

    except Exception as e:
        logging.error(f"An error occurred during data retrieval: {e}")
        raise  # Re-raise the exception to handle it later if necessary


def table_exists(engine, table_name):
    """
    Check if a table exists in the PostgreSQL database.

    :param engine: SQLAlchemy engine object.
    :param table_name: Name of the table to check.
    :return: True if the table exists, False otherwise.
    """
    inspector = inspect(engine)
    return inspector.has_table(table_name)


def load_existing_data_from_pg(engine):
    """
    Load existing order numbers from the PostgreSQL table if it exists.

    :param engine: SQLAlchemy engine object.
    :return: DataFrame of existing order numbers.
    """
    if table_exists(engine, table_name):
        query = f"SELECT order_number FROM {table_name}"
        existing_df = pd.read_sql_query(query, engine)
        return existing_df
    else:
        logging.info(
            f"Table {table_name} does not exist. Skipping loading of existing data."
        )
        return pd.DataFrame(columns=["order_number"])


def filter_new_data(df, existing_df):
    # Filter out rows in the CSV data that already exist in PostgreSQL
    new_data = df[~df["order_number"].isin(existing_df["order_number"])]
    return new_data


# Function to process the CSV file and load it into PostgreSQL in batches
def process_csv_to_postgres(csv_file, table_name, batch_size):
    try:
        logging.info("Starting the process to load data from CSV to PostgreSQL")

        # Create a connection to the PostgreSQL database
        logging.info(
            f"Creating connection to PostgreSQL database at {postgres_host}:{postgres_port}"
        )
        engine = create_engine(
            f"postgresql://{postgres_user}:{postgres_password}@{postgres_host}:{postgres_port}/{postgres_db}"
        )

        # Read the CSV file into a DataFrame
        logging.info(f"Reading CSV file from {csv_file}")
        df = pd.read_csv(csv_file)
        logging.info(f"Successfully read the CSV file with {len(df)} records")

        # Load existing data from PostgreSQL
        logging.info(
            f"Checking if table {table_name} exists and loading existing data if available"
        )
        existing_df = load_existing_data_from_pg(engine)

        # Filter out any data that already exists in the PostgreSQL table
        logging.info("Filtering out records already present in the PostgreSQL table")
        new_data = filter_new_data(df, existing_df)

        if not new_data.empty:
            # Batch Loading into the PostgreSQL table within a transaction
            logging.info(f"Starting batch insertion with batch size of {batch_size}")
            with engine.begin() as connection:  # Explicit transaction management
                for i in range(0, len(new_data), batch_size):
                    batch_df = new_data[i : i + batch_size]
                    logging.info(
                        f"Inserting records {i} to {i + len(batch_df)} into the PostgreSQL table: {table_name}"
                    )
                    batch_df.to_sql(
                        table_name,
                        connection,
                        if_exists="append",
                        index=False,
                        method="multi",
                    )

            logging.info(
                f"Data successfully loaded into the {table_name} table in PostgreSQL"
            )
        else:
            logging.info("No new data to load.")

    except Exception as e:
        logging.error(f"An error occurred during processing: {e}")
        raise  # Re-raise the exception to handle it later if necessary


if __name__ == "__main__":
    try:
        # Record the start time of the script
        start_time = time.time()

        # Check if there are files in the directory and get the first file path
        csv_file_path = check_for_files(directory_path)

        if csv_file_path:
            # Process the CSV file and load it into PostgreSQL
            process_csv_to_postgres(csv_file_path, table_name, batch_size)

            # Rename and move the processed CSV file
            move_csv_file(csv_file_path, destination_dir)
        else:
            logging.info("No files found to process.")

    finally:
        # Record the end time and calculate the total duration
        end_time = time.time()
        total_time = end_time - start_time
        logging.info(f"Process completed in {total_time:.2f} seconds")
