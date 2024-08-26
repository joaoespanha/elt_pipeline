import pandas as pd
from model.google_connections.storage_client import get_storage_client


def get_order_proof_data(bucket_name, prefix=None):
    # Initialize Google Cloud Storage client with credentials
    google_storage = get_storage_client()
    # Initialize the bucket object
    bucket = google_storage.bucket(bucket_name)

    # Create an empty list to hold the data
    data = []

    # Loop through each blob in the bucket with an optional prefix
    for blob in bucket.list_blobs(prefix=prefix):
        if blob.name.lower().endswith(".jpg") or blob.name.lower().endswith(".jpeg"):

            print(f"Processing image: {blob.name}")

            # Extract the order number from the filename
            order_number = blob.name.split("/")[-1].split(".")[0]

            # Append the order number and GCS path to the data list
            data.append(
                {
                    "order_number": order_number,
                    "gcs_path": f"gs://{bucket_name}/{blob.name}",
                }
            )

    # Convert the list to a pandas DataFrame
    df = pd.DataFrame(data, columns=["order_number", "gcs_path"])

    df_dict_to_load = {"order_proofs": df}

    return df_dict_to_load


if __name__ == "__main__":
    bucket_name = "desafio-eng-dados"
    prefix = "evidencias_atendimentos/"  # Subdirectory in the bucket
    df = get_order_proof_data(bucket_name, prefix=prefix)
    print(df.head())  # Display the first few rows of the DataFrame
