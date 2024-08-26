import os
from google.cloud import storage
from google.oauth2 import service_account

json_key_path = os.path.expanduser("~/bigquery_gcp_key.json")


def get_storage_client():
    credentials = service_account.Credentials.from_service_account_file(json_key_path)
    return storage.Client(credentials=credentials, project=credentials.project_id)
