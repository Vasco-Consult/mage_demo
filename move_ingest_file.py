from pandas import DataFrame
from os import path
import json
from google.cloud import storage
from google.oauth2 import service_account
if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


# Transfer file from bucket/Read to bucket/Out

with open('Path to key.json') as source:
    info = json.load(source)

credentials = service_account.Credentials.from_service_account_info(info)

@data_exporter
def export_data_to_google_cloud_storage(df: DataFrame, **kwargs) -> None:
    
    # Import variables
    bucket_name = kwargs['bucket']
    watch_folder = kwargs['watch_folder']
    destination_folder = kwargs['destination_folder']
    # blob_name = f"{watch_folder}/{kwargs['ingest_file']}"
    blob_name = kwargs['ingest_file']

    # Create output file destination
    blob_path = f'{watch_folder}/{blob_name}'
    file_destination = f"{destination_folder}/{blob_name}"


    # Get storage bucket and blob refs
    storage_client = storage.Client(credentials=credentials)

    bucket = storage_client.bucket(bucket_name)
    source_blob = bucket.blob(blob_path)
    

    # Copy blob to destination and delete initial blob
    bucket.copy_blob(source_blob, bucket, file_destination)    
    bucket.delete_blob(blob_path)

    return print(
        "Blob {} in bucket {} moved to blob {} in bucket {}.".format(
            source_blob.name,
            bucket.name,
            file_destination,
            bucket.name,
        )
    )