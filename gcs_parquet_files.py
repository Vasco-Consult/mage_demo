from mage_ai.data_preparation.variable_manager import set_global_variable
from os import path
from pandas import DataFrame
from google.cloud import storage
from google.oauth2 import service_account
import json


if 'sensor' not in globals():
    from mage_ai.data_preparation.decorators import sensor

## Read oldest updated parquet file from bucket -> First in first out 

with open('Path to key.json') as source:
    info = json.load(source)

credentials = service_account.Credentials.from_service_account_info(info)

@sensor
def check_condition(*args, **kwargs) -> bool:
    bucket = kwargs['bucket']
    watch_folder = kwargs['watch_folder']
    storage_client = storage.Client(credentials=credentials)
    #Get all blobs in bucket
    blobs = storage_client.list_blobs(bucket,prefix=f"{watch_folder}/", delimiter='/')

    blob_list = []

    #Get blob details , name and updated date
    for blob in blobs:
        blob_name = blob.name.split('/')[-1]
        blob_updated = blob.updated
    
        if '.parquet' in blob_name:
            blob_list.append({'name':blob_name, 'date': blob_updated})

    # Check if we have files for ingestion 
    if blob_list:
        first_in = sorted(blob_list, key=lambda x: x['date'])[0]
        set_global_variable("mage_demo_dev", "ingest_file", first_in['name'])
        print(f'File {first_in["name"]} to be imported')

    return True if first_in else False
