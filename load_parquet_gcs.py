
import duckdb
import pandas as pd
from mage_ai.data_preparation.shared.secrets import get_secret_value

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data(*args, **kwargs):   

    bucket = kwargs['bucket']
    watch_folder = kwargs['watch_folder']
    ingest_file = kwargs['ingest_file']

    duckdb.sql("INSTALL httpfs;")
    duckdb.sql("LOAD httpfs;")
    duckdb.sql("SET s3_endpoint='storage.googleapis.com';")
    duckdb.sql(f"SET s3_access_key_id='{get_secret_value('DUCKDBACCESS')}';")
    duckdb.sql(f"SET s3_secret_access_key='{get_secret_value('DUCKDBSECRET')}';")

        # dataframe = duckdb.sql("SELECT * FROM read_parquet('s3://mage-vasco-bucket/banklist_test.parquet');")
    df = duckdb.sql(f"SELECT * FROM read_parquet('s3://{bucket}/{watch_folder}/{ingest_file}');").df()
        
    return df
