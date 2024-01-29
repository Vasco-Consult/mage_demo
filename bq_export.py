from mage_ai.settings.repo import get_repo_path
from mage_ai.io.bigquery import BigQuery
from mage_ai.io.config import ConfigFileLoader
from pandas import DataFrame
from os import path

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def export_data_to_big_query(df: DataFrame, **kwargs) -> None:

    project_id = kwargs['project_id']
    dataset = kwargs['dataset']
    ingest_file = kwargs['ingest_file']
    table_name = ingest_file.replace('.parquet','')

    table_id = f'{project_id}.{dataset}.{table_name}'

    print(table_id)
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'

    print(config_path)
    BigQuery.with_config(ConfigFileLoader(config_path, config_profile)).export(
        df,
        table_id,
        if_exists='replace',  
    )