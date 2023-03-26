from google.cloud import bigquery
from prefect import flow, task

import os

os.environ['GOOGLE_APPLICATION_CREDENTIALS']='/Users/x/OneDrive/Documents/Python/Jan_2023/DE_Zoomcamp/week_4_analytics_engineering/.google/credentials/google_credentials.json'

client = bigquery.Client()

@flow()
def create_table(color: str, year: int) -> None:
    ''' Create an external table in bigquery from google cloud buckets'''
    project = 'airlinepipeline'       #change project, dataset and table_id for use in other projects
    dataset = 'airline_on_time'
    table_id = f'external_{color}_tripdata_{year}'

    dataset_ref = bigquery.DatasetReference(project, dataset)

    table = bigquery.Table(dataset_ref.table(table_id), schema=globals()[color])
    external_config = bigquery.ExternalConfig("CSV")
    external_config.source_uris = [f"gs://dtc_data_lake_dtc-de-375918/{color}_tripdata_{year}-*.csv.gz"]
    external_config.options.skip_leading_rows = 1
    table.external_data_configuration = external_config

    table = client.create_table(table)

if __name__ == '__main__':
    color = 'green'
    year = 2019
    create_table(color, year)