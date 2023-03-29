import os
from schema import schema
from google.cloud import bigquery
from prefect import flow, task

os.environ['GOOGLE_APPLICATION_CREDENTIALS']='.google/airlinepipeline.json'

client = bigquery.Client()


@flow()
def create_table(project: str, dataset: str, table_id: str) -> None:
    ''' Create an external table in bigquery from google cloud buckets'''


    dataset_ref = bigquery.DatasetReference(project, dataset)

    table = bigquery.Table(dataset_ref.table(table_id), schema=schema)
    external_config = bigquery.ExternalConfig("CSV")
    external_config.source_uris = [f"gs://airline-buckets/data/*.csv.gz"]
    external_config.options.skip_leading_rows = 1
    table.external_data_configuration = external_config
    
    table = client.create_table(table)


if __name__ == '__main__':
    project = 'airlinepipeline'      
    dataset = 'airline_on_time'
    table_id = 'airline_trips'
    create_table(project, dataset, table_id)