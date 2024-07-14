from datetime import datetime

from airflow.decorators import dag
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator

from commons import (
    BUCKET,
    PROJECT_ID,
    BUCKET
)
from data_ingestion.functions import (
    download_data_and_upload_to_gcs
)

# Change dates before running
DOWNLOAD_START_DATE = datetime(2019, 1, 1)
DOWNLOAD_END_DATE = datetime(2019, 2, 1)
# Change variables before running
BIGQUERY_DATASET = 'trips_data'
TABLE_ID = 'taxi_data_external'
FILE_FORMAT = 'PARQUET'

@dag(
    dag_id='dag__data_ingestion',
    description="""
        This pipeline downloads Chicago Taxi data within specified period, 
        uploads it to a bucket and creates en external table""",
    schedule_interval='@once',
    start_date=datetime(2024, 1, 1)
)
def task_flow():
    download_data_and_upload_to_gcs_task = download_data_and_upload_to_gcs(
        start_date=DOWNLOAD_START_DATE, end_date=DOWNLOAD_END_DATE, bucket_name=BUCKET
    )
    
    create_external_table_task = BigQueryCreateExternalTableOperator(
        task_id='bigquery_external_table_inner',
        table_resource={
            'tableReference': {
                'projectId': PROJECT_ID,
                'datasetId': BIGQUERY_DATASET,
                'tableId': TABLE_ID
            },
            'externalDataConfiguration': {
                'sourceFormat': FILE_FORMAT,
                'sourceUris': download_data_and_upload_to_gcs_task,
                'hivePartitioningOptions': {
                    'mode': 'AUTO',
                    'sourceUriPrefix': f'gs://{BUCKET}',
                    'requirePartitionFilter': False
                }
            }
        }
    )

    download_data_and_upload_to_gcs_task >> create_external_table_task

dag = task_flow()