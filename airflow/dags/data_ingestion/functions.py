from io import BytesIO
from datetime import datetime
import logging
from pathlib import Path
import requests as rq
from tempfile import TemporaryDirectory

from airflow.decorators import task
# from dateutil.relativedelta import relativedelta
from dateutil.rrule import rrule, MONTHLY
from google.cloud.storage import Client, transfer_manager
import pyarrow.csv
import pyarrow.parquet


FILENAME = 'chicago_taxi_data_{year}_{month}.parquet'

def download_file(start_date: str, end_date: str) -> BytesIO:
    """
    Requests data for a specified period of time and saves it to memory
    
    start_date: beginning of the period for which the data was downloaded
    end_date: end of the period for which the data was downloaded
    """
    # Query is limited by 3M rows
    # Should be sufficient for any year-month
    limit = 3_000_000
    url = ("https://data.cityofchicago.org/resource/wrvz-psew.csv"
           f"?%24where=trip_start_timestamp>='{start_date}'"
           f"%20AND%20trip_start_timestamp<'{end_date}'&%24limit={limit}")
    
    logging.info(f'Downloading data from {start_date} till {end_date}: {url}')
    response = rq.get(url, timeout=None)
    
    if response:
        logging.info('Download is finished')
        return BytesIO(response.content)


def save_file_as_parquet(csv_io: BytesIO, path: str) -> None:
    """
    Saves a csv located in memory as parquet

    csv_io: csv file in bytes format
    path: path where to save the file in parquet format
    """
    logging.info(f'Saving csv file as parquet to {path}')
    table = pyarrow.csv.read_csv(csv_io)
    pyarrow.parquet.write_table(table, path)
    logging.info(f'Saving finished')


def upload_folder_to_gcs(
        bucket_name: str, source_directory: Path) -> list[str]:
    """
    Uploads all files located in the specified directory to a GCP bucket
    preserving the same file paths

    bucket_name: name of your bucket in GCP
    source_directory: Path to a folder with parquet files
    """

    storage_client = Client()
    bucket = storage_client.bucket(bucket_name)

    # Get paths of all parquet files relative to `source directory`
    files_paths_str = [str(path.relative_to(source_directory)) for path 
                       in source_directory.rglob('*/*/chicago_taxi_data_*.parquet')]

    results = transfer_manager.upload_many_from_filenames(
        bucket, files_paths_str, source_directory=source_directory, worker_type='thread')

    for name, result in zip(files_paths_str, results):
        if isinstance(result, Exception):
            logging.info(f'Failed to upload {name} due to exception: {result}')
        else:
            logging.info(f'Uploaded {name} to {bucket.name}.')

    # Create URIs for files in bucket to push them to next task
    bucket_paths = [f'gs://{bucket_name}/' + path for path in files_paths_str]

    return bucket_paths


@task
def download_data_and_upload_to_gcs(
    start_date: datetime, end_date: datetime, bucket_name: str) -> list[str]:
    """
    Task does the following:
     - downloads data for a specified period of time
     - saves it as a parquet file in a temporary folder
     - uploads all files to a gcp bucket

    start_date: beginning of the period for which the data was downloaded
    end_date: end of the period for which the data was downloaded
    """

    # Create temporary folder
    with TemporaryDirectory() as temp_dir:
        temp_dir_path = Path(temp_dir)
        logging.info(f'Temporary folder: {temp_dir_path}')

        dates = [date.date() 
                 for date 
                 in rrule(freq=MONTHLY, dtstart=start_date, until=end_date)]
        start_dates = dates[:-1]
        end_dates = dates[1:]
        
        for start, end in zip(start_dates, end_dates):
            # Download file
            csv_io = download_file(start.strftime('%Y-%m-%d'), end.strftime('%Y-%m-%d'))

            # Create path for a file in the format: temp_folder/year/month
            year = start.year
            month = start.month

            temp_file_path = (temp_dir_path / f'year={year}' / f'month={month}' 
                              / FILENAME.format(year=year, month=month))
            # Create all parent directories
            temp_file_path.parent.mkdir(parents=True, exist_ok=True) 
            
            # Save as parquet
            save_file_as_parquet(csv_io, str(temp_file_path))

        # Upload whole folder to the bucket in gcp
        bucket_paths = upload_folder_to_gcs(bucket_name, temp_dir_path)

    return bucket_paths