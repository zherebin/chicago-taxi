# Using Airflow

Airflow is used to run a pipeline that fetches data from an external server, uploads parquet files to a GCP bucket, and creates an external table in BigQuery.

Before running, ensure that:
- variable values in `.env`
- data ingestion period in `dag__data_ingestion.py`

are set as needed.