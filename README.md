# Chicago Taxi data ingestion project

This project builds a data pipeline for loading the Chicago taxi trips [dataset](https://data.cityofchicago.org/Transportation/Taxi-Trips/wrvz-psew/data) into BigQuery for subsequent analysis.

Project Steps:
1. Terraform: Create a bucket in GCP and a dataset in BigQuery.
2. AirFlow: Pipeline for loading data into the bucket and subsequently creating an external table in BigQuery.
3. dbt: Create models for use in subsequent analysis.


## Terraform [docs](terraform/README.md)

#### Before Running Terraform
You need to:
 - configure a service account in GCP
 - install Google Cloud SDK
 - authenticate in GCP
 - install Terraform

To create the infrastructure, run the following script:

```shell
bash run_terraform.sh
```
Check what it is going to do and press `yes`.

To destroy the created infrastructure run:

```shell
bash destroy_terraform.sh
```


## Airflow [docs](airflow/README.md)

#### Before Running the Docker Container with Airflow
You need to place the Google credentials in the `~/.google/credentials/` directory on your machine (either local or VM).

```shell
cd ~ && mkdir -p ~/.google/credentials/
mv <path/to/your/service-account-authkey>.json ~/.google/credentials/google_credentials.json
```

Before running the container, remember to update:
- the `GCP_PROJECT_ID` and `GCP_GCS_BUCKET` variable values in the `.env` file
- the `DOWNLOAD_START_DATE`, `DOWNLOAD_END_DATE`, `BIGQUERY_DATASET`, `TABLE_ID` variable values in [`dag__data_ingestion.py`](airflow/dags/data_ingestion/dag__data_ingestion.py)

Execution:
1. Run the following command to build an image, initialize Airflow, and kick up all services:

```shell
bash run_airflow.sh
```

2. Login to Airflow web UI on `localhost:8080` with default credentials `admin/admin` and run DAG named `dag__data_ingestion`

3. To shutdown all Airflow services run:
```shell
bash shutdown_airflow.sh
```



## dbt [docs](dbt/README.md)

Before running models please install `dbt-core` or set up `dbt cloud`. For more details, refer to the [official documentation](https://docs.getdbt.com/docs/get-started-dbt)

Commands to run dbt models:
```
dbt seed
dbt build
```

Models overview:

![Models overview](/pictures/dbt_overview.jpg)