from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator

from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator

from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator


from datetime import datetime
import os

from etl_scripts.transform import transform_data
from etl_scripts.load import load_data, load_fact_data

SOURCE_URL = 'https://data.austintexas.gov/api/views/9t4d-g238/rows.csv'
AIRFLOW_HOME = os.environ.get('AIRFLOW_HOME', '/opt/airflow')
CSV_TARGET_DIR = AIRFLOW_HOME + '/data/{{ ds }}/downloads'
CSV_TARGET_FILE = CSV_TARGET_DIR+'/outcomes_{{ ds }}.csv'

GCS_BUCKET = os.environ['GCS_BUCKET']
GCS_RAW_PATH = 'data/{{ ds }}/raw/'
GCS_RAW_FILE = GCS_RAW_PATH+'outcomes_{{ ds }}.csv'

GCP_PROJECT_ID = os.environ['GCP_PROJECT_ID']

PQ_TARGET_DIR = AIRFLOW_HOME + '/data/{{ ds }}/processed'
GCS_PQ_PATH = 'data/{{ ds }}/processed'


with DAG(
    dag_id="outcomes_dag",
    start_date = datetime(2023,12,4),
    schedule_interval='@daily'
) as dag:
    
    extract = BashOperator(
        task_id="extract",
        bash_command=f"curl --create-dirs -o {CSV_TARGET_FILE} {SOURCE_URL}",
    )

    upload_raw_to_gsc = LocalFilesystemToGCSOperator(
        task_id="upload_raw_to_gsc",
        src=CSV_TARGET_FILE,
        dst=GCS_RAW_FILE,
        bucket=GCS_BUCKET,
    )    

    transform = PythonOperator(
        task_id="transform",
        python_callable=transform_data,
        op_kwargs = {
            'source_csv': f"gs://{GCS_BUCKET}/" + GCS_RAW_FILE,
            'target_dir': PQ_TARGET_DIR
        }
    )

    extract >> upload_raw_to_gsc >> transform

    # upload_animals_pq_to_gsc = LocalFilesystemToGCSOperator(
    #     task_id="upload_animals_pq_to_gsc",
    #     src=PQ_TARGET_DIR+'/dim_animals.parquet',
    #     dst=GCS_PQ_PATH + '/dim_animals.parquet',
    #     bucket=GCS_BUCKET,
    # )   

    # upload_dates_pq_to_gsc = LocalFilesystemToGCSOperator(
    #     task_id="upload_dates_pq_to_gsc",
    #     src=PQ_TARGET_DIR+'/dim_dates.parquet',
    #     dst=GCS_PQ_PATH + '/dim_dates.parquet',
    #     bucket=GCS_BUCKET,
    # )   

    # upload_outcome_types_pq_to_gsc = LocalFilesystemToGCSOperator(
    #     task_id="upload_outcome_types_pq_to_gsc",
    #     src=PQ_TARGET_DIR+'/dim_outcome_types.parquet',
    #     dst=GCS_PQ_PATH + '/dim_outcome_types.parquet',
    #     bucket=GCS_BUCKET,
    # )   

    # upload_outcomes_pq_to_gsc = LocalFilesystemToGCSOperator(
    #     task_id="upload_outcomes_pq_to_gsc",
    #     src=PQ_TARGET_DIR+'/fct_outcomes.parquet',
    #     dst=GCS_PQ_PATH + '/fct_outcomes.parquet',
    #     bucket=GCS_BUCKET,
    # )     


    gcp_upload_complete = DummyOperator(
        task_id = 'gcp_upload_complete'
    )    

    for table in ["dim_animals", "dim_dates", "dim_outcome_types", "fct_outcomes"]:
        upload_parquet_to_gsc = LocalFilesystemToGCSOperator(
            task_id=f"upload_{table}_pq_to_gsc",
            src=PQ_TARGET_DIR+f'/{table}.parquet',
            dst=GCS_PQ_PATH + f'/{table}.parquet',
            bucket=GCS_BUCKET,
        )

        transform >> upload_parquet_to_gsc >> gcp_upload_complete



    for table in ["dim_animals", "dim_dates", "dim_outcome_types", "fct_outcomes"]:
        create_table = GCSToBigQueryOperator(
            task_id=f"create_bq_{table}",
            destination_project_dataset_table=f"{GCP_PROJECT_ID}.shelter.{table}",
            bucket=GCS_BUCKET,
            source_objects=[f"{GCS_PQ_PATH}/{table}.parquet"],
            source_format='PARQUET',
            autodetect=True,
            write_disposition="WRITE_TRUNCATE"
        )

        gcp_upload_complete >> create_table
        

   