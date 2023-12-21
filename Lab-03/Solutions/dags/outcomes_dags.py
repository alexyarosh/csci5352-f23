from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from datetime import datetime
import os
from etl_scripts.transform import transform_data
from etl_scripts.load import load_data, load_fact_data

source_url = 'https://data.austintexas.gov/resource/9t4d-g238.csv' #used API
AIRFLOW_HOME= os.environ.get('AIRFLOW_HOME', '/opt/airflow')
CSV_TARGET_DIR= AIRFLOW_HOME + '/data/{{ ds }}/downloads'
CSV_TARGET_FILE= CSV_TARGET_DIR+'/outcomes_{{ ds }}.csv'

PQ_TARGET_DIR = AIRFLOW_HOME + '/data/{{ ds }}/processed'

with DAG(
    dag_id="outcomes_dags",
    start_date= datetime(2023,11,15),
    schedule_interval='@daily'
)as dag:

    extract = BashOperator(
        task_id="extract",
        bash_command=f"curl --create-dirs -o {CSV_TARGET_FILE} {source_url}",
    )

    transform = PythonOperator(
        task_id="transform",
        python_callable=transform_data,
        op_kwargs = {
            'source_csv': CSV_TARGET_FILE, 
            'target_dir': PQ_TARGET_DIR
        }
    )

    load_animals_dim = PythonOperator(
        task_id="load_animals_dim",
        python_callable=load_data,
        op_kwargs = {
            'table_file': PQ_TARGET_DIR+'/dim_animals.parquet', 
            'table_name': 'dim_animals',
            'key': 'animal_id'
        }
    )


    load_dates_dim = PythonOperator(
        task_id="load_dates_dim",
        python_callable=load_data,
        op_kwargs = {
            'table_file': PQ_TARGET_DIR+'/dim_dates.parquet', 
            'table_name': 'dim_dates',
            'key': 'date_id'
        }
    )
    
    load_outcome_types_dim = PythonOperator(
        task_id="load_outcome_types_dim",
        python_callable=load_data,
        op_kwargs = {
            'table_file': PQ_TARGET_DIR+'/dim_outcome_types.parquet', 
            'table_name': 'dim_outcome_types',
            'key': 'outcome_type_id'
        }
    )

    load_outcome_fct = PythonOperator(
        task_id="load_outcome_fct", 
        python_callable=load_fact_data,
        op_kwargs={
            'table_file': PQ_TARGET_DIR + '/fct_outcomes.parquet',
            'table_name': 'fct_outcomes',
            'DB_URL': 'postgresql+psycopg2://sudha:hunter2@db:5432/shelter'
        }
    )

    extract >> transform >> [load_animals_dim, load_dates_dim, load_outcome_types_dim] >> load_outcome_fct

