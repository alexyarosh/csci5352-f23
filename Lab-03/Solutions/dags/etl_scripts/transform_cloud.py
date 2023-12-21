import pandas as pd
import numpy as np
from collections import OrderedDict
import logging
from pathlib import Path
from google.cloud import storage
import requests
from io import BytesIO
from etl_scripts.transform import prep_animal_dim, prep_data, prep_date_dim, prep_outcome_types_dim, prep_outcomes_fct 

bucket_name=""
service_account_key_path=""

def extract_data(source_url, csv_filename):
    response = requests.get(source_url)
    if response.status_code == 200:
        file_object = BytesIO(response.content)
    
    client = storage.Client.from_service_account_json(service_account_key_path)
    bucket = client.get_bucket(bucket_name)
    blob = bucket.blob(csv_filename)
    blob.upload_from_file(file_object)

def transform_data(source_csv, target_dir):
    new_data = pd.read_csv(source_csv)
    new_data = prep_data(new_data)

    dim_animal = prep_animal_dim(new_data)
    dim_dates = prep_date_dim(new_data)
    dim_outcome_types = prep_outcome_types_dim(new_data)
    fct_outcomes = prep_outcomes_fct(new_data)

    Path(target_dir).mkdir(parents=True, exist_ok=True)

    dim_animal.to_parquet(target_dir+'/dim_animal.parquet')
    dim_dates.to_parquet(target_dir+'/dim_dates.parquet')
    dim_outcome_types.to_parquet(target_dir+'/outcome_types.parquet')
    fct_outcomes.to_parquet(target_dir+'/fct_outcomes.parquet')

    #uploading files to cloud
    client = storage.Client.from_service_account_json(service_account_key_path)
    bucket = client.get_bucket(bucket_name)

    for file_prefix in ['dim_animal', 'dim_dates', 'outcome_types', 'fct_outcomes']:
        blob = bucket.blob(file_prefix+'.parquet')
        blob.upload_from_filename(target_dir+'/fct_outcomes.parquet')
