import pandas as pd
from sqlalchemy import create_engine
import os
from sqlalchemy.dialects.postgresql import insert
from google.cloud import storage
from io import BytesIO

bucket_name=""
service_account_key_path=""

def load_data(table_file, table_name, key):
    db_url = os.environ.get('DB_URL', 'postgresql+psycopg2://sudha:hunter2@db:5432/shelter')
    # Rest of your code
    conn = create_engine(db_url)

    def insert_on_conflict_nothing(table, conn, keys, data_iter):
    # "key" is the primary key in "conflict_table"
        data = [dict(zip(keys, row)) for row in data_iter]
        stmt = insert(table.table).values(data).on_conflict_do_nothing(index_elements=[key])
        result = conn.execute(stmt)
        return result.rowcount

    #pd.read_parquet(table_file).to_sql(table_name, conn, if_exists="append", index=False, method=insert_on_conflict_nothing)
    #print(table_name+" loaded")
    try:
        client = storage.Client.from_service_account_json(service_account_key_path)
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(table_file)
        file_content = BytesIO(blob.download_as_bytes())
        df = pd.read_parquet(file_content)
        
        df.to_sql(table_name, conn, if_exists="append", index=False, method=insert_on_conflict_nothing)
    except FileNotFoundError as e:
        print(f"Error: {e}")

def load_fact_data(table_file, table_name):
    # DB connection string specified in docker-compose
    db_url = os.environ.get('DB_URL', 'postgresql+psycopg2://sudha:hunter2@db:5432/shelter')

    conn = create_engine(db_url)


    client = storage.Client.from_service_account_json(service_account_key_path)
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(table_file)
    file_content = BytesIO(blob.download_as_bytes())
    
    pd.read_parquet(file_content).to_sql(table_name, conn, if_exists="replace", index=False)
    print(table_name+" loaded")