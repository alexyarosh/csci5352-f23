#!/usr/bin/env python
# coding: utf-8


import pandas as pd
import argparse 
from sqlalchemy import create_engine
import os

from transform import transform_data

def extract_data(source):
    return pd.read_csv(source)


def load_data(data_dict):
    # DB connection string specified in docker-compose
    db_url = os.environ['DB_URL']
    conn = create_engine(db_url)

    # load all the data in a loop
    for table_name in data_dict:
        data_dict[table_name].to_sql(table_name, conn, if_exists="append", index=False)
        print(table_name+" loaded")


if __name__ == "__main__": 
    parser = argparse.ArgumentParser()
    parser.add_argument('source', help='source csv')
    args = parser.parse_args()

    print("Starting...")
    df = extract_data(args.source)
    df_dict = transform_data(df)
    load_data(df_dict)
    print("Complete")