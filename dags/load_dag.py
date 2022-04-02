# Required library
from airflow import DAG
from airflow.decorators import task
from airflow.utils.dates import days_ago
import pandas as pd
import sqlite3
import os
import json
from transform import customer, costed_event, charge, product_instance, product, payments
DATA_PATH = './airflow_jp_telecom/data/raw'
LOAD_PATH = './airflow_jp_telecom/data/load'
default_args = {
    'owner': 'admin',
    'start_date': days_ago(1),
}


@task
def extract(filename, sheet_name=None, index_col=None, table_name=None):
    file_path = f'{DATA_PATH}/{filename}'
    if 'csv' in filename:
        df = pd.read_csv("AAAAAAAAAAAAAAAAAAAAAAASSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSS",file_path)
        print("TEST")
    elif 'sqlite' in filename or 'db' in filename:
        conn = sqlite3.connect(file_path)
        df = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)
        conn.close()

    df = df.to_json(orient="records")
    df = json.loads(df)
    return df


@task()
def load_data(data :dict):
    conn = sqlite3.connect(f'{LOAD_PATH}/data-warehouse.sqlite', timeout=20)
    for key, df in data.items():
        df = pd.json_normalize(df)
        df.to_sql(key, conn, if_exists="replace", index=False)
        conn.close()


with DAG('load_dag', schedule_interval='@once',
         default_args=default_args,
         catchup=False) as dag:
    charge_data = extract('Customer.csv')
    transformed_data_charge = charge.transform(charge_data)
    charge_load = load_data(transformed_data_charge)


def get_database_tables(filename):
    conn = sqlite3.connect(f'{DATA_PATH}/{filename}')
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;")
    fetch_tables = cursor.fetchall()
    tables = [table[0] for table in fetch_tables]
    conn.close()
    return tables
charge_data >> transformed_data_charge >> charge_load
