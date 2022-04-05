from airflow import DAG
import pandas as pd
from airflow.hooks.base import BaseHook
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from os import getenv
from sqlalchemy import create_engine
from transform.first_datamart import data_transform as d_tr
from datetime import datetime
from airflow.decorators import task
import json
DAG_DEFAULT_ARGS = {'start_date': datetime(2020, 1, 1), 'depends_on_past': False}
DEFAULT_POSTGRES_CONN_ID = "postgres_default"
AIRFLOW_HOME = "./airflow_jp_telecom"

DATA_PATH = './data/raw'
EXTRACT_PATH = './data/extract'
LOAD_PATH = './data/load'

DAG_ID = "transformer"
schedule = "@once"

@task()
def first_datamart_transform(customer, product_instance, product, payment):
    costed_event = pd.read_csv(f"{DATA_PATH}/costed_event0.csv", index_col=False)
    res = d_tr.transform_p(customer, product_instance, product, payment, costed_event)
    res.to_csv(f"{LOAD_PATH}/first_datamart.csv", index=False)

@task()
def load_csv_from_extract_folder(filename, path):
    df = pd.read_csv(f"{path}/{filename}.csv",index_col=False)
    print("PATHASSSSSS",f"{path}/{filename}.csv" )
    df = df.to_json(orient="records")
    df = json.loads(df)
    return df

@task
def extract_sql(table_name: str, schema: str = "raw", conn_id: str = None):
    conn_object = BaseHook.get_connection(conn_id or DEFAULT_POSTGRES_CONN_ID)  # conn_id or DEFAULT_POSTGRES_CONN_ID
    jdbc_url = f"postgresql://{conn_object.login}:{conn_object.password}@" \
               f"{conn_object.host}/{conn_object.schema}"
    engine = create_engine(jdbc_url)
    df = pd.read_sql(f"""
                        select * from raw.{table_name}
                        """,
                     engine)
    return df
def load_csv_pandas(file_path: str, table_name: str, schema: str = "raw", conn_id: str = None) -> None:
    conn_object = BaseHook.get_connection(conn_id or DEFAULT_POSTGRES_CONN_ID) # conn_id or
    # extra = conn_object.extra_dejson
    jdbc_url = f"postgresql://{conn_object.login}:{conn_object.password}@" \
               f"{conn_object.host}/{conn_object.schema}"
    df = pd.read_csv(file_path)
    engine = create_engine(jdbc_url)
    df.to_sql(table_name, engine, schema=schema, if_exists="replace")





with DAG(dag_id=DAG_ID,
         description='Dag for transforming data to datamart and load[version 1.0]',
         schedule_interval=schedule,
         default_args=DAG_DEFAULT_ARGS,
         is_paused_upon_creation=True,
         max_active_runs=1,
         catchup=False
         ) as dag:
    start_task = DummyOperator(task_id='START', dag=dag)
    end_task = DummyOperator(task_id='END', dag=dag)

    customer_table_name = "customer"
    payments_table_name = "payment"
    charges_table_name = "charge"
    costed_events_table_name = "costed_event"
    products_table_name = "product"
    product_instances_table_name = "product_instance"

    customer_table = extract_sql(table_name=customer_table_name, conn_id="raw_postgres")
    payments_table = extract_sql(table_name=payments_table_name, conn_id="raw_postgres")
    #costed_events = extract_sql(table_name=costed_events_table_name, conn_id="raw_postgres")
    products_table = extract_sql(table_name=products_table_name, conn_id="raw_postgres")
    product_instances = extract_sql(table_name=product_instances_table_name, conn_id="raw_postgres")

    fdm = first_datamart_transform(customer_table, product_instances, products_table, payments_table)


    start_task >> [customer_table , payments_table , products_table , product_instances] >> fdm >> end_task

    # 1) DAG for raw layer, separated postgres
    # 2) DAG for datamart layer and dependency resolving
    # 3) Data from external source (e.g. google drive)