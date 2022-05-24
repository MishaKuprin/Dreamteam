from airflow import DAG
import pandas as pd
from airflow.hooks.base import BaseHook
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from os import getenv
from sqlalchemy import create_engine

from datetime import datetime

DAG_DEFAULT_ARGS = {'start_date': datetime(2020, 1, 1), 'depends_on_past': False}
DEFAULT_POSTGRES_CONN_ID = "postgres_default"
AIRFLOW_HOME = "./airflow_jp_telecom"

DATA_PATH = './data/raw'
EXTRACT_PATH = './data/extract'

DAG_ID = "extractor"
schedule = "@once"


def extract_sql(table_name: str, schema: str = "raw", conn_id: str = None) -> None:
    conn_object = BaseHook.get_connection(conn_id or DEFAULT_POSTGRES_CONN_ID)  # conn_id or DEFAULT_POSTGRES_CONN_ID
    jdbc_url = f"postgresql://{conn_object.login}:{conn_object.password}@" \
               f"{conn_object.host}/{conn_object.schema}"
    engine = create_engine(jdbc_url)
    df = pd.read_sql(f"""
                        select * from raw.{table_name}
                        """,
                     engine)
    df.to_csv(f"{EXTRACT_PATH}/{table_name}.csv", index=False)


with DAG(dag_id=DAG_ID,
         description='Dag extract data from postgres to csv [version 1.0]',
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
    # datamart_table = "customer_totals"

    load_customer_raw_task = PythonOperator(dag=dag,
                                            task_id=f"{DAG_ID}.RAW.{customer_table_name}",
                                            python_callable=extract_sql,
                                            op_kwargs={
                                                "file_path": f"{EXTRACT_PATH}/Customer.csv",
                                                "table_name": customer_table_name,
                                                "conn_id": "raw_postgres"
                                            }
                                            )

    # load_payments_raw_task = PythonOperator(dag=dag,
    #                                         task_id=f"{DAG_ID}.RAW.{payments_table_name}",
    #                                         python_callable=extract_sql,
    #                                         op_kwargs={
    #                                             "file_path": f"{EXTRACT_PATH}/payment0.csv",
    #                                             "table_name": payments_table_name,
    #                                             "conn_id": "raw_postgres"
    #                                         }
    #                                         )
    # load_charge_raw_task = PythonOperator(dag=dag,
    #                                         task_id=f"{DAG_ID}.RAW.{charges_table_name}",
    #                                         python_callable=extract_sql,
    #                                         op_kwargs={
    #                                              "table_name": charges_table_name,
    #                                             "conn_id": "raw_postgres"
    #                                         }
    #                                         )
    # load_costed_events_raw_task = PythonOperator(dag=dag,
    #                                       task_id=f"{DAG_ID}.RAW.{costed_events_table_name}",
    #                                       python_callable=extract_sql,
    #                                       op_kwargs={
    #                                           "table_name": costed_events_table_name,
    #                                           "conn_id": "raw_postgres"
    #                                       }
    #                                       )
    # load_product_raw_task = PythonOperator(dag=dag,
    #                                              task_id=f"{DAG_ID}.RAW.{products_table_name}",
    #                                              python_callable=extract_sql,
    #                                              op_kwargs={
    #                                                  "table_name": products_table_name,
    #                                                  "conn_id": "raw_postgres"
    #                                              }
    #                                              )
    # load_product_instance_raw_task = PythonOperator(dag=dag,
    #                                        task_id=f"{DAG_ID}.RAW.{product_instances_table_name}",
    #                                        python_callable=extract_sql,
    #                                        op_kwargs={
    #                                            "table_name": product_instances_table_name,
    #                                            "conn_id": "raw_postgres"
    #                                        }
    #                                        )

    start_task >> load_customer_raw_task >> end_task

    # 1) DAG for raw layer, separated postgres
    # 2) DAG for datamart layer and dependency resolving
    # 3) Data from external source (e.g. google drive)
