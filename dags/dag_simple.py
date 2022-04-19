from airflow import DAG
import pandas as pd
from airflow.hooks.base import BaseHook
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from os import getenv
from sqlalchemy import create_engine
from airflow.models import Variable
from datetime import datetime
from airflow.decorators import task
import os
DAG_DEFAULT_ARGS = {'start_date': datetime(2020, 1, 1), 'depends_on_past': False, 'provide_context': True}
DEFAULT_POSTGRES_CONN_ID = "postgres_default"
AIRFLOW_HOME = "./airflow_jp_telecom"

DATA_PATH = './data/raw'
LOAD_PATH = './data/load'

DAG_ID = "loader"
schedule = "@once"


def check_file_names():
    existing_files_in_data_path = os.listdir(DATA_PATH)
    return existing_files_in_data_path

def file_name_starts_with(file_names: list, start_with: str):
    return [file_names[i] for i in range(len(file_names)) if file_names[i].endswith(start_with)]

def load_csv_pandas(file_path: str, table_name: str, schema: str = "raw", conn_id: str = None) -> None:
    conn_object = BaseHook.get_connection(conn_id or DEFAULT_POSTGRES_CONN_ID) # conn_id or
    # extra = conn_object.extra_dejson
    jdbc_url = f"postgresql://{conn_object.login}:{conn_object.password}@" \
               f"{conn_object.host}/{conn_object.schema}"
    df = pd.read_csv(file_path)
    engine = create_engine(jdbc_url)
    df.to_sql(table_name, engine, schema=schema, if_exists="replace")


def exec_date_set(**context):
    Variable.set("psql_loader_exec_date",context['execution_date'].format("YYYY-MM-DD"))

with DAG(dag_id=DAG_ID,
         description='Dag to transfer data from csv to postgres [version 1.0]',
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

    exec_dt_set= PythonOperator(dag=dag, task_id=f"{DAG_ID}.exec_date",
                                            python_callable=exec_date_set,
                                            provide_context=True,
                                            )

    load_customer_raw_task = PythonOperator(dag=dag,
                                            task_id=f"{DAG_ID}.RAW.{customer_table_name}",
                                            python_callable=load_csv_pandas,
                                            op_kwargs={
                                                "file_path": f"{DATA_PATH}/customer.csv",
                                                "table_name": customer_table_name,
                                                "conn_id": "raw_postgres"
                                            }
                                            )

    load_payments_raw_task = PythonOperator(dag=dag,
                                            task_id=f"{DAG_ID}.RAW.{payments_table_name}",
                                            python_callable=load_csv_pandas,
                                            op_kwargs={
                                                "file_path": f"{DATA_PATH}/payment.csv",
                                                "table_name": payments_table_name,
                                                "conn_id": "raw_postgres"
                                            }
                                            )
    load_charge_raw_task = PythonOperator(dag=dag,
                                            task_id=f"{DAG_ID}.RAW.{charges_table_name}",
                                            python_callable=load_csv_pandas,
                                            op_kwargs={
                                                "file_path": f"{DATA_PATH}/charge.csv",
                                                "table_name": charges_table_name,
                                                "conn_id": "raw_postgres"
                                            }
                                            )
    load_costed_events_raw_task = PythonOperator(dag=dag,
                                          task_id=f"{DAG_ID}.RAW.{costed_events_table_name}",
                                          python_callable=load_csv_pandas,
                                          op_kwargs={
                                              "file_path": f"{DATA_PATH}/costed_event.csv",
                                              "table_name": costed_events_table_name,
                                              "conn_id": "raw_postgres"
                                          }
                                          )
    load_product_raw_task = PythonOperator(dag=dag,
                                                 task_id=f"{DAG_ID}.RAW.{products_table_name}",
                                                 python_callable=load_csv_pandas,
                                                 op_kwargs={
                                                     "file_path": f"{DATA_PATH}/product.csv",
                                                     "table_name": products_table_name,
                                                     "conn_id": "raw_postgres"
                                                 }
                                                 )
    load_product_instance_raw_task = PythonOperator(dag=dag,
                                           task_id=f"{DAG_ID}.RAW.{product_instances_table_name}",
                                           python_callable=load_csv_pandas,
                                           op_kwargs={
                                               "file_path": f"{DATA_PATH}/product_instance.csv",
                                               "table_name": product_instances_table_name,
                                               "conn_id": "raw_postgres"
                                           }
                                           )
    # customer_totals_datamart_task = PythonOperator(dag=dag,
    #                                                task_id=f"{DAG_ID}.DATAMART.{datamart_table}",
    #                                                python_callable=datamart_pandas,
    #                                                op_kwargs={
    #                                                    "table_name": datamart_table,
    #                                                    "conn_id": "datamart_postgres"
    #                                                }
    #                                                )
    start_task >> [load_customer_raw_task, load_payments_raw_task, load_charge_raw_task, load_costed_events_raw_task, load_product_raw_task, load_product_instance_raw_task] >> exec_dt_set >> end_task


