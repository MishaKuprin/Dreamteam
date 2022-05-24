from airflow import DAG
import pandas as pd
from airflow.hooks.base import BaseHook
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from os import getenv
from sqlalchemy import create_engine
from airflow.models import Variable
import os
from datetime import datetime



DAG_DEFAULT_ARGS = {'start_date': datetime(2020, 1, 1), 'depends_on_past': False, 'provide_context': True}
DEFAULT_POSTGRES_CONN_ID = "postgres_default"
AIRFLOW_HOME = "./airflow"

DATA_PATH = './data/raw'
LOAD_PATH = './data/load'

DATA_PATH = './data/raw'
EXTRACT_PATH = './data/extract'

DAG_ID = "arpu"
schedule = "@once"

def exec_date_set(**context):
    Variable.set("psql_datamart2_exec_date",context['execution_date'].format("YYYY-MM-DD"))

def datamart_pandas(table_name: str, schema: str = "raw", conn_id: str = None) -> None:
     conn_object = BaseHook.get_connection(conn_id or DEFAULT_POSTGRES_CONN_ID)
     jdbc_url = f"postgresql://{conn_object.login}:{conn_object.password}@" \
                f"{conn_object.host}/{conn_object.schema}"
     engine = create_engine(jdbc_url)
     sql = open('./dags/sql2.txt')
     sql_read = sql.read()
     df = pd.read_sql(sql_read, engine)
     sql.close()
     df.to_sql(table_name, engine, schema=schema, if_exists="replace")
     df.to_csv(f"{EXTRACT_PATH}/{table_name}.csv", index=False)


with DAG(dag_id=DAG_ID,
         description='Dag to create datamart with SQL',
         schedule_interval=schedule,
         default_args=DAG_DEFAULT_ARGS,
         is_paused_upon_creation=True,
         max_active_runs=1,
         catchup=False
         ) as dag:
    start_task = DummyOperator(task_id='START', dag=dag)
    end_task = DummyOperator(task_id='END', dag=dag)

    exec_dt_set = PythonOperator(dag=dag, task_id=f"{DAG_ID}.exec_date",
                                 python_callable=exec_date_set,
                                 provide_context=True,
                                 )

    datamart_table = "arpu"



    customer_totals_datamart_task = PythonOperator(dag=dag,
                                             task_id=f"{DAG_ID}.RAW.{datamart_table}",
                                                    python_callable=datamart_pandas,
                                                    op_kwargs={
                                                        "table_name": datamart_table,
                                                        "conn_id": "raw_postgres"
                                                    }
                                                    )
    start_task >> customer_totals_datamart_task >> exec_dt_set >> end_task