from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from os import getenv
from datetime import datetime

from core.utils.constants import ARGS_APP_NAME, ARGS_TABLE_NAME, ARGS_PATHS, ARGS_JDBC_URL, ARGS_QUERY, SPARK_CONF
from core.utils.macros import custom_macros_dict
from core.utils.spark_utils import build_spark_args

DAG_DEFAULT_ARGS = {'start_date': datetime(2020, 1, 1), 'depends_on_past': False}
AIRFLOW_HOME = getenv('AIRFLOW_HOME', '/opt/airflow')

postgres_driver_jar = "./spark/resources/postgresql-9.4.1207.jar"
DAG_ID = "SPARK"
schedule = "@hourly"
DATA_PATH = './data/raw'
LOAD_PATH = './data/load'

with DAG(dag_id=DAG_ID,
         description='Dag to transfer data from csv to postgres [version 1.0]',
         schedule_interval=schedule,
         default_args=DAG_DEFAULT_ARGS,
         is_paused_upon_creation=True,
         max_active_runs=1,
         catchup=False,
         render_template_as_native_obj=True,
         user_defined_macros=custom_macros_dict
         ) as dag:
    start_task = DummyOperator(task_id='START', dag=dag)
    end_task = DummyOperator(task_id='END', dag=dag)

    customer_table_name = "costed_event"
    payments_table_name = "payments"
    datamart_table = "customer_totals"

    customer_raw_task_id = f"{DAG_ID}.RAW.{customer_table_name}"
    load_costed_raw_task = SparkSubmitOperator(dag=dag,
                                                 task_id=customer_raw_task_id,
                                                 application=f"./dags/core/local_file_to_raw.py",
                                                 application_args=build_spark_args(
                                                     **{
                                                         ARGS_APP_NAME: customer_raw_task_id,
                                                         ARGS_TABLE_NAME: customer_table_name,
                                                         ARGS_PATHS: f"{DATA_PATH}/costed_event.csv",
                                                         ARGS_JDBC_URL: "{{ get_conn() }}"
                                                     }
                                                 ),
                                                 conf=SPARK_CONF,
                                                 jars=postgres_driver_jar)

    payments_raw_task_id = f"{DAG_ID}.RAW.{payments_table_name}"
    load_payments_raw_task = SparkSubmitOperator(dag=dag,
                                                 task_id=payments_raw_task_id,
                                                 application=f"./dags/core/local_file_to_raw.py",
                                                 application_args=build_spark_args(
                                                     **{
                                                         ARGS_APP_NAME: payments_raw_task_id,
                                                         ARGS_TABLE_NAME: payments_table_name,
                                                         ARGS_PATHS: f"{DATA_PATH}/payments.csv",
                                                         ARGS_JDBC_URL: "{{ get_conn() }}"
                                                     }
                                                 ),
                                                 conf=SPARK_CONF,
                                                 jars=postgres_driver_jar)



    start_task >> [load_costed_raw_task, load_payments_raw_task]  >> end_task
