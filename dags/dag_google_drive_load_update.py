from custom_hooks.CustomGoogleDriveHook import CustomGoogleDriveHook
from airflow import DAG
from datetime import datetime
from airflow.operators.dummy import DummyOperator
from airflow.decorators import task

DAG_DEFAULT_ARGS = {'start_date': datetime(2020, 1, 1), 'depends_on_past': False}
AIRFLOW_HOME = "./airflow"

DATA_PATH = './data/raw'
LOAD_PATH = './data/load'

DAG_ID = "gdrive_load_diff"
schedule = "@once"

@task()
def google_drive_load(conn_id: str, mode: str):
    hook = CustomGoogleDriveHook(gcp_conn_id=conn_id)
    hook.download_all_files(data_path=DATA_PATH,mode=mode)

with DAG(dag_id=DAG_ID,
         description='Dag to transfer data from google drive to raw [version 1.0]',
         schedule_interval=schedule,
         default_args=DAG_DEFAULT_ARGS,
         is_paused_upon_creation=True,
         max_active_runs=1,
         catchup=False
         ) as dag:
    start_task = DummyOperator(task_id='START', dag=dag)
    end_task = DummyOperator(task_id='END', dag=dag)

    google_drive_to_raw_task = google_drive_load(conn_id="gdrive_to_airflow",mode="diff")

    start_task >> [google_drive_to_raw_task] >> end_task
