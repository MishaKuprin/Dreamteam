from google.oauth2 import service_account
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload
from googleapiclient.discovery import build
import pprint
import io
from airflow import DAG
from datetime import datetime
import pandas as pd
from airflow.hooks.base import BaseHook
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.decorators import task

DAG_DEFAULT_ARGS = {'start_date': datetime(2020, 1, 1), 'depends_on_past': False}
DEFAULT_POSTGRES_CONN_ID = "postgres_default"
AIRFLOW_HOME = "./airflow"

DATA_PATH = './data/raw'
LOAD_PATH = './data/load'

DAG_ID = "gdriveload"
schedule = "@once"


@task()
def google_drive_load():
    SCOPES = ['https://www.googleapis.com/auth/drive']
    SERVICE_ACCOUNT_FILE = './bigdatagroup2-ea06eda5a64a.json'
    credentials = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
    service = build('drive', 'v3', credentials=credentials)

    results = service.files().list(pageSize=100,
                                   fields="nextPageToken, files(id, name)",
                                   q="'1-8SSL_9Q6xwKPVxzVU7II57vripv_gbe' in parents").execute()

    file_ids = []
    file_names = []
    for i in range(len(results['files'])):
        file_ids.append(results['files'][i]['id'])
        file_names.append(results['files'][i]['name'])

    for i in range(len(file_ids)):
        request = service.files().get_media(fileId=file_ids[i])
        filename = '{0}/{1}'.format(DATA_PATH, file_names[i])
        fh = io.FileIO(filename, 'wb')
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while done is False:
            status, done = downloader.next_chunk()


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

    google_drive_to_raw_task = google_drive_load()

    start_task >> [google_drive_to_raw_task] >> end_task
