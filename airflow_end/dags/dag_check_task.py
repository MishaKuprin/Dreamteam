from airflow import DAG
from airflow.operators.dummy import DummyOperator
from datetime import datetime
from airflow.decorators import task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.models import Variable

DAG_DEFAULT_ARGS = {'start_date': datetime(2020, 1, 1), 'depends_on_past': False}

AIRFLOW_HOME = "./airflow_jp_telecom"

DATA_PATH = './data/raw'
LOAD_PATH = './data/load'

DAG_ID = "dag_loader_check"
schedule = "@hourly"




def checker():
    loader_exec_date = Variable.get("psql_loader_exec_date")
    datamart1_exec_date = Variable.get("psql_datamart1_exec_date")
    datamart2_exec_date = Variable.get("psql_datamart2_exec_date")
    if loader_exec_date > datamart1_exec_date or loader_exec_date > datamart2_exec_date:
        return True
    else:
        return False

with DAG(dag_id=DAG_ID,
         description='Dag to check updates of raw data',
         schedule_interval=schedule,
         default_args=DAG_DEFAULT_ARGS,
         is_paused_upon_creation=True,
         max_active_runs=1,
         catchup=False
         ) as dag:
    start_task = DummyOperator(task_id='START', dag=dag)
    end_task = DummyOperator(task_id='END', dag=dag)
    if not checker():
        trigger1 = TriggerDagRunOperator(
            task_id="trigger_data_mart_1",
            trigger_dag_id="loader"  # ADD HERE DATAMART1 DAG ID
        )
        trigger2 = TriggerDagRunOperator(
            task_id="trigger_data_mart_2",
            trigger_dag_id="loader"  # ADD HERE DATAMART2 DAG ID
        )
        start_task >> trigger1 >> trigger2 >> end_task
    else:
        start_task >> end_task
