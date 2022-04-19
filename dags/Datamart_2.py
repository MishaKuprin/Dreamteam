from airflow import DAG
import pandas as pd
from airflow.hooks.base import BaseHook
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from os import getenv
from sqlalchemy import create_engine
from airflow.models import Variable

from datetime import datetime

DAG_DEFAULT_ARGS = {'start_date': datetime(2020, 1, 1), 'depends_on_past': False, 'provide_context': True}
DEFAULT_POSTGRES_CONN_ID = "postgres_default"
AIRFLOW_HOME = "./airflow"

DATA_PATH = './data/raw'
LOAD_PATH = './data/load'

DATA_PATH = './data/raw'
EXTRACT_PATH = './data/extract'

DAG_ID = "datamart_sql_2"
schedule = "@once"

def exec_date_set(**context):
    Variable.set("psql_datamart2_exec_date",context['execution_date'].format("YYYY-MM-DD"))

def datamart_pandas(table_name: str, schema: str = "raw", conn_id: str = None) -> None:
     conn_object = BaseHook.get_connection(conn_id or DEFAULT_POSTGRES_CONN_ID)
     jdbc_url = f"postgresql://{conn_object.login}:{conn_object.password}@" \
                f"{conn_object.host}/{conn_object.schema}"
     engine = create_engine(jdbc_url)
     df = pd.read_sql("""
                with payment_last as
                (
                select p.customer_id,max(p.date) as last_date,p.amount
                from raw.payment as p
                group by p.customer_id,p.amount,p.date
                having p.date in (select max(date) from raw.payment group by customer_id)
                ),
                payment_for_all_time as
                (
                select customer_id, sum(amount) as all_money
                from raw.payment
                group by customer_id
                ),
                tariff as
                (
                select i.customer_id,i.business_product_instance_id,i.activation_date,i.termination_date,p.product_name,
                p.allowance_sms,p.allowance_voice,p.allowance_data,p.cost_for_call,p.cost_for_sms,p.cost_for_data,
                p.total_cost
                from raw.product_instance as i
                join raw.product as p on i.product_id = p.product_id
                where i."Status" = 'Active' and p.product_type = 'tariff'
                )
                select distinct c.customer_id,c.first_name,c.last_name,c.date_of_birth,c.gender,c.email,c.agree_for_promo,c.autopay_card,
                c.customer_category,c.language,c.customer_since,c.customer_termination_date,c.region,c.status,c."MSISDN",
                f.last_date,f.amount,j.all_money, d.product_name,d.activation_date,d.termination_date
                from raw.customer as c
                left join payment_last as f on f.customer_id = c.customer_id
                left join payment_for_all_time as j on j.customer_id = c.customer_id
                left join tariff as d on d.customer_id = c.customer_id
                """, engine)
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

    datamart_table = "customer_totals_2"



    customer_totals_datamart_task = PythonOperator(dag=dag,
                                             task_id=f"{DAG_ID}.RAW.{datamart_table}",
                                                    python_callable=datamart_pandas,
                                                    op_kwargs={
                                                        "table_name": datamart_table,
                                                        "conn_id": "raw_postgres"
                                                    }
                                                    )
    start_task >> customer_totals_datamart_task >> exec_dt_set >> end_task