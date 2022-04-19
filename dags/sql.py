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

DAG_ID = "datamart_sql"
schedule = "@once"

def exec_date_set(**context):
    Variable.set("psql_datamart1_exec_date",context['execution_date'].format("YYYY-MM-DD"))

def datamart_pandas(table_name: str, schema: str = "raw", conn_id: str = None) -> None:
     conn_object = BaseHook.get_connection(conn_id or DEFAULT_POSTGRES_CONN_ID)
     jdbc_url = f"postgresql://{conn_object.login}:{conn_object.password}@" \
                f"{conn_object.host}/{conn_object.schema}"
     engine = create_engine(jdbc_url)
     df = pd.read_sql("""
                with tariff as
                (
                select i.customer_id,i.business_product_instance_id,i.activation_date,i.termination_date,p.product_name,
                p.allowance_sms,p.allowance_voice,p.allowance_data,p.cost_for_call,p.cost_for_sms,p.cost_for_data,
                p.total_cost
                from raw.product_instance as i
                join raw.product as p on i.product_id = p.product_id
                where i."Status" = 'Active' and p.product_type = 'tariff'
                ), 
                addon as
                (
                select i.customer_id,i.business_product_instance_id,sum(p.allowance_sms) as all_sms,
                sum(p.allowance_voice) as all_voice,sum(p.allowance_data) as all_data,
                min(p.cost_for_call) as for_call,min(p.cost_for_sms) as for_sms,min(p.cost_for_data) as for_data,
                sum(p.total_cost) as total
                from raw.product_instance as i
                join raw.product as p on i.product_id = p.product_id
                group by i.customer_id,i.business_product_instance_id,i."Status",p.product_type 
                having i."Status" = 'Active' and p.product_type = 'addon'
                ),
                events_calls_1 as
                (
                select c.business_product_instance_id,sum(c.total_volume) as calls_1
                from (select * from raw.costed_event0 where event_type = 'call' and date > '2022-03-01') as c
                group by c.business_product_instance_id
                ),
                payment_last as
                (
                select p.customer_id,max(p.date) as last_date,p.amount
                from raw.payment as p
                group by p.customer_id,p.amount,p.date
                having p.date in (select max(date) from raw.payment group by customer_id)
                ),
                payment_avg_6 as
                (
                select s.customer_id, avg(s.amount) as mid
                from (select * from raw.payment where date > '2021-10-01') as s
                group by customer_id
                ),
                events_sms_1 as
                (
                select c.business_product_instance_id,sum(c.total_volume) as sms_1
                from (select * from raw.costed_event0 where event_type = 'sms' and date > '2022-03-01') as c
                group by c.business_product_instance_id
                ),
                events_data_1 as
                (
                select c.business_product_instance_id,sum(c.total_volume) as data_1
                from (select * from raw.costed_event0 where event_type = 'data' and date > '2022-03-01') as c
                group by c.business_product_instance_id
                ),
                events_sms_6 as
                (
                select c.business_product_instance_id,(sum(c.total_volume)/6) as sms_6
                from (select * from raw.costed_event0 where event_type = 'sms' and date > '2021-10-01') as c
                group by c.business_product_instance_id
                ),
                events_data_6 as
                (
                select c.business_product_instance_id,(sum(c.total_volume)/6) as data_6
                from (select * from raw.costed_event0 where event_type = 'data' and date > '2021-10-01') as c
                group by c.business_product_instance_id
                ),
                events_calls_6 as
                (
                select c.business_product_instance_id,(sum(c.total_volume)/6) as calls_6
                from (select * from raw.costed_event0 where event_type = 'call' and date > '2021-10-01') as c
                group by c.business_product_instance_id            
                )
                select distinct c.customer_id,c.first_name,c.last_name,c.date_of_birth,c.gender,c.email,c.agree_for_promo,c.autopay_card,
                c.customer_category,c.language,c.customer_since,c.region,c.status,c."MSISDN",
                d.product_name,d.allowance_sms,d.allowance_voice,d.allowance_data,d.cost_for_call,d.cost_for_sms,
                d.cost_for_data,d.total_cost,d.activation_date,d.termination_date,
                a.all_sms,a.all_voice,a.all_data,a.for_call,a.for_sms,a.for_data,a.total,
                l.calls_1,l1.sms_1,l2.data_1,l3.calls_6,l4.sms_6,l5.data_6,
                f.last_date,f.amount,k.mid
                from raw.customer as c
                left join tariff as d on d.customer_id = c.customer_id
                left join addon as a on a.customer_id = c.customer_id
                left join events_calls_1 as l on l.business_product_instance_id = d.business_product_instance_id
                left join events_sms_1 as l1 on l1.business_product_instance_id = d.business_product_instance_id
                left join events_data_1 as l2 on l2.business_product_instance_id = d.business_product_instance_id
                left join events_calls_6 as l3 on l3.business_product_instance_id = d.business_product_instance_id
                left join events_sms_6 as l4 on l4.business_product_instance_id = d.business_product_instance_id
                left join events_data_6 as l5 on l5.business_product_instance_id = d.business_product_instance_id
                left join payment_last as f on f.customer_id = c.customer_id
                left join payment_avg_6 as k on k.customer_id = c.customer_id
                order by c.customer_id
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

    datamart_table = "customer_totals"



    customer_totals_datamart_task = PythonOperator(dag=dag,
                                             task_id=f"{DAG_ID}.RAW.{datamart_table}",
                                                    python_callable=datamart_pandas,
                                                    op_kwargs={
                                                        "table_name": datamart_table,
                                                        "conn_id": "raw_postgres"
                                                    }
                                                    )
    start_task >> customer_totals_datamart_task >> exec_dt_set >> end_task

