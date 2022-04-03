from airflow.decorators import task
import pandas as pd

@task()
def transform(df):
    df["full_name"] = df['first_name'].astype(str) +" "+ df['last_name']
    return df[["ID","full_name","date_of_birth",
               "gender","agree_for_promo","email","MSISDN","status","customer_category",
               "customer_since","customer_termination_date","region","language"]]