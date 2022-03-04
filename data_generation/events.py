import numpy as np
from faker import Faker
import datetime
import pandas as pd

product_instance_Data = pd.read_csv('product_instance.csv',index_col=False)
fake = Faker()

product_instance_Data_test = product_instance_Data.head(10)

def business_product_instance_id_gen(product_instance_Data):
    business_product_instance_id_for = product_instance_Data.business_product_instance_id
    return business_product_instance_id_for

def format_time_transoform(product_instance_Data_test):

    product_instance_Data_test.termination_date.fillna(datetime.date.today(),inplace=True)

    for i in range(len(product_instance_Data_test['activation_date'])):
        date_split = str(product_instance_Data_test.activation_date.values[i]).split("-")
        product_instance_Data_test.activation_date.values[i] = datetime.date(int(date_split[0]),
                                                                             int(date_split[1]),
                                                                             int(date_split[2]))

    for i in range(len(product_instance_Data_test['termination_date'])):
        date_split = str(product_instance_Data_test.termination_date.values[i]).split("-")
        product_instance_Data_test.termination_date.values[i] = datetime.date(int(date_split[0]),
                                                                              int(date_split[1]),
                                                                              int(date_split[2]))
    all_days = []
    a = []
    b = []
    for i in range(len(product_instance_Data_test['activation_date'])):
        x = product_instance_Data_test.termination_date.values[i]-product_instance_Data_test.activation_date.values[i]
        x1 = int(x.days)
        all_days.append(x1)
        a.append(product_instance_Data_test.termination_date.values[i])
        b.append(product_instance_Data_test.activation_date.values[i])
    return all_days,a,b


def date_of_event_gen(all_days,a,b):
    dateEvent = []
    timeEvent = []
    business_product_instance_id = []
    for i in range(len(all_days)):
        for j in range(all_days[i]):
            business_product_instance_id.append(i)
            dateEvent.append(fake.date_between(start_date=b[i],end_date=a[i]))
            timeEvent.append(fake.time())
    return business_product_instance_id, dateEvent, timeEvent


all_days,a,b = format_time_transoform(product_instance_Data_test)
c,c1,c2 = date_of_event_gen(all_days,a,b)
print(c)
print(c1)
print(c2)