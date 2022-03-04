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

def date_of_event_gen():
    dateEvent = []
    timeEvent = []
    events_per_day = np.random.randint(1,20, size=10)
    for i in range(len(events_per_day)):
        for j in range(events_per_day[i]*600):
            dateEvent.append(fake.date_between(start_date=datetime.date(2018,1,1)))
            timeEvent.append(fake.time())
    return dateEvent,timeEvent

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
    for i in range(len(product_instance_Data_test['activation_date'])):
        x = product_instance_Data_test.termination_date.values[i]-product_instance_Data_test.activation_date.values[i]
        x1 = int(x.days)
        all_days.append(x1)
    print(all_days)







#product_instance_Data_test['Amount_of_time'] = product_instance_Data_test['termination_date'] - product_instance_Data_test['activation_date']

#x = datetime.date(2022,2,2)-datetime.date(2022,2,1)
#x1 = int(x.days)
format_time_transoform(product_instance_Data_test)

