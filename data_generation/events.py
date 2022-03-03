import numpy as np
from faker import Faker
import datetime
import pandas as pd

customer_Data = pd.read_csv('Customer.csv',index_col=False)
fake = Faker()

def Customer_ID_for_product_instanse(customer_Data):
    customer_ID = customer_Data.ID
    return  customer_ID

def date_of_event():
    dateEvent = []
    timeEvent = []
    events_per_day = np.random.randint(1,20, size=10)
    for i in range(len(events_per_day)):
        for j in range(events_per_day[i]*600):
            dateEvent.append(fake.date_between(start_date=datetime.date(2018,1,1)))
            timeEvent.append(fake.time())
    return dateEvent,timeEvent

