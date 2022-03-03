import numpy as np
from faker import Faker
import datetime
import pandas as pd

customer_Data = pd.read_csv('Customer.csv',index_col=False)
fake = Faker()
#print(type(fake.date_time_between_dates(datetime_start=datetime.datetime(2020,1,1,6,1,1), datetime_end=datetime.datetime(2020,1,1,7,1,1))))
def date_of_event():
    dateEvent = []
    events_per_day = np.random.randint(1,20, size=10)
    for i in range(len(events_per_day)):
        for j in range(events_per_day[i]*600):
            dateEvent.append(fake.date_between(start_date=datetime.date(2018,1,1)))
    return dateEvent

date_of_event()