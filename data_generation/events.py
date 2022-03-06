import numpy as np
from faker import Faker
import datetime
import pandas as pd

product_instance_Data = pd.read_csv('product_instance.csv',index_col=False)
customer_Data = pd.read_csv('Customer.csv',index_col=False)
fake = Faker()

product_instance_Data_test = product_instance_Data.head(10)
customer_Data_test = customer_Data.head(10)

product_instance_Data_test = product_instance_Data_test.rename(columns={'customer_id':'ID'})
marge_2 = product_instance_Data_test.merge(customer_Data_test)

def business_product_instance_id_gen(product_instance_Data):
    business_product_instance_id_for = product_instance_Data.business_product_instance_id
    return business_product_instance_id_for

def format_time_transoform(product_instance_Data_test,marge_2):

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
    for i in range(len(marge_2['date_of_birth'])):
        date_split = str(marge_2.date_of_birth.values[i]).split("-")
        marge_2.date_of_birth.values[i] = datetime.date(int(date_split[0]),
                                                        int(date_split[1]),
                                                        int(date_split[2]))

    all_days = []
    a = []
    b = []
    IDs = []
    birth = []
    for i in range(len(product_instance_Data_test['activation_date'])):
        x = product_instance_Data_test.termination_date.values[i]-product_instance_Data_test.activation_date.values[i]
        x1 = int(x.days)
        all_days.append(x1)
        a.append(product_instance_Data_test.termination_date.values[i])
        b.append(product_instance_Data_test.activation_date.values[i])
        IDs.append(marge_2.ID.values[i])
        birth.append(marge_2.date_of_birth.values[i])
    return all_days,a,b,IDs,birth

def date_of_event_gen(all_days,a,b,IDs,birth):
    IDs_new = []
    birth_new = []
    dateEvent = []
    business_product_instance_id = []
    hour_chance = [0.01,0.01,0.01,0.01,0.01,0.02,0.03,0.06,0.07,0.08,0.06,0.07,0.04,0.03,0.04,0.06,0.08,0.06,0.08,0.06,0.04,0.03,0.02,0.02]
    for i in range(len(all_days)):
        for j in range(all_days[i]):
            business_product_instance_id.append(i)
            dateEvent.append(fake.date_between(start_date=b[i],end_date=a[i]))
            IDs_new.append(IDs[i])
            birth_new.append(birth[i])
    len1 = len(business_product_instance_id)
    x = np.arange(1,25)
    hour = np.random.choice(x,size=len1, p=hour_chance)
    minuts = np.random.randint(1,60,size=len1)
    return business_product_instance_id, dateEvent, hour, minuts,IDs_new,birth_new

def type_of_event_gen(IDs_new,birth_new):
    type_of_events = []
    type_of_events_for_rand = ['Call','SMS','Data']
    ver_old = [0.65,0.25,0.1]
    ver_middle = [0.4,0.1,0.5]
    ver_young = [0.2,0.05,0.75]
    x = datetime.date(1997,1,1)
    y = datetime.date(1967,1,1)
    for i in range(len(IDs_new)):
        if (birth_new[i] > x) and (birth_new[i] > y):
            type_of_events.append(np.random.choice(type_of_events_for_rand,p=ver_young))
        if (birth_new[i] < x) and (birth_new[i] > y):
            type_of_events.append(np.random.choice(type_of_events_for_rand, p=ver_middle))
        if (birth_new[i] < x) and (birth_new[i] < y):
            type_of_events.append(np.random.choice(type_of_events_for_rand, p=ver_old))
    return type_of_events

def duration_gen(type_of_events):
    duration_of_event = []
    for i in range(len(type_of_events)):
        if type_of_events[i] == 'Call':
            duration_of_event.append(np.random.choice(np.arange(60,2700)))
        if type_of_events[i] == 'SMS':
            duration_of_event.append(1)
        if type_of_events[i] == 'Data':
            duration_of_event.append(np.random.choice(np.arange(60, 5400)))
    return duration_of_event

all_days,a,b,IDs,birth = format_time_transoform(product_instance_Data_test,marge_2)
business_product_instance_id, dateEvent, hour, minuts,IDs_new,birth_new = date_of_event_gen(all_days,a,b,IDs,birth)
type_of_events = type_of_event_gen(IDs_new,birth_new)
duration_of_event = duration_gen(type_of_events)
