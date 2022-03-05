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


a1,a2,a3,a4,a5 = format_time_transoform(product_instance_Data_test,marge_2)
b1,b2,b3,b4,b5,b6 = date_of_event_gen(a1,a2,a3,a4,a5)
