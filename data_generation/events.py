import numpy as np
from faker import Faker
import datetime
import pandas as pd
import math as m

product_instance_Data = pd.read_csv('product_instance.csv',index_col=False)
customer_Data = pd.read_csv('Customer.csv',index_col=False)
product = pd.read_csv('product.csv',index_col=False)
fake = Faker()

product_instance_Data_test = product_instance_Data.head(10)
customer_Data_test = customer_Data.head(10)

product_instance_Data_test = product_instance_Data_test.rename(columns={'customer_id':'ID'})
marge_2 = product_instance_Data_test.merge(customer_Data_test)
marge_3 = marge_2.merge(product)

def business_product_instance_id_gen(product_instance_Data):
    business_product_instance_id_for = product_instance_Data.business_product_instance_id
    return business_product_instance_id_for

def format_time_transoform(product_instance_Data_test,marge_3):

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
    for i in range(len(marge_3['date_of_birth'])):
        date_split = str(marge_3.date_of_birth.values[i]).split("-")
        marge_3.date_of_birth.values[i] = datetime.date(int(date_split[0]),
                                                        int(date_split[1]),
                                                        int(date_split[2]))

    all_days = []
    a = []
    b = []
    IDs = []
    birth = []
    MSISDNs = []
    product_ids = []
    for i in range(len(product_instance_Data_test['activation_date'])):
        x = product_instance_Data_test.termination_date.values[i]-product_instance_Data_test.activation_date.values[i]
        x1 = int(x.days)
        all_days.append(x1)
        a.append(product_instance_Data_test.termination_date.values[i])
        b.append(product_instance_Data_test.activation_date.values[i])
        MSISDNs.append(marge_3.MSISDN.values[i])
        product_ids.append(marge_3.product_id.values[i])
        IDs.append(marge_3.ID.values[i])
        birth.append(marge_3.date_of_birth.values[i])
    return all_days,a,b,IDs,birth,MSISDNs,product_ids

def date_of_event_gen(all_days,a,b,IDs,birth,MSISDNs,product_ids):
    IDs_new = []
    birth_new = []
    dateEvent = []
    MSISDNs_new = []
    product_ids_new = []
    business_product_instance_id = []
    hour_chance = [0.01,0.01,0.01,0.01,0.01,0.02,0.03,0.06,0.07,0.08,0.06,0.07,0.04,0.03,0.04,0.06,0.08,0.06,0.08,0.06,0.04,0.03,0.02,0.02]
    for i in range(len(all_days)):
        for j in range(all_days[i]*14):
            business_product_instance_id.append(i)
            dateEvent.append(fake.date_between(start_date=b[i],end_date=a[i]))
            IDs_new.append(IDs[i])
            birth_new.append(birth[i])
            MSISDNs_new.append(MSISDNs[i])
            product_ids_new.append(product_ids[i])
    len1 = len(business_product_instance_id)
    x = np.arange(1,25)
    hour = np.random.choice(x,size=len1, p=hour_chance)
    minuts = np.random.randint(1,60,size=len1)
    return business_product_instance_id, dateEvent, hour, minuts,IDs_new,birth_new,MSISDNs_new,product_ids_new

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

def total_volume_gen(type_of_events,duration_of_event):
    total_volume = []
    for i in range(len(type_of_events)):
        if type_of_events[i] == 'Call':
            total_volume.append(duration_of_event[i]//60)
        if type_of_events[i] == 'SMS':
            total_volume.append(np.random.choice(np.arange(10, 200)))
        if type_of_events[i] == 'Data':
            total_volume.append(np.random.choice(np.arange(10, 1500)))
    return total_volume

def number_of_sms_gen(type_of_events,total_volume):
    number_of_sms = []
    for i in range(len(type_of_events)):
        if type_of_events[i] == 'SMS':
            number_of_sms.append(m.ceil(total_volume[i]/70))
        if  (type_of_events[i] == 'Call') or (type_of_events[i] == 'Data'):
            number_of_sms.append(0)
    return number_of_sms

def event_id_gen(type_of_events):
    event_id = [i for i in range(0,len(type_of_events))]
    return event_id

def direction_gen(event_id,type_of_events):
    direction = []
    for i in range(len(event_id)):
        if  (type_of_events[i] == 'Call') or (type_of_events[i] == 'SMS'):
            direction.append(np.random.choice(['In','Out'],p=[0.5,0.5]))
        if type_of_events[i] == 'Data':
            direction.append('NaN')
    return direction

def roaming_gen(event_id):
    roaming = []
    for i in range(len(event_id)):
        roaming.append(np.random.choice(['Yes','No'],p=[0.1,0.9]))
    return roaming

def cost_gen(product_ids,type_of_events,total_volume,number_of_sms,roaming):
    cost = []
    for i in range(len(product_ids)):
        if (product_ids[i] == 1) or (product_ids[i] == 3) or (product_ids[i] == 4) or (product_ids[i] == 5) or (product_ids[i] == 8) or (product_ids[i] == 12) or (product_ids[i] == 13):
            if roaming[i] == 'Yes':
                if type_of_events[i] == 'Call':
                    cost.append(total_volume[i]*22*3)
                if type_of_events[i] == 'SMS':
                    cost.append(number_of_sms[i]*3.3*3)
                if type_of_events[i] == 'Data':
                    cost.append(0)
            if roaming[i] == 'No':
                if type_of_events[i] == 'Call':
                    cost.append(total_volume[i]*22)
                if type_of_events[i] == 'SMS':
                    cost.append(number_of_sms[i]*3.3)
                if type_of_events[i] == 'Data':
                    cost.append(0)
        if (product_ids[i] == 2) or (product_ids[i] == 7) or (product_ids[i] == 9) or (product_ids[i] == 10) or (product_ids[i] == 11):
            if roaming[i] == 'Yes':
                if type_of_events[i] == 'Call':
                    cost.append(0)
                if type_of_events[i] == 'SMS':
                    cost.append(0)
                if type_of_events[i] == 'Data':
                    cost.append(0)
            if roaming[i] == 'No':
                if type_of_events[i] == 'Call':
                    cost.append(0)
                if type_of_events[i] == 'SMS':
                    cost.append(0)
                if type_of_events[i] == 'Data':
                    cost.append(0)
        if (product_ids[i] == 6):
            if roaming[i] == 'Yes':
                if type_of_events[i] == 'Call':
                    cost.append(0)
                if type_of_events[i] == 'SMS':
                    cost.append(number_of_sms[i]*3.3*3)
                if type_of_events[i] == 'Data':
                    cost.append(0)
            if roaming[i] == 'No':
                if type_of_events[i] == 'Call':
                    cost.append(0)
                if type_of_events[i] == 'SMS':
                    cost.append(number_of_sms[i]*3.3)
                if type_of_events[i] == 'Data':
                    cost.append(0)
    return cost

def calling_msisdn_and_called_msisdn_gen(direction,MSISDNs_new):
    calling_msisdn = []
    called_msisdn = []
    for i in range(len(direction)):
        if direction[i] == 'In':
            called_msisdn.append(MSISDNs_new[i])
            calling_msisdn.append(fake.numerify(text='90-####-####'))
        if direction[i] == 'Out':
            called_msisdn.append(fake.numerify(text='90-####-####'))
            calling_msisdn.append(MSISDNs_new[i])
        if direction[i] == 'NaN':
            called_msisdn.append('NaN')
            calling_msisdn.append('NaN')
    return calling_msisdn,called_msisdn

def all(event_id,business_product_instance_id,dateEvent, hour, minuts,cost,duration_of_event,number_of_sms,total_volume,type_of_events,direction,roaming,calling_msisdn,called_msisdn):
    df = pd.DataFrame({'event_id':event_id,'business_product_instance_id':business_product_instance_id,'date':dateEvent,'hour':hour,'minuts':minuts,
                       'cost':cost,'duration':duration_of_event,'number_of_sms':number_of_sms,'total_volume':total_volume,'event_type':type_of_events,
                       'direction':direction,'roaming':roaming,'calling_msisdn':calling_msisdn,'called_msisdn':called_msisdn})
    return df

all_days,a,b,IDs,birth,MSISDNs,product_ids = format_time_transoform(product_instance_Data_test,marge_2)
business_product_instance_id, dateEvent, hour, minuts,IDs_new,birth_new,MSISDNs_new,product_ids_new = date_of_event_gen(all_days,a,b,IDs,birth,MSISDNs,product_ids)
type_of_events = type_of_event_gen(IDs_new,birth_new)
duration_of_event = duration_gen(type_of_events)
total_volume = total_volume_gen(type_of_events,duration_of_event)
number_of_sms = number_of_sms_gen(type_of_events,total_volume)
event_id = event_id_gen(type_of_events)
direction = direction_gen(event_id,type_of_events)
roaming = roaming_gen(event_id)
cost = cost_gen(product_ids_new,type_of_events,total_volume,number_of_sms,roaming)
calling_msisdn,called_msisdn = calling_msisdn_and_called_msisdn_gen(direction,MSISDNs_new)

df = all(event_id,business_product_instance_id,dateEvent, hour, minuts,cost,duration_of_event,number_of_sms,total_volume,type_of_events,direction,roaming,calling_msisdn,called_msisdn)
df.to_csv("costed_event.csv")

