from airflow.decorators import task
import pandas as pd

@task()
def transform_c(df):
    df["full_name"] = df['first_name'].astype(str) +" "+ df['last_name']
    return df[["customer_id","full_name","date_of_birth",
               "gender","agree_for_promo","email","MSISDN","status","customer_category",
               "customer_since","customer_termination_date","region","language"]]

@task()
def transform_pid(customer, product_instance, product, payment, costed, i):  # pid
    instance = product_instance[product_instance.Status == "Active"]
    customer_id = customer.iloc(0)[i].customer_id
    instances_for_customer = instance[instance.customer_id == customer_id]

    cust_inst = instances_for_customer.merge(product)

    prod_inst_payment = cust_inst.merge(payment)
    tariff = prod_inst_payment[prod_inst_payment.product_type == "tariff"]
    if len(tariff) >= 6:
        tariff = tariff[len(tariff) - 6:]

    addon = prod_inst_payment[prod_inst_payment.product_type == "addon"]
    if len(addon) >= 6:
        addon = addon[len(addon) - 6:]
    # print(tariff.business_product_instance_id)
    # print(costed.business_product_instance_id)
    Data = costed[costed.business_product_instance_id == tariff.business_product_instance_id.values[0]]
    data_calls = Data[Data['event_type'] != 'data']

    events_by_date = data_calls[data_calls['date'] > '2022-03-01']  # .groupby('business_product_instance_id').sum())
    calling = events_by_date.groupby(events_by_date.calling_msisdn.tolist(), as_index=False).size()
    # print(calling,"calling")

    called = events_by_date.groupby(events_by_date.called_msisdn.tolist(), as_index=False).size()
    # print(called)
    events = pd.concat([calling, called], ignore_index=True)
    events = events.groupby(events['index'], as_index=False).size()
    # print(events)
    # print(called)
    events = events[events['index'] != customer.iloc(0)[i].MSISDN]
    if events['size'].max() > 1:

        print(customer.iloc(0)[i].MSISDN)
        print(events['size'].max(), "MAX")
        high_event_count_number = events[events['size'] == events['size'].max()]['index'].values[0]
        number_event_count = round(events['size'].max(), 2)
        relative_num_of_event = round(number_event_count / events.shape[0], 2)
    else:
        high_event_count_number = None
        number_event_count = None
        relative_num_of_event = None
    events_from_month = events.shape[0]
    # print("high_event_count_number ",high_event_count_number,
    #      "\n number_event_count ",number_event_count ,
    #      "\n relative_num_of_event ", relative_num_of_event)

    last_pay_date = tariff.date.values[-1]
    last_pay = tariff.amount.values[-1]

    tariff.date = pd.to_datetime(tariff['date'], format="%Y/%m/%d")
    pay_for_six_month_mean = round(
        tariff.set_index('date').groupby(pd.Grouper(freq='M'))['amount'].sum().reset_index().amount.mean())

    tariff_for_datamart = tariff[["product_name", "allowance_voice", "allowance_sms", "allowance_data",
                                  "cost_for_call", "cost_for_sms", "cost_for_data", "total_cost", "activation_date",
                                  "termination_date"]].iloc[0:1].reset_index(drop=True)

    tariff_for_datamart = tariff_for_datamart.rename(
        columns={'product_name': 'Название текущего тарифа', 'allowance_voice': 'Лимит Звонков(Мин)',
                 'allowance_sms': 'Лимит SMS(шт)',
                 'allowance_data': 'Лимит интернета(Гб)', 'cost_for_call': 'Цена звонков для тарифа (¥/мин)',
                 'cost_for_sms': 'Цена СМС для тарифа (¥/шт)',
                 'cost_for_data': 'Цена интернета для тарифа (¥/Мб)', 'total_cost': 'Полная цена тарифа',
                 'activation_date': 'Дата активации тарифа',
                 'termination_date': 'Дата деактивации тарифа'})

    addon_for_datamart = addon[["product_name", "allowance_voice", "allowance_sms", "allowance_data",
                                "cost_for_call", "cost_for_sms", "cost_for_data", "total_cost", "activation_date",
                                "termination_date"]].iloc[0:1].reset_index(drop=True)
    all_data = tariff_for_datamart.join(addon_for_datamart)

    last_data_chunk = [last_pay_date, round(last_pay), round(pay_for_six_month_mean),
                       events_from_month, high_event_count_number, number_event_count,
                       relative_num_of_event]
    last_data_chunk = pd.DataFrame([[last_pay_date, round(last_pay), round(pay_for_six_month_mean),
                                     events_from_month, high_event_count_number, number_event_count,
                                     relative_num_of_event]], columns=["AL", "AM", "AN", "AO", "AP", "AQ", "AR"])
    # last_data_chunk['Дата последней оплаты'] = last_pay_date
    # last_data_chunk['Оплата за последний месяц'] = round(last_pay)
    # last_data_chunk['Средняя оплата за последние шесть месяцев'] = round(pay_for_six_month_mean)
    # last_data_chunk['Общее количество событий (шт) (кол-во входящих/исходящих смс и звонков)'] = events_from_month
    # last_data_chunk['Номер, с которым наибольшее количество событий'] = high_event_count_number
    # last_data_chunk['Количество событий с этим номером'] = number_event_count
    # last_data_chunk['Относительное количество событий с эти номером'] = relative_num_of_event
    return all_data, last_data_chunk

@task()
def transform_cost(Data, Data_payment):
    Data_calls_onem = Data[Data['event_type'] == 'call']
    Data2 = round(Data_calls_onem[Data_calls_onem['date'] > '2022-03-01'].groupby('business_product_instance_id').sum())
    AF = Data2.drop(['event_id', 'event_id', 'minute', 'cost', 'duration', 'number_of_sms', 'hour']
                    , axis='columns').rename(columns={'total_volume': 'Использованные за прошедший месяц звонки (мин)'})

    Data_SMS_onem = Data[Data['event_type'] == 'sms']
    Data3 = Data_SMS_onem[Data_SMS_onem['date'] > '2022-03-01'].groupby('business_product_instance_id').sum()
    AG = Data3.drop(['event_id', 'event_id', 'minute', 'cost', 'duration', 'total_volume', 'hour']
                    , axis='columns').rename(columns={'number_of_sms': 'Использованные за прошедший месяц СМС (шт)'})

    Data_Data_onem = Data[Data['event_type'] == 'data']
    Data4 = Data_Data_onem[Data_Data_onem['date'] > '2022-03-01'].groupby('business_product_instance_id').sum()
    AH = Data4.drop(['event_id', 'event_id', 'minute', 'cost', 'duration', 'number_of_sms', 'hour']
                    , axis='columns').rename(
        columns={'total_volume': 'Использованный за прошедший месяц интернет (Мб)'})

    Data_calls_mean6 = Data[Data['event_type'] == 'call']
    Data5 = round(
        Data_calls_mean6[Data_calls_mean6['date'] > '2021-11-01'].groupby('business_product_instance_id').mean())
    AI = Data5.drop(['event_id', 'event_id', 'minute', 'cost', 'duration', 'number_of_sms', 'hour']
                    , axis='columns').rename(
        columns={'total_volume': 'Использованное за последние шесть месяцев среднее количество звонков (мин)'})

    Data_SMS_mean6 = Data[Data['event_type'] == 'sms']
    Data6 = round(Data_SMS_mean6[Data_SMS_mean6['date'] > '2021-11-01'].groupby('business_product_instance_id').mean())
    AJ = Data6.drop(['event_id', 'event_id', 'minute', 'cost', 'duration', 'total_volume', 'hour']
                    , axis='columns').rename(
        columns={'number_of_sms': 'Использованное за последние шесть месяцев среднее количество СМС (шт)'})

    Data_Data_mean6 = Data[Data['event_type'] == 'data']
    Data7 = round(
        Data_Data_mean6[Data_Data_mean6['date'] > '2021-11-01'].groupby('business_product_instance_id').mean())
    AK = Data7.drop(['event_id', 'event_id', 'minute', 'cost', 'duration', 'number_of_sms', 'hour']
                    , axis='columns').rename(
        columns={'total_volume': 'Использованное за последние шесть месяцев среднее количество интернета (Мб)'})

    # Data_events = Data[Data['event_type'] != 'data']
    # Data_events1 = Data_events[Data_events['date'] > '2022-03-01'].groupby('business_product_instance_id').count()
    # AO = Data_events1.drop(['date','minute','cost','duration','number_of_sms','hour','total_volume','event_type','direction','roaming','calling_msisdn','called_msisdn']
    # ,axis='columns').rename(columns={'event_id':'Общее количество событий (шт) (кол-во входящих/исходящих смс и звонков)'})
    cost_data = AF.join(AG.join(AH.join(AI.join(AJ.join(AK)))))

    return cost_data.reset_index(drop=True)

@task()
def transform_p(customer, product_instance, product, payment, costed_event):
    data_0 = transform_c(customer)
    data_1 = pd.DataFrame()
    data_3 = pd.DataFrame()
    for i in range(customer.shape[0]):
        tpid = transform_pid(customer, product_instance, product, payment, costed_event, i)
        data_1 = pd.concat([data_1,tpid[0]],ignore_index=True)
        data_3 = pd.concat([data_3,tpid[1]],ignore_index=True)
    data_2 = transform_cost(costed_event, payment)
    data = data_0.join(data_1.join(data_2.join(data_3)))
    return data

