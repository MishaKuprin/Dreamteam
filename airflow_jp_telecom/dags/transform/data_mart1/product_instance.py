from airflow.decorators import task
import pandas as pd

@task()
def transform_pid(customer, product_instance, product, charge, payment, i):  # pid
    instance = product_instance[product_instance.Status == "Active"]
    customer_id = customer.iloc(0)[i].ID
    instances_for_customer = instance[instance.customer_id == customer_id]
    instances_for_customer = instances_for_customer.rename(columns={'customer_id': 'ID'})
    cust_inst = instances_for_customer.merge(product)
    prod_inst_charge = cust_inst.merge(charge)
    tariff = prod_inst_charge[prod_inst_charge.product_type == "tariff"]
    if len(tariff) >= 6:
        tariff = tariff[len(tariff) - 6:]
    # print(tariff)
    addon = prod_inst_charge[prod_inst_charge.product_type == "addon"]
    if len(addon) >= 6:
        addon = addon[len(addon) - 6:]

    return tariff, addon

@task()
def transform(customer, product_instance, product, charge, payment):
    for i in range(len(customer)):
        t_pid = transform_pid(customer, product_instance, product, charge, payment,i)
        tariff = t_pid[0]
        addon = t_pid[1]