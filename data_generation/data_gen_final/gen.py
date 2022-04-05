import pandas as pd
import generation_pipeline as gp
import numpy as np
from tqdm import tqdm

cdg = gp.CustomerDataGeneration('ja_JP', 10000)
cdg.generate_all_data()
cdg.create_data_frame()
cdg.save_to_csv()

customer_df = pd.read_csv("customer.csv", index_col=False)
product_df = pd.read_csv("product.csv", index_col=False)

pi_gen = gp.ProductInstanceGeneration(customer_df, product_df, "ja_JP")
pi_gen.generate_all()
pi_gen.save_to_csv()

pi_df = pd.read_csv("product_instance.csv", index_col=False)

n = 500
customer_batch_list = []

for g, df in customer_df.groupby(np.arange(len(customer_df)) // n):
    customer_batch_list.append(df)

for batch_index in tqdm(range(0, len(customer_batch_list))):
    instance_batch = pi_df.loc[pi_df.customer_id.isin(np.arange(customer_batch_list[batch_index].customer_id.iloc[0],
                                                                customer_batch_list[batch_index].customer_id.iloc[-1] + 1))]
    evg = gp.EventGeneration(product_df=product_df, customer_df=customer_batch_list[batch_index], product_instance_df=instance_batch)
    evg.generate_all()
    evg.save_to_csv(file_name="costed_event{0}.csv".format(str(batch_index)))
    # events = gp.event_generation(instance_batch, customer_batch_list[batch_index], form=batch_index)
    costed_df = pd.read_csv("costed_event{0}.csv".format(str(batch_index)), index_col=False)

    cg = gp.ChargeGeneration(costed_df, product_df, instance_batch)
    cg.generate_all()
    cg.save_to_csv(file_name="charge{0}.csv".format(str(batch_index)))

    charge_df = pd.read_csv("charge{0}.csv".format(str(batch_index)), index_col=False)
    pg = gp.PaymentGeneration(costed_df, product_df, instance_batch, charge_df, customer_batch_list[batch_index])
    pg.generate_all()
    pg.save_to_csv(file_name="payment{0}.csv".format(str(batch_index)))
