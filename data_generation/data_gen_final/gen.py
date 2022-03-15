import pandas as pd
import generation_pipeline as gp

cdg = gp.CustomerDataGeneration('ja_JP',10000)
cdg.generate_all_data()
cdg.create_data_frame()
cdg.save_to_csv()

customer_df = pd.read_csv("Customer.csv",index_col = False)
product_df = pd.read_csv("product.csv",index_col = False)

pi_gen = gp.ProductInstanceGeneration(customer_df,product_df,"ja_JP")
pi_gen.generate_all()
pi_gen.save_to_csv()

pi_df = pd.read_csv("product_instance.csv",index_col = False)

events = gp.event_generation()

costed_df = pd.read_csv("costed_event.csv",index_col = False)

cg = gp.ChargeGeneration(costed_df,product_df,pi_df)
cg.generate_all()
cg.save_to_csv()

charge_df = pd.read_csv("charge.csv",index_col = False)

pg = gp.PaymentGeneration(costed_df, product_df, pi_df, charge_df, customer_df)
pg.generate_all()
pg.save_to_csv()