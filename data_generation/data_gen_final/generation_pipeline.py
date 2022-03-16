import pandas as pd
import numpy as np
import datetime
from threading import Thread
import warnings
from tqdm import tqdm
import math as m
from faker import Faker

warnings.filterwarnings('ignore')

class CustomerDataGeneration:
    """Class for generating customer data.
    
    :param faker_locale: Localization for data generation
    :type faker_locale: str
    :param count_to_generate: Number of customers generated
    :type b: int
    """
    
    def __init__(self, faker_locale, count_to_generate):
        """Constructor."""
        
        self.count_to_generate = count_to_generate
        self.faker_locale = faker_locale
        self.fake = Faker(self.faker_locale)
        self.legal_age_to_buy_sim = 15
        
        # Data block
        self.names = []
        self.first_name = []
        self.last_name = []
        self.date_of_birth = [] #   20 < date < 82 years
        self.gender = []
        self.email = []
        self.phone_number = []
        self.agree_for_promo = []
        self.autopay_card = []
        self.customer_category = []
        self.language = []
        self.customer_since = []
        self.region = []
        # Добавить churn(клиент сменил оператора), debt(клиент не оплатил сумму по charge или сумма была меньше)
        self.status = []
        self.customer_data_frame = pd.DataFrame(columns = ['ID',
                                                           'first_name',
                                                           'last_name',
                                                           'date_of_birth',
                                                           'gender',
                                                           'email',
                                                           'MSISDN',
                                                           'agree_for_promo',
                                                           'autopay_card',
                                                          'customer_category',
                                                          'language',
                                                          'customer_since',
                                                          'region',
                                                          'status',
                                                          'customer_termination_date'])
    def generate_names(self):
        """Full names generator."""
        
        self.names = []   
        for _ in range(self.count_to_generate):
            self.names.append(self.fake.name())
        self._split_names()
    
    def generate_gender(self, prob_M = 0.487, prob_F = 0.513):
        """Gender generator.
        
        :param prob_M: Percentage of males in the population of Japan, defaults to 0.487
        :type faker_locale: float
        :param prob_F: Percentage of females in the population of Japan, defaults to 0.513
        :type b: float
        """
        
        gender_list = self._dist_data_gen(gen_mask = ['M','F'],probs = [prob_M, prob_F])
        self.gender = list(gender_list)
             
    def generate_email(self):
        """Email generator"""
        
        self.email = []   
        for _ in range(self.count_to_generate):
            self.email.append(self.fake.bothify(text='?*******#')+"@"+self.fake.free_email_domain())
            
    def generate_customer_category(self, prob_business = 0.02, prob_physical = 0.98):
        """Customer category generator.
           
           :param prob_business: Percentage of business users, defaults to 0.02
           :type prob_business: float
           :param prob_physical: Percentage of physical users, defaults to 0.98
           :type prob_physical: float
        """
        
        self.customer_category = []
        self.customer_category = self._dist_data_gen(gen_mask = ['business','physical'],probs = [prob_business, prob_physical])
        
    def generate_agree_for_promo(self, prob_Y = 0.33, prob_N = 0.67):
        """Agree for promo generator.
           
           :param prob_Y: Percentage of Yes, defaults to 0.33
           :type prob_Y: float
           :param prob_N: Percentage of No, defaults to 0.67
           :type prob_N: float
        """
        
        self.agree_for_promo =[]
        self.agree_for_promo = list(self._dist_data_gen(gen_mask = ['Yes', 'No'], probs = [prob_Y, prob_N]))
    
    def generate_autopay_card(self, prob_Y=0.6365):
        """Autopay card generator
           
           :param prob_Y: Percentage of Yes, defaults to 0.6365
           :type prob_Y: float
           
        """
            
        self.autopay_card = []
        self.autopay_card = list(self._dist_data_gen(gen_mask = ['Yes', 'No'], probs = [prob_Y, 1-prob_Y]))
    
    def generate_birth_date(self):
        """Birth date generator."""
        
        self.date_of_birth = []   
        generations = self._dist_data_gen(gen_mask = ["middle","old"],probs = [0.7874,1-0.7874])
        for gen in generations:
            if gen == "middle":
                self.date_of_birth.append(self.fake.date_between(start_date='-65y', end_date="-"+str(self.legal_age_to_buy_sim)+"y"))
            if gen == "old":
                self.date_of_birth.append(self.fake.date_between(start_date='-82y', end_date="-65y"))
        
    
    def generate_customer_since(self):
        """Customer since date generator."""
        
        self.customer_since = []
        telecom_generations = np.random.choice(["3g","lte","5g"], size = self.count_to_generate, p = [0.2,0.6,0.2])
        for gen in telecom_generations:
            if gen == "3g":
                self.customer_since.append(self.fake.date_between(start_date=datetime.date(2000,1,1), end_date=datetime.date(2010,1,1)))
            elif gen =="lte":
                self.customer_since.append(self.fake.date_between(start_date=datetime.date(2010,1,1), end_date=datetime.date(2020,1,1)))
            elif gen =="5g":
                self.customer_since.append(self.fake.date_between(start_date=datetime.date(2020,1,1), end_date=datetime.date.today()))
            else:
                self.self.customer_since.append(self.fake.date_between(start_date=datetime.date(2000,1,1), end_date=datetime.date.today()))
        
        for i in range(self.count_to_generate):
            if self.date_of_birth[i].month != 2:
                legal_date_to_buy_sim = datetime.date(self.date_of_birth[i].year+self.legal_age_to_buy_sim,
                                                      self.date_of_birth[i].month,self.date_of_birth[i].day)
            else:
                legal_date_to_buy_sim = datetime.date(self.date_of_birth[i].year+self.legal_age_to_buy_sim,
                                                      self.date_of_birth[i].month,28)
            if self.customer_since[i] < legal_date_to_buy_sim:
                self.customer_since[i] = self.fake.date_between(start_date=legal_date_to_buy_sim, end_date=datetime.date.today())
                
                
    def generate_region(self):
        """Region generator."""
        
        regions = ['Hokkaidō', 'Tōhoku', 'Kantō', 'Chūbu', 'Kansai(Kinki)', 'Chūgoku',
       'Shikoku', 'Kyūshū & Okinawa']
        percents_of_region = [0.04213037, 0.06995231, 0.34340223, 0.16931638, 0.17806041,
       0.0572337 , 0.02941176, 0.11049285]
        self.region = []
        self.region = list(self._dist_data_gen(gen_mask = regions, probs = percents_of_region))
        
    def generate_status(self, prob_A = 0.95, prob_Ic = 0.02, prob_Id = 0.03):
        """Customer status generator.
           
           :param prob_A: Percentage of Active customers, defaults to 1.0
           :type prob_A: float
           :param prob_I: Percentage of Inactive customers, defaults to 0.0
           :type prob_I: float
        """
        
        self.status = []
        self. status = list(self._dist_data_gen(gen_mask = ["active"], probs = [1]))
        
    def generate_phone_number(self):
        """Mobile phone number generator."""
        
        self.phone_number = []
        for _ in range(self.count_to_generate):
            self.phone_number.append(self.fake.numerify(text='90-####-####'))
    
    def generate_language(self, prob_jap = 0.986, prob_ch = 0.009, prob_kor = 0.005):
        """Language generator.
           
           :param prob_jap: Percentage of Japanese language, defaults to 0.986
           :type prob_jap: float
           :param prob_ch: Percentage of Chinese language, defaults to 0.009
           :type prob_ch: float
           :param prob_kor: Percentage of Korean language, defaults to 0.005
           :type prob_kor: float
        """
        
        self.language = []
        self.language = list(self._dist_data_gen(gen_mask = ['Japanese', 'Chinese','Korean'], probs = [prob_jap, prob_ch,prob_kor]))
    
    def _split_names(self):
        """Split full name to first name and last name."""
        
        self.first_name = []
        self.last_name = []
        for i in range(len(self.names)):
            self.first_name.append(self.names[i].split(' ')[1])
            self.last_name.append(self.names[i].split(' ')[0])
    
    def create_data_frame(self):
        """Filling a dataframe with data.
        :rtype: DataFrame
        :return: DataFrame with customers data 
        """
        
        self.customer_data_frame['ID'] = np.arange(0,self.count_to_generate)
        self.customer_data_frame['first_name'] = self.first_name
        self.customer_data_frame['last_name'] = self.last_name
        self.customer_data_frame['date_of_birth'] = self.date_of_birth
        self.customer_data_frame['gender'] = self.gender
        self.customer_data_frame['email'] = self.email
        self.customer_data_frame['MSISDN'] = self.phone_number
        self.customer_data_frame['agree_for_promo'] = self.agree_for_promo
        self.customer_data_frame['autopay_card'] = self.autopay_card
        self.customer_data_frame['customer_category'] = self.customer_category
        self.customer_data_frame['language'] = self.language
        self.customer_data_frame['customer_since'] = self.customer_since
        self.customer_data_frame['region'] = self.region
        self.customer_data_frame['status'] = self.status
        self.customer_data_frame['customer_termination_date'] = None
        
        return self.customer_data_frame
                                                           
    def generate_all_data(self):
        """Full customer data generator"""
        
        self.generate_names()
        self.generate_birth_date()
        self.generate_gender()
        self.generate_email()
        self.generate_phone_number()
        self.generate_agree_for_promo()
        self.generate_autopay_card()
        self.generate_customer_category()
        self.generate_language()
        self.generate_customer_since()
        self.generate_region()
        self.generate_status()
        
    def _dist_data_gen(self, gen_mask=[0,1], probs=[0.5,0.5]):
        """Distribution generator.
           
           :param gen_mask: List of values to generate, defaults to [0,1]
           :type gen_mask: list
           :param probs: Values to generates percentage, defaults to [0.5,0.5]
           :type probs: list
           :rtype: list
           :return: list of generated values
        """
            
        return np.random.choice(gen_mask, size=self.count_to_generate, p=probs)
         
    def customer_data_print(self):
        """Prints all generated data"""
        
        print("ID:",np.arange(0,self.count_to_generate),
            "\nFirst Names:",self.first_name,
              "\nLast Names:",self.last_name,
              "\nFull Names:",self.names,
              "\nDate of Birth:",self.date_of_birth,
             "\nGender:",self.gender,
             "\nEmail:",self.email,
             "\nMSISDN",self.phone_number,
             "\nagree_for_promo",self.agree_for_promo,
             "\nautopay_card",self.autopay_card,
             "\ncustomer_category",self.customer_category,
             "\nlanguage",self.language,
             "\ncustomer_since",self.customer_since,
             "\nregion",self.region,
             "\nStatus",self.status)
    
    def save_to_csv(self, file_name="Customer.csv"):
        """Saves generated data to csv table.
           :param file_name: The name of the file to be written to, defaults to Customer.csv
           :type file_name: str
        """
        
        self.customer_data_frame.to_csv(file_name,index=False)
        
class ProductInstanceGeneration:
    def __init__(self, customer_df, product_df, faker_locale):
        self.customer_df = customer_df
        self.product_df = product_df
        self.faker_locale = faker_locale
        self.fake = Faker(self.faker_locale)
        
        #Data block
        
        self.product_instance_df = pd.DataFrame(columns = ["business_product_instance_id",
                                                          "customer_id",
                                                          "product_id",
                                                          "activation_date",
                                                          "termination_date",
                                                          "Status",
                                                          "distribution_channel"]) # Финальный этап
        
        
    
    def _product_generation(self, customer_id):
        product_type_tariff = self.product_df[self.product_df.product_type == "tariff"]
        customer = self.customer_df[self.customer_df["ID"] == customer_id]
        #print(customer)
        tariff_list = []
        activation_date = []
        termination_date = []
        Status = []
        distribution_channel = []
        #print(self._age_dist(customer.date_of_birth))
        date_since_split = str(customer.customer_since.values[0]).split("-")
        customer_since = datetime.date(int(date_since_split[0]), int(date_since_split[1]), int(date_since_split[2])) # datetime.date(int(date_since_strip[0]),int(date_since_strip[1]),int(date_since_strip[2]))
        age = self._age_dist(customer.date_of_birth)
        
        #print(customer_since)
        if customer_since < datetime.date(2020,1,1): #LTE first
            # Tariff generation
            tariff = self._tariff_lte(age)
            tariff_list.append(int(np.random.choice(tariff[0], size=1, p=tariff[1])))
            activation_date.append(self.fake.date_between(start_date=datetime.date(2010,1,1), end_date=datetime.date(2020,1,1)))
            Status.append("Active")
            distribution_channel.append(np.random.choice(["online","physical_shop","other"],p=[0.2,0.75,0.05]))
            termination_date.append(None)
            # Addon generation
            if np.random.choice([True,False],p=[0.5,0.5]): # Prob for addon
                addon = self._addon_lte(age, str(customer.customer_category.values[0]))
                tariff_list.append(int(np.random.choice(addon[0], size=1, p=addon[1])))
                activation_date.append(self.fake.date_between(start_date=activation_date[-1], end_date=datetime.date(2020,1,1)))
                Status.append("Active")
                distribution_channel.append(np.random.choice(["online","physical_shop","other"],p=[0.2,0.75,0.05]))
                termination_date.append(None)
            
            if np.random.choice([True,False],p=[0.8,0.2]): # Дописать логику для выбора termination и activation_date
                tariff = self._tariff_all(age)
                new_tariff = int(np.random.choice(tariff[0], size=1, p=tariff[1]))
                while(tariff_list[0] == new_tariff): new_tariff = int(np.random.choice(tariff[0], size=1, p=tariff[1]))
                tariff_list.append(new_tariff)
                activation_date.append(self.fake.date_between(start_date=datetime.date(2020,1,1), end_date=datetime.date.today()))
                distribution_channel.append(np.random.choice(["online","physical_shop","other"],p=[0.75,0.2,0.05]))
                Status= ["Inactive" for i in range(len(Status))]
                Status.append("Active")
                termination_date = [activation_date[-1] for i in range(len(termination_date))]
                termination_date.append(None)
                
                    # Addon generation
                if np.random.choice([True,False],p=[0.5,0.5]): # Prob for addon
                    
                    addon = self._addon_all(age, tariff_list[-1], str(customer.customer_category.values[0]))
                    tariff_list.append(int(np.random.choice(addon[0], size=1, p=addon[1])))
                    activation_date.append(self.fake.date_between(start_date=activation_date[-1], end_date=datetime.date.today()))
                    Status.append("Active")
                    distribution_channel.append(np.random.choice(["online","physical_shop","other"],p=[0.2,0.75,0.05]))
                    termination_date.append(None)
                
                
        elif customer_since > datetime.date(2020,1,1): #5G or LTE users
            tariff = self._tariff_all(age)
            tariff_list.append(int(np.random.choice(tariff[0], size=1, p=tariff[1])))
            distribution_channel.append(np.random.choice(["online","physical_shop","other"],p=[0.75,0.2,0.05]))
            activation_date.append(customer_since)
            Status.append("Active")
            termination_date.append(None)
                                           
            if np.random.choice([True,False],p=[0.5,0.5]): # Prob for addon
                
                addon = self._addon_all(age, tariff_list[-1], str(customer.customer_category.values[0]))
                tariff_list.append(int(np.random.choice(addon[0], size=1, p=addon[1])))
                activation_date.append(self.fake.date_between(start_date=activation_date[-1], end_date=datetime.date.today()))
                Status.append("Active")
                distribution_channel.append(np.random.choice(["online","physical_shop","other"],p=[0.2,0.75,0.05]))
                termination_date.append(None)
                                       
        return tariff_list, activation_date, termination_date, Status, distribution_channel  
        #return np.random.choice(product_type_tariff.product_id)
    
    
    def _random_addon_generation(self):
        product_type_addon = self.product_df[self.product_df.product_type == "addon"]
        return np.random.choice(product_type_tariff.product_id)
    
    def _age_dist(self,date_of_birth):
        date = str(date_of_birth.values[0]).split("-")
        birth = datetime.date(int(date[0]), int(date[1]), int(date[2])) 
        age = int((datetime.date.today()-birth).days)//365
        if age <= 25:
            return "zoomer"
        elif age > 25 and age < 55:
            return "doomer"
        elif age >= 55:
            return "boomer"
    
    def _tariff_lte(self, generation):
        if generation == "boomer":
            return [12,5,4], [0.6, 0.3, 0.1]
        elif generation == "doomer":
            return [5, 4, 12], [0.6, 0.3, 0.1]
        elif generation == "zoomer":
            return [4, 5, 12, 13], [0.5, 0.2, 0.25, 0.05]
    
    def _tariff_all(self,generation):
        if generation == "boomer":
            return [6, 8, 5, 12, 3, 4, 1], [0.2, 0.2, 0.2, 0.1, 0.1, 0.1, 0.1]
        elif generation == "doomer":
            return [8, 5, 3, 4, 1, 6, 12], [0.2, 0.2, 0.2, 0.1, 0.1, 0.1, 0.1]
        elif generation == "zoomer":
            return [4, 1, 3, 5, 8, 12, 6, 13], [0.2, 0.2, 0.2, 0.1, 0.1, 0.1, 0.05, 0.05]
            
    def _addon_lte(self, generation, customer_category):# Возможные комбинации дополнений к тарифам
        if customer_category =="business":
            if generation == "boomer":
                return [7, 10, 11], [0.7, 0.29, 0.01]
            elif generation == "doomer":
                return [7, 10, 11], [0.5, 0.49, 0.01]
            elif generation == "zoomer":
                return [7, 10, 11], [0.3, 0.69, 0.01]
        else:
            if generation == "boomer":
                return [7, 10], [0.7,0.3]
            elif generation == "doomer":
                return [7, 10], [0.5,0.5]
            elif generation == "zoomer":
                return [7, 10], [0.3,0.7]
            
    def _addon_all(self, generation, tariff, customer_category):
        if customer_category =="business":
            if generation == "boomer":
                if tariff == 1:
                    return [2, 7], [0.5, 0.5]
                elif tariff == 3:
                    return [2, 9, 7], [0.5,0.25,0.25]
                elif tariff == 6:
                    return [9, 10, 11], [0.5,0.49,0.01]
                elif tariff == 8:
                    return [7, 9, 10, 11], [0.5,0.25,0.24,0.01]
                else:
                    return self._addon_lte(generation, customer_category)
            elif generation == "doomer":
                 if tariff == 1:
                    return [2, 7], [0.5, 0.5]
                 elif tariff == 3:
                    return [2, 9, 7], [0.2,0.4,0.4]
                 elif tariff == 6:
                    return [9, 10, 11], [0.5,0.49,0.01]
                 elif tariff == 8:
                    return [7, 9, 10, 11], [0.3,0.35,0.34,0.01]
                 else:
                    return self._addon_lte(generation, customer_category)
            elif generation == "zoomer":
                 if tariff == 1:
                    return [2, 7], [0.5, 0.5]
                 elif tariff == 3:
                    return [2, 9, 7], [0.25,0.5,0.25]
                 elif tariff == 6:
                    return [9, 10, 11], [0.5,0.49,0.01]
                 elif tariff == 8:
                    return [7, 9, 10, 11], [0.1,0.45,0.44,0.01]
                 else:
                    return self._addon_lte(generation, customer_category)
        else:
            if generation == "boomer":
                if tariff == 1:
                    return [2, 7], [0.5, 0.5]
                elif tariff == 3:
                    return [2, 9, 7], [0.5,0.25,0.25]
                elif tariff == 6:
                    return [9, 10], [0.5,0.5]
                elif tariff == 8:
                    return [7, 9, 10], [0.5,0.25,0.25]
                else:
                    return self._addon_lte(generation, customer_category)
                
            elif generation == "doomer":
                 if tariff == 1:
                    return [2, 7], [0.5, 0.5]
                 elif tariff == 3:
                    return [2, 9, 7], [0.2,0.4,0.4]
                 elif tariff == 6:
                    return [9, 10], [0.5,0.5]
                 elif tariff == 8:
                    return [7, 9, 10], [0.3,0.35,0.35]
                 else:
                    return self._addon_lte(generation, customer_category)
            elif generation == "zoomer":
                 if tariff == 1:
                    return [2, 7], [0.5, 0.5]
                 elif tariff == 3:
                    return [2, 9, 7], [0.25,0.5,0.25]
                 elif tariff == 6:
                    return [9, 10], [0.5,0.5]
                 elif tariff == 8:
                    return [7, 9, 10], [0.1,0.45,0.45]
                 else:
                    return self._addon_lte(generation, customer_category)
            
    def product_inst_for_customer(self,customer_id):
        tariff = self._product_generation(customer_id)
        
        for i in range(len(tariff[0])):
            
            instance = ["None",customer_id,tariff[0][i],tariff[1][i],tariff[2][i],tariff[3][i],tariff[4][i]]
            df_to_add = pd.DataFrame([instance],columns=self.product_instance_df.columns.values)
            
            self.product_instance_df = self.product_instance_df.append(df_to_add,ignore_index=True)
       
            # [["None"],[customer_id],[tariff[0][i]],[tariff[1][i]],[tariff[2][i]],[tariff[3][i]],["unknown"]]
            #self.product_instance_df["customer_id"][i] = customer_id
           # self.product_instance_df["product_id"][i] = tariff[0][i]
            #self.product_instance_df["activation_date"] = tariff[1][i]
            #self.product_instance_df["termination_date"] = tariff[2][i]
            #self.product_instance_df["Status"] = tariff[3][i]
    
    def generate_all(self):
        for i in range(self.customer_df.shape[0]):
            self.product_inst_for_customer(i)
        self.product_instance_df.business_product_instance_id = np.arange(0,self.product_instance_df.shape[0])
    
    def generate_all_thread(self,start,end):
        for i in range(start, end):
            self.product_inst_for_customer(i)
        
    
    def thread_execute(self,coeff):
        if self.customer_df.shape[0]%coeff == 0:
            print("thread execution",coeff)
            #print("thread",coeff)
            part_length = int(self.customer_df.shape[0]/coeff)
            #print(part_length)
            threads = []
            for i in range(coeff):
                threads.append(Thread(target=self.generate_all_thread, args = (i*part_length, (i+1)*part_length)))
                #print(i,i*part_length, (i+1)*part_length)
            for thr in tqdm(threads):
                thr.start()
                thr.join()
           
            self.product_instance_df.business_product_instance_id = np.arange(0,self.product_instance_df.shape[0])
        else:
            print("thread_error")
            
    def save_to_csv(self, file_name="product_instance.csv"):
        """Saves generated data to csv table.
           :param file_name: The name of the file to be written to, defaults to product_instance.csv
           :type file_name: str
        """
        
        self.product_instance_df.to_csv(file_name,index=False)
        
#EVENTS
def read_and_marge(product_instance_Data, customer_Data):
    #product_instance_Data = pd.read_csv('product_instance.csv',index_col=False)
    #customer_Data = pd.read_csv('Customer.csv',index_col=False)
    product = pd.read_csv('product.csv',index_col=False)
    fake = Faker()

    product_instance_Data_test = product_instance_Data
    customer_Data_test = customer_Data

    product_instance_Data_test = product_instance_Data_test.rename(columns={'customer_id':'ID'})
    marge_2 = product_instance_Data_test.merge(customer_Data_test)
    marge_3 = marge_2.merge(product)
    return marge_3

def format_time_transoform(marge_3):

    marge_3.termination_date.fillna(datetime.date.today(),inplace=True)
    for i in range(len(marge_3['activation_date'])):
        if (marge_3.product_type.values[i] == 'addon') and (marge_3.Status.values[i] == 'active'):
            for j in range(len(marge_3['activation_date'])):
                if (marge_3.ID.values[i] == marge_3.ID.values[j]) and (marge_3.Status.values[j] == 'active'):
                    marge_3.termination_date.values[j] = marge_3.activation_date.values[i]
    for i in range(len(marge_3['activation_date'])):
        if (marge_3.product_type.values[i] == 'addon') and (marge_3.Status.values[i] == 'inactive'):
            for j in range(len(marge_3['activation_date'])):
                if (marge_3.ID.values[i] == marge_3.ID.values[j]) and (marge_3.Status.values[j] == 'inactive'):
                    marge_3.termination_date.values[j] = marge_3.activation_date.values[i]

    for i in range(len(marge_3['activation_date'])):
        date_split = str(marge_3.activation_date.values[i]).split("-")
        marge_3.activation_date.values[i] = datetime.date(int(date_split[0]),
                                                                             int(date_split[1]),
                                                                             int(date_split[2]))

    for i in range(len(marge_3['termination_date'])):
        date_split = str(marge_3.termination_date.values[i]).split("-")
        marge_3.termination_date.values[i] = datetime.date(int(date_split[0]),
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
    for i in range(len(marge_3['activation_date'])):
        x = marge_3.termination_date.values[i]- marge_3.activation_date.values[i]
        x1 = int(x.days)
        all_days.append(x1)
        a.append(marge_3.termination_date.values[i])
        b.append(marge_3.activation_date.values[i])
        MSISDNs.append(marge_3.MSISDN.values[i])
        product_ids.append(marge_3.product_id.values[i])
        IDs.append(marge_3.ID.values[i])
        birth.append(marge_3.date_of_birth.values[i])
    return all_days,a,b,IDs,birth,MSISDNs,product_ids

def date_of_event_gen(all_days,a,b,IDs,birth,MSISDNs,product_ids,product_instance_Data):
    fake = Faker()
    IDs_new = []
    birth_new = []
    dateEvent = []
    MSISDNs_new = []
    product_ids_new = []
    business_product_instance_id = []
    hour_chance = [0.01,0.01,0.01,0.01,0.01,0.02,0.03,0.06,0.07,0.08,0.06,0.07,0.04,0.03,0.04,0.06,0.08,0.06,0.08,0.06,0.04,0.03,0.02,0.02]
    for i in range(len(all_days)):
        for j in range(all_days[i]*6):
            business_product_instance_id.append(product_instance_Data.business_product_instance_id.values[0]+i)
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
            duration_of_event.append(np.random.choice(np.arange(60,2100)))
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
                    cost.append(total_volume[i]*2.2*3)
                if type_of_events[i] == 'SMS':
                    cost.append(number_of_sms[i]*3.3*3)
                if type_of_events[i] == 'Data':
                    cost.append(0)
            if roaming[i] == 'No':
                if type_of_events[i] == 'Call':
                    cost.append(total_volume[i]*2.2)
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
    fake = Faker()
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

def all_gen(event_id,business_product_instance_id,dateEvent, hour, minuts,cost,duration_of_event,number_of_sms,total_volume,type_of_events,direction,roaming,calling_msisdn,called_msisdn):
    df = pd.DataFrame({'event_id':event_id,'business_product_instance_id':business_product_instance_id,'date':dateEvent,'hour':hour,'minuts':minuts,
                       'cost':cost,'duration':duration_of_event,'number_of_sms':number_of_sms,'total_volume':total_volume,'event_type':type_of_events,
                       'direction':direction,'roaming':roaming,'calling_msisdn':calling_msisdn,'called_msisdn':called_msisdn})
    return df

def event_generation(product_instance_Data, customer_Data, form=0):
    marge_3 = read_and_marge(product_instance_Data, customer_Data)
    all_days,a,b,IDs,birth,MSISDNs,product_ids = format_time_transoform(marge_3)
    business_product_instance_id, dateEvent, hour, minuts,IDs_new,birth_new,MSISDNs_new,product_ids_new = date_of_event_gen(all_days,a,b,IDs,birth,MSISDNs,product_ids,product_instance_Data)
    type_of_events = type_of_event_gen(IDs_new,birth_new)
    duration_of_event = duration_gen(type_of_events)
    total_volume = total_volume_gen(type_of_events,duration_of_event)
    number_of_sms = number_of_sms_gen(type_of_events,total_volume)
    event_id = event_id_gen(type_of_events)
    direction = direction_gen(event_id,type_of_events)
    roaming = roaming_gen(event_id)
    cost = cost_gen(product_ids_new,type_of_events,total_volume,number_of_sms,roaming)
    calling_msisdn,called_msisdn = calling_msisdn_and_called_msisdn_gen(direction,MSISDNs_new)
    df = all_gen(event_id,business_product_instance_id,dateEvent, hour, minuts,cost,duration_of_event,number_of_sms,total_volume,type_of_events,direction,roaming,calling_msisdn,called_msisdn)
    df.to_csv("costed_event{0}.csv".format(str(form)))

class ChargeGeneration:
    def __init__(self,costed_df,product_df,pi_df):
        self.costed_df = costed_df
        self.product_df = product_df
        self.pi_df = pi_df
        
        self.charge = pd.DataFrame(columns = ["charge_id",
                                              "business_product_instance_id",
                                              "charge_counter",
                                              "date",
                                              "cost",
                                              "event_type"]) # Финальный этап
    
    def generate_for_one_instance(self, product_instance_id):
        event_for_instance_id = self.costed_df[self.costed_df.business_product_instance_id == product_instance_id].sort_values(by="date")
        
        less_event_data = event_for_instance_id[["date","cost"]].copy()
       
        cost_for_product_by_instance_id = self.product_df[self.product_df.product_id == int(self.pi_df[self.pi_df.business_product_instance_id == product_instance_id].product_id)].total_cost
        
        business_product_instance_id = []
        charge_counter = [] # Counter for recurrent charge. You can use it as technical field for generation
        date = [] # Date of the event
        cost = [] # Cost for charge
        event_type = [] # Charge type. One-time or recurrent
        
        instance_charges = self._charge_by_events(less_event_data)
        
        #print(instance_charges)
        for index,rows in instance_charges.iterrows():
            business_product_instance_id.append(product_instance_id)
            #print(rows.cost)
            charge_counter.append(float(rows.cost))
            cost.append(float(rows.cost) + float(cost_for_product_by_instance_id.values[0]))
            date.append(str(rows.date.date()))
            event_type.append("recurrent")
            
        
        return business_product_instance_id, charge_counter, date, cost, event_type
        
    
    def _charge_by_events(self, less_event_data):
        less_event_data.date = pd.to_datetime(less_event_data['date'],format="%Y/%m/%d")
        #less_event_data.date = less_event_data['date'].dt.date
        less_event_data = less_event_data.set_index('date')    
        sum_by_m = less_event_data.groupby(pd.Grouper(freq='M'))['cost'].sum().reset_index()
        return sum_by_m
    
    def charge_for_instance(self, product_instance_id):
        charge = self.generate_for_one_instance(product_instance_id)
        for i in range(len(charge[0])):
            
            instance = ["None", charge[0][i], charge[1][i], charge[2][i], charge[3][i], charge[4][i]]
            df_to_add = pd.DataFrame([instance],columns=self.charge.columns.values)
            
            self.charge = self.charge.append(df_to_add,ignore_index=True)
        return charge
    
    def generate_all(self):
        for i in range(self.pi_df.business_product_instance_id.iloc[0], self.pi_df.business_product_instance_id.iloc[-1]+1):
            self.charge_for_instance(i)
        self.charge.charge_id = np.arange(0,self.charge.shape[0])
    
    def generate_all_thread(self,start,end):
        for i in range(start, end):
            self.charge_for_instance(i)
        
    
    def thread_execute(self,coeff):
        if self.costed_df.business_product_instance_id.nunique()%coeff == 0:
            print("thread",coeff)
            part_length = int(self.costed_df.business_product_instance_id.nunique()/coeff)
            print(part_length)
            threads = []
            for i in range(coeff):
                threads.append(Thread(target=self.generate_all_thread, args = (i*part_length, (i+1)*part_length)))
                print(i,i*part_length, (i+1)*part_length)
            for thr in tqdm(threads):
                thr.start()
                thr.join()
           
            self.charge.charge_id = np.arange(0,self.charge.shape[0])
            '''
            thr1 = Thread(target=self.generate_all_thread, args = (0, part_length))
            thr2 = Thread(target = self.generate_all_thread, args = (part_length, 2*part_length))
            thr3 = Thread(target=self.generate_all_thread, args = (2*part_length, 3*part_length))
            thr4 = Thread(target = self.generate_all_thread, args = (3*part_length, 4*part_length))
            thr1.start(), thr2.start(),thr3.start(),thr4.start()
            thr1.join(), thr2.join(),thr3.join(), thr4.join()
            '''
    def save_to_csv(self, file_name="charge.csv"):
        """Saves generated data to csv table.
           :param file_name: The name of the file to be written to, defaults to charge.csv
           :type file_name: str
        """
        
        self.charge.to_csv(file_name,index=False)
        
class PaymentGeneration:
    def __init__(self,costed_df,product_df,pi_df,charge_df,customer_df):
        self.costed_df = costed_df
        self.product_df = product_df
        self.pi_df = pi_df
        self.charge_df = charge_df
        self.customer_df = customer_df
        self.customer_count_to_gen2 = int(self.pi_df[self.pi_df.business_product_instance_id == max(self.costed_df.business_product_instance_id.values)].customer_id)+1
        self.customer_count_to_gen1 = int(self.pi_df[self.pi_df.business_product_instance_id == min(self.costed_df.business_product_instance_id.values)].customer_id)
        self.payment = pd.DataFrame(columns = ["payment_id",
                                              "customer_id",
                                              "payment_method",
                                              "date",
                                              "amount"]) # Финальный этап
    
    def generate_for_one_customer(self, customer_id):
        payment = self.customer_df[self.customer_df.ID == customer_id].autopay_card.values[0]
        
        customer_id_list = [] # Counter for recurrent charge. You can use it as technical field for generation
        payment_method = [] # Cost for charge
        date = [] # Date of the event
        amount = [] # Charge type. One-time or recurrent
        customer_instance_list = list(self.pi_df[self.pi_df.customer_id == customer_id].business_product_instance_id)
        
        for i in customer_instance_list:
            instance_charges = self._get_charge_for_customer_instance(i)
            for index,rows in instance_charges.iterrows():
                customer_id_list.append(customer_id)
                if payment == "Yes":
                    payment_method.append("card")
                if payment == "No":
                    payment_method.append(np.random.choice(["gift_card","physical_store","terminal"],size=1)[0])
                date.append(rows.date)
                amount.append(float(rows.cost))
        return customer_id_list,payment_method,date,amount
    
    def _get_charge_for_customer_instance(self,instance_id):
        cdf = self.charge_df[self.charge_df.business_product_instance_id ==instance_id]
        less_charge_data = cdf[["date","cost"]].copy()
        return less_charge_data
            
        
    def payment_for_customer(self, customer_id):
        payment = self.generate_for_one_customer(customer_id)
        for i in range(len(payment[0])):
            
            payments = ["None", payment[0][i], payment[1][i], payment[2][i], payment[3][i]]
            df_to_add = pd.DataFrame([payments],columns=self.payment.columns.values)
            
            self.payment = self.payment.append(df_to_add,ignore_index=True)
        
    
    def generate_all(self):
        for i in range(self.customer_count_to_gen1,self.customer_count_to_gen2): #self.customer_count_to_gen
            self.payment_for_customer(i)
        self.payment.payment_id = np.arange(0,self.payment.shape[0])
    
    def generate_all_thread(self,start,end):
        for i in range(start, end):
            self.payment_for_customer(i)
        
    
    def thread_execute(self,coeff=1): #self.customer_count_to_gen
        if self.customer_count_to_gen%coeff == 0:
            print("thread execution",coeff)
            part_length = int(self.customer_count_to_gen/coeff) #self.customer_count_to_gen
            print(part_length)
            threads = []
            for i in range(coeff):
                threads.append(Thread(target=self.generate_all_thread, args = (i*part_length, (i+1)*part_length)))
                #print(i,i*part_length, (i+1)*part_length)
            for thr in tqdm(threads):
                thr.start()
                thr.join()
             
           
            self.payment.payment_id = np.arange(0,self.payment.shape[0])
        else:
            print("thread_error")
            
    def save_to_csv(self, file_name="payment.csv"):
        """Saves generated data to csv table.
           :param file_name: The name of the file to be written to, defaults to payment.csv
           :type file_name: str
        """
        
        self.payment.to_csv(file_name,index=False)