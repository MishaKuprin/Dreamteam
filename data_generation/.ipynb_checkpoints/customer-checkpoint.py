import numpy as np
import pandas as pd
from faker import Faker
import datetime



class CustomerDataGeneration:
    """Class for generating customer data.

    :param faker_locale: Localization for data generation
    :type faker_locale: str
    :param count_to_generate: Number of customers generated
    :type count_to_generate: int
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
        self.date_of_birth = []  # 20 < date < 82 years
        self.gender = []
        self.email = []
        self.phone_number = []
        self.agree_for_promo = []
        self.autopay_card = []
        self.customer_category = []
        self.language = []
        self.customer_since = []
        self.region = []
        self.status = []
        self.customer_data_frame = pd.DataFrame(columns=['ID',
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
                                                         'status'])

    def generate_names(self):
        """Full names generator."""

        self.names = []
        for _ in range(self.count_to_generate):
            self.names.append(self.fake.name())
        self._split_names()

    def generate_gender(self, prob_M=0.487, prob_F=0.513):
        """Gender generator.

        :param prob_M: Percentage of males in the population of Japan, defaults to 0.487
        :type faker_locale: float
        :param prob_F: Percentage of females in the population of Japan, defaults to 0.513
        :type b: float
        """

        gender_list = self._dist_data_gen(gen_mask=['M', 'F'], probs=[prob_M, prob_F])
        self.gender = list(gender_list)

    def generate_email(self):
        """Email generator"""

        self.email = []
        for _ in range(self.count_to_generate):
            self.email.append(self.fake.bothify(text='?*******#') + "@" + self.fake.free_email_domain())

    def generate_customer_category(self, prob_business=0.02, prob_physical=0.98):
        """Customer category generator.

           :param prob_business: Percentage of business users, defaults to 0.02
           :type prob_business: float
           :param prob_physical: Percentage of physical users, defaults to 0.98
           :type prob_physical: float
        """

        self.customer_category = []
        self.customer_category = self._dist_data_gen(gen_mask=['business', 'physical'],
                                                     probs=[prob_business, prob_physical])

    def generate_agree_for_promo(self, prob_Y=0.33, prob_N=0.67):
        """Agree for promo generator.

           :param prob_Y: Percentage of Yes, defaults to 0.33
           :type prob_Y: float
           :param prob_N: Percentage of No, defaults to 0.67
           :type prob_N: float
        """

        self.agree_for_promo = []
        self.agree_for_promo = list(self._dist_data_gen(gen_mask=['Yes', 'No'], probs=[prob_Y, prob_N]))

    def generate_autopay_card(self, prob_Y=0.6365):
        """Autopay card generator

           :param prob_Y: Percentage of Yes, defaults to 0.6365
           :type prob_Y: float

        """

        self.autopay_card = []
        self.autopay_card = list(self._dist_data_gen(gen_mask=['Yes', 'No'], probs=[prob_Y, 1 - prob_Y]))

    def generate_birth_date(self):
        """Birth date generator."""

        self.date_of_birth = []
        generations = self._dist_data_gen(gen_mask=["middle", "old"], probs=[0.7874, 1 - 0.7874])
        for gen in generations:
            if gen == "middle":
                self.date_of_birth.append(
                    self.fake.date_between(start_date='-65y', end_date="-" + str(self.legal_age_to_buy_sim) + "y"))
            if gen == "old":
                self.date_of_birth.append(self.fake.date_between(start_date='-82y', end_date="-65y"))

    def generate_customer_since(self):
        """Customer since date generator."""

        self.customer_since = []
        telecom_generations = np.random.choice(["3g", "lte", "5g"], size=self.count_to_generate, p=[0.2, 0.6, 0.2])
        for gen in telecom_generations:
            if gen == "3g":
                self.customer_since.append(
                    self.fake.date_between(start_date=datetime.date(2000, 1, 1), end_date=datetime.date(2010, 1, 1)))
            elif gen == "lte":
                self.customer_since.append(
                    self.fake.date_between(start_date=datetime.date(2010, 1, 1), end_date=datetime.date(2020, 1, 1)))
            elif gen == "5g":
                self.customer_since.append(
                    self.fake.date_between(start_date=datetime.date(2020, 1, 1), end_date=datetime.date.today()))
            else:
                self.self.customer_since.append(
                    self.fake.date_between(start_date=datetime.date(2000, 1, 1), end_date=datetime.date.today()))

        for i in range(self.count_to_generate):
            if self.date_of_birth[i].month != 2:
                legal_date_to_buy_sim = datetime.date(self.date_of_birth[i].year + self.legal_age_to_buy_sim,
                                                      self.date_of_birth[i].month, self.date_of_birth[i].day)
            else:
                legal_date_to_buy_sim = datetime.date(self.date_of_birth[i].year + self.legal_age_to_buy_sim,
                                                      self.date_of_birth[i].month, 28)
            if self.customer_since[i] < legal_date_to_buy_sim:
                self.customer_since[i] = self.fake.date_between(start_date=legal_date_to_buy_sim,
                                                                end_date=datetime.date.today())

    def generate_region(self):
        """Region generator."""

        regions = ['Hokkaidō', 'Tōhoku', 'Kantō', 'Chūbu', 'Kansai(Kinki)', 'Chūgoku',
                   'Shikoku', 'Kyūshū & Okinawa']
        percents_of_region = [0.04213037, 0.06995231, 0.34340223, 0.16931638, 0.17806041,
                              0.0572337, 0.02941176, 0.11049285]
        self.region = []
        self.region = list(self._dist_data_gen(gen_mask=regions, probs=percents_of_region))

    def generate_status(self, prob_A=1.0, prob_I=0.0):
        """Customer status generator.

           :param prob_A: Percentage of Active customers, defaults to 1.0
           :type prob_A: float
           :param prob_I: Percentage of Inactive customers, defaults to 0.0
           :type prob_I: float
        """

        self.status = []
        self.status = list(self._dist_data_gen(gen_mask=["Active", "Inactive"], probs=[prob_A, prob_I]))

    def generate_phone_number(self):
        """Mobile phone number generator."""

        self.phone_number = []
        for _ in range(self.count_to_generate):
            self.phone_number.append(self.fake.numerify(text='90-####-####'))

    def generate_language(self, prob_jap=0.986, prob_ch=0.009, prob_kor=0.005):
        """Language generator.

           :param prob_jap: Percentage of Japanese language, defaults to 0.986
           :type prob_jap: float
           :param prob_ch: Percentage of Chinese language, defaults to 0.009
           :type prob_ch: float
           :param prob_kor: Percentage of Korean language, defaults to 0.005
           :type prob_kor: float
        """

        self.language = []
        self.language = list(
            self._dist_data_gen(gen_mask=['Japanese', 'Chinese', 'Korean'], probs=[prob_jap, prob_ch, prob_kor]))

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

        self.customer_data_frame['ID'] = np.arange(0, self.count_to_generate)
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

    def _dist_data_gen(self, gen_mask=[0, 1], probs=[0.5, 0.5]):
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

        print("ID:", np.arange(0, self.count_to_generate),
              "\nFirst Names:", self.first_name,
              "\nLast Names:", self.last_name,
              "\nFull Names:", self.names,
              "\nDate of Birth:", self.date_of_birth,
              "\nGender:", self.gender,
              "\nEmail:", self.email,
              "\nMSISDN", self.phone_number,
              "\nagree_for_promo", self.agree_for_promo,
              "\nautopay_card", self.autopay_card,
              "\ncustomer_category", self.customer_category,
              "\nlanguage", self.language,
              "\ncustomer_since", self.customer_since,
              "\nregion", self.region,
              "\nStatus", self.status)

    def save_to_csv(self, file_name="Customer.csv"):
        """Saves generated data to csv table.
           :param file_name: The name of the file to be written to, defaults to Customer.csv
           :type file_name: str
        """

        self.customer_data_frame.to_csv(file_name, index=False)