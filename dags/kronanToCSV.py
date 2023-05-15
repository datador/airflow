from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# Other required imports
import pandas as pd
import numpy as np
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
import time

# Variables:
page_list = ['01-00-00-avextir', '02-00-00-graenmeti', '03-00-00-fiskur'\
    , '04-00-00-kjot', '05-00-00-mjolkurvorur-og-egg', '06-00-00-braud-kokur-og-kex'\
    , '07-00-00-a-braud', '08-00-00-eldamennskan']

name_xpath = '//div[@class="ProductCard__CardInfo-sc-1wuo8qx-7 jHoaBC"]/div[2]/p[1]'
price_xpath = '//div[@class="ProductCard__CardInfo-sc-1wuo8qx-7 jHoaBC"]/div[4]/p[1]'
end_xpath = '//p[@class="P__PLarge-sc-7y1ajs-2 ezWkvj"]'
append_list = []

chrome_options = Options()
chrome_options.add_argument("--headless")
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")
chrome_options.add_argument("--disable-gpu")
chrome_options.add_experimental_option(
    "prefs", {"profile.managed_default_content_settings.images": 2}
)

driver = webdriver.Chrome('/usr/bin/chromedriver', options=chrome_options) # Raspberry pi path

today = datetime.now().strftime('%Y%m%d%H%M')

def scrape_data():
    append_list = []
    for i in page_list:
        url = f'https://kronan.is/snjallverslun/{i}'
        driver.get(url)
        end = driver.find_elements_by_xpath(end_xpath)
        while end == []:
            time.sleep(0.5)
            driver.execute_script('window.scrollBy(0,document.body.scrollHeight)')
            end = driver.find_elements_by_xpath(end_xpath)

        name = driver.find_elements_by_xpath(name_xpath)
        price = driver.find_elements_by_xpath(price_xpath)
        for j in range(len(name)):
            temp_dict = {
                'product': name[j].text,
                'price': price[j].text,
                'category': i
            }
            append_list.append(temp_dict)
        time.sleep(0.1)
    #today = datetime.now().strftime('%Y%m%d%H%M')
    df = pd.DataFrame(append_list)
    df['timestamp'] = today
    df['store'] = 'kronan'
    return df

def create_csv(df):
    df.to_csv(f'data/kronan_{today}.csv', index=False)
    df.to_csv('data/kronan.csv', mode='a', index=False, header=False)

def transform_data(df):
    df_transformed = df.copy()
    df_transformed['price_stk'] = df_transformed['price'].str.split('-').str[-1:]
    df_transformed['price_stk'] = df_transformed['price_stk'].astype(str).str.replace('.','').str.extract('(\d+)').astype(int)

    df_transformed['price_kg'] = np.where(df_transformed['price'].str.contains('/'), df_transformed['price'].str.split('-').str[1], np.nan)
    df_transformed['price_kg'] = df_transformed['price_kg'].astype(str).str.replace('.','').str.extract('(\d+)').astype(float)

    df_transformed['weigth'] = df_transformed['price'].str.replace('ca. ', '').str.split('-').str[0].str.split(' ').str[0].astype(float)
    df_transformed['measure'] = df_transformed['price'].str.replace('ca. ', '').str.split('-').str[0].str.split(' ').str[1].str.replace('.', '')

    df_transformed[['product', 'price_stk', 'price_kg', 'weigth', 'measure', 'category', 'store', 'timestamp']].to_csv(f'data/kronan_transformed{today}.csv')

# Define the DAG
dag = DAG('kronan_scraper', description='Scrapes kronan data and transforms it',
          schedule_interval='@daily',
          start_date=datetime(2023, 5, 15), catchup=False)

# Define the tasks/operators in the DAG
scrape_data_task = PythonOperator(task_id='scrape_data', python_callable=scrape_data, dag=dag)

create_csv_task = PythonOperator(task_id='create_csv', python_callable=create_csv, dag=dag)

transform_data_task = PythonOperator(task_id='transform_data', python_callable=transform_data, dag=dag)

# Define the dependencies between the tasks/operators
scrape_data_task >> create_csv_task >> transform_data_task
