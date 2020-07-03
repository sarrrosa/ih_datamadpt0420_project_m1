import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from functools import reduce
import requests
from bs4 import BeautifulSoup

#This functions will acquire all the data we need for the project

#This function will get the information from the Database

def get_data_ddbb(path):
#Connection details database
    print('Retrieving data from database...')
    engine = create_engine(f'sqlite:///{path}')
    personal_info = pd.read_sql_table(table_name='personal_info', con=engine)
    career_info = pd.read_sql_table(table_name='career_info', con=engine)
    country_info = pd.read_sql_table(table_name='country_info', con=engine)
    poll_info = pd.read_sql_table(table_name='poll_info', con=engine)

    df_database_final = reduce(lambda left_table, right_table: pd.merge(left_table, right_table, on='uuid'),
                       [personal_info, career_info, country_info, poll_info])

    df_database_final = df_database_final.rename(columns = {"uuid": "id", "normalized_job_code": "uuid"})

    return df_database_final

#Function to retrieve data from API

def get_data_api():
    #Creating connection with the API
    print('Retrieving data from API provided...')
    URL_API = 'http://api.dataatwork.org/v1/jobs/autocomplete?contains=data'
    response = requests.get(URL_API)
    json_data = response.json()
    df_api = pd.DataFrame(json_data)
    df_api = df_api[['uuid', 'suggestion']]
    df_api = df_api.rename(columns = {"suggestion": "job_title"})
    return df_api

#Function to retrieve scraping data

def get_data_scraping():
#Creating connection with Beautifulsoup to retrieve scraping data
    print('Retrieving data from data scraping in order to get the countries...')
    url = 'https://ec.europa.eu/eurostat/statistics-explained/index.php/Glossary:Country_codes'
    country_code_page = requests.get(url)
    soup_country_code = BeautifulSoup(country_code_page.content,'lxml')
    #Extracting all the values for table
    country_table = soup_country_code.find_all('table')
    return country_table

def df_merge(df_1, df_2, on):
    print('Merging database...')
    df_bbdd_final = pd.merge(df_1, df_2, on='uuid', how='inner')
    return df_bbdd_final

def acquire(path):
    df_ddbb = get_data_ddbb(path)
    df_api = get_data_api()
    df_m1 = pd.merge(df_ddbb, df_api, on='uuid', how='inner')
    print('Database merged completely!')
    return df_m1