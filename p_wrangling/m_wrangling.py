import pandas as pd
from bs4 import BeautifulSoup
import re
import requests

#Retrieving data from scraping
def get_data_scraping():
#Creating connection with Beautifulsoup to retrieve scraping data
    print('Retrieving data from data scraping in order to get the countries...')
    url = 'https://ec.europa.eu/eurostat/statistics-explained/index.php/Glossary:Country_codes'
    country_code_page = requests.get(url)
    soup_country_code = BeautifulSoup(country_code_page.content,'lxml')
    #Extracting all the values for table
    country_table = soup_country_code.find_all('table')
    return country_table

def cleaning_data_scraping(country_table, df_m1):
#Creating connection with Beautifulsoup to retrieve scraping data
    #Creating a new list for country_table_text
    country_table_text = []

    #Going through all values in country_table and append them in the new list
    for country in country_table:
        country_table_text.append(country.text)

    #Splitting the variables removing the \n values
    country_table_split = list(map(lambda x: x.split('\n'), country_table_text))

    empty_list = []

    #Appending the values cleaned in a new variable called empty_list
    for country in country_table_split:
        for country_code in country:
            if (country_code != '') and (country_code != '\xa0'):
                empty_list.append(country_code)

    #Creating a list of lists
    row_split = 2
    rows_refactored = [empty_list[x:x+row_split] for x in range(0, len(empty_list), row_split)]

    #Selecting the final columns
    country_code_df = pd.DataFrame(rows_refactored, columns=['country', 'country_code'])

    #Selecting chosen columns
    data = pd.DataFrame(rows_refactored, columns=['country', 'country_code'])

    #Replacing unwanted values
    country_code_df['country_code']= country_code_df['country_code'].str.replace('(', '')
    country_code_df['country_code']= country_code_df['country_code'].str.replace(')', '')

    #Stripping cleaned data
    country_code_df['country_code'] = country_code_df['country_code'].str.strip()

    print('Replacing country codes...')
    #Replacing country codes
    country_code_df['country_code'] = country_code_df['country_code'].str.replace('IE', 'GB')
    country_code_df['country_code'] = country_code_df['country_code'].str.replace('EL', 'GR')
    country_code_df['country_code'] = country_code_df['country_code'].str.replace('UK', 'GB')

    #Final merge of all information
    df_final_merged_country = pd.merge(df_m1, country_code_df, on='country_code', how='inner')
    df_project = df_final_merged_country[['job_title', 'country', 'dem_education_level', 'dem_full_time_job', 'rural', 'age', 'gender', 'dem_has_children', 'question_bbi_2016wave4_basicincome_awareness', 'question_bbi_2016wave4_basicincome_vote', 'question_bbi_2016wave4_basicincome_effect', 'question_bbi_2016wave4_basicincome_argumentsfor', 'question_bbi_2016wave4_basicincome_argumentsagainst']]
    df_project['dem_has_children'] = df_project['dem_has_children'].str.lower()
    df_project['gender'] = df_project['gender'].str.lower()
    df_project['question_bbi_2016wave4_basicincome_effect'] = df_project['question_bbi_2016wave4_basicincome_effect'].str.replace('‰Û_', '')

    print('Replacing misspelled values...')

    #Replace of mispelled values
    df_project['rural'] = df_project['rural'].str.replace('city','urban')
    df_project['rural'] = df_project['rural'].str.replace('Non-Rural','urban')
    df_project['rural'] = df_project['rural'].str.replace('Country','rural')
    df_project['rural'] = df_project['rural'].str.replace('countryside','rural')
    df_project['gender'] = df_project['gender'].str.replace('fem','female')
    df_project['gender'] = df_project['gender'].str.replace('femaleale','female')

    #Replacing column names
    df_project = df_project[['country', 'job_title', 'gender', 'rural', 'dem_education_level', 'dem_has_children', 'question_bbi_2016wave4_basicincome_awareness', 'question_bbi_2016wave4_basicincome_vote', 'question_bbi_2016wave4_basicincome_effect', 'question_bbi_2016wave4_basicincome_argumentsfor', 'question_bbi_2016wave4_basicincome_argumentsagainst']]
    df_rural_final = df_project.rename(columns = {"country": "Country", "job_title": "Job Title", "gender": "Gender", "rural": "Rural", "dem_education_level": "Educational Level", "dem_has_children": "Children", "question_bbi_2016wave4_basicincome_awareness": "Poll_bi_awareness", "question_bbi_2016wave4_basicincome_vote": "Poll_bi_vote", "question_bbi_2016wave4_basicincome_effect": "Poll_bi_effect", "question_bbi_2016wave4_basicincome_argumentsfor": "Poll_bi_argfor", "question_bbi_2016wave4_basicincome_argumentsagainst": "Poll_bi_argagainst"})
    return df_rural_final

def calculations(df):
    #Getting the quantity column
    print('Calculating the quality column...')
    aggr = df.groupby(['Country', 'Job Title', 'Rural']).agg({'Job Title': 'count'})
    aggr.columns = ['Quantity']
    aggr = aggr.reset_index()

    #Calculating the percentage
    print('Calculating the percentage...')
    aggr['Percentage'] = (aggr['Quantity'] / aggr['Quantity'].sum())
    aggr.sort_values(by='Quantity', ascending=True)
    aggr['Percentage'] = aggr['Percentage'].astype(float).map("{:.2%}".format)
    print('Data from web scraping cleaned! !(•̀ᴗ•́)و ̑̑')
    return aggr

def wrangling(df_m1):
    df_gross_scraping = get_data_scraping()
    df_cleaned_scraping = cleaning_data_scraping(df_gross_scraping, df_m1)
    df_calculated_scraping = calculations(df_cleaned_scraping)
    print('Finalising wrangling data...')
    print(df_calculated_scraping.head())
    return df_calculated_scraping