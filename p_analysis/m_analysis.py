import pandas as pd

def all_country_to_csv(df):
    print('printing data to csv file...')
    all_countries_csv = df.to_csv('./data/results/all_countries_analysis.csv', index=False)
    print('csv information with all countries generated!!')
    return all_countries_csv

def country_single_analysis(df, country):
    print('printing country data to csv file...')
    df = df.loc[df['Country'] == country]
    print('Filtering country data... please wait a few seconds...')
    single_country_csv = df.to_csv('./data/results/country_analysis.csv', index=False)
    print('csv information with the country of your choice generated!')
    return single_country_csv

def analysis(df, country):
    if country == 'all':
        csv_generation = all_country_to_csv(df)
    else:
        csv_generation = country_single_analysis(df, country)
    print('End of analysis...')
    return csv_generation