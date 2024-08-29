import glob
import pandas as pd
from bs4 import BeautifulSoup
import requests
import numpy as np
import sqlite3
from datetime import datetime

# Setting the constants

url = 'https://web.archive.org/web/20230902185326/https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29'
db_name = 'World_Economies.db'
table_name = 'Countries_by_GDP'
csv_path = r'C:\Users\Moufid_Alsaig\OneDrive - Dell Technologies\Desktop\New folder (2)\Project File\Countries_by_GDP.csv'
table_attribs = ['Country','GDP_USD_millions']
count = 0
query_statement = f"SELECT * from {table_name} WHERE GDP_USD_billions >= 100"

sql_connection = sqlite3.connect(db_name)

# Automation

def extract(url, table_attribs):
    ''' This function extracts the required
    information from the website and saves it to a dataframe. The
    function returns the dataframe for further processing. '''
    html_page= requests.get(url).text
    data = BeautifulSoup(html_page, 'html.parser')
    df = pd.DataFrame(columns=table_attribs)
    tables = data.find_all("tbody")
    rows = tables[2].find_all('tr')
    for row in rows:
        col = row.find_all('td')
        if len(col) != 0:
            if col[0].find('a') is not None and '—' not in col[2]:
                data_dict= {'Country': col[0].a.contents[0],
                            'GDP_USD_millions': col[2].contents[0]}
                df1 = pd.DataFrame(data_dict, index =[0])
                df = pd.concat([df, df1], ignore_index=True)
    return df

def transform(df):
    ''' This function converts the GDP information from Currency
    format to float value, transforms the information of GDP from
    USD (Millions) to USD (Billions) rounding to 2 decimal places.
    The function returns the transformed dataframe.'''
    GDP_list = df['GDP_USD_millions'].tolist()
    GDP_list = [float("".join(x.split(","))) for x in GDP_list]
    GDP_list = [np.round(x/1000, 2) for x in GDP_list]

    df['GDP_USD_millions'] = GDP_list
    df = df.rename(columns = {'GDP_USD_millions': 'GDP_USD_billions'})
    return df

def load_to_csv(df, csv_path):
    ''' This function saves the final dataframe as a `CSV` file 
    in the provided path. Function returns nothing.'''
    df.to_csv(csv_path)

def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final dataframe as a database table
    with the provided name. Function returns nothing.'''
    df.to_sql(table_name, sql_connection, if_exists = 'replace', index=False)

def run_query(query_statement, sql_connection):
    ''' This function runs the stated query on the database table and
    prints the output on the terminal. Function returns nothing. '''
    query_output = pd.read_sql(query_statement,sql_connection)
    print(query_output)

def log_progress(message):
    ''' This function logs the mentioned message at a given stage of the code execution to a log file. Function returns nothing'''
    timestamp_format = '%Y-%h-%d:%H-%M-%S'
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open('logging_file.txt', 'a') as f:
        f.write(timestamp +  ' : '+ message + '\n')

sql_connection.close()