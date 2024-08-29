import pandas as pd
import requests
import numpy as np
from bs4 import BeautifulSoup
from datetime import datetime
import glob
import sqlite3

'''
project scenario: 
you have been hired as a data engineer by research organization.
Your boss has asked you to create a code that can be used to compile the list of the top 10 largest banks in the world ranked by market capitalization in billion USD. 
Further, the data needs to be transformed and stored in GBP, EUR and INR as well, in accordance with the exchange rate information that has been made available to you as a CSV file.
The processed information table is to be saved locally in a CSV format and as a database table.
Your job is to create an automated system to generate this information so that the same can be executed in every financial quarter to prepare the report.
'''

# Initialization of Known Entities
url = 'https://web.archive.org/web/20230908091635%20/https://en.wikipedia.org/wiki/List_of_largest_banks'
table_attrs = ['Name','MC_USD_Billion']
database_name = 'Banks.db'
table_name = 'Largest_Banks'
log_file = 'code_log.txt'

csv_path = r'C:\Users\Moufid_Alsaig\OneDrive - Dell Technologies\Desktop\New folder (2)\Project File\Larget_Banks_Data.csv'

def log_process(message):
    timestamp_format = '%Y-%h-%d: %H-%M-%S'
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open (log_file,'a') as f:
        f.write(timestamp + ' : ' + message + '\n' )

# Extracting Data

def extract_data(url, table_attrs):
    try:
        html_page = requests.get(url).text
        soup = BeautifulSoup(html_page, 'html.parser')

        # Find the first table on the page (assuming it's the one you want)
        table = soup.find('table', {'class': 'wikitable'})

        if table is None:
            print("No table found.")
            return pd.DataFrame(columns=table_attrs)

        # Extract the rows from the table
        rows = table.find_all('tr')

        # Initialize the list to store the data
        data = []

        for row in rows:
            cols = row.find_all('td')
            if len(cols) >= 3:  # Ensure there are enough columns
                name = cols[1].get_text(strip=True)  # Bank name in the second column
                mc_usd_billions = cols[2].get_text(strip=True)  # Market cap in the third column
                
                # Append the data to the list
                data.append({'Name': name, 'MC_USD_Billions': mc_usd_billions})

        # Convert the list of dictionaries to a DataFrame
        df = pd.DataFrame(data, columns=table_attrs)
        return df

    except Exception as e:
        print(f"An error occurred: {e}")
        return pd.DataFrame(columns=table_attrs)

# Define the columns you want in your DataFrame
table_attrs = ['Name', 'MC_USD_Billions']

# Tranforming Data

def transform_data(df):
    df['MC_EUR_Billions'] = round(df['MC_USD_Billions'] * 0.93,2)
    df['MC_GBP_Billions'] = round(df['MC_USD_Billions'] * 0.8,2)

    return df

def database_comm(database_name, conn, df):
    conn = sqlite3.connect(database_name)
    df.to_sql(table_name, conn, if_exists = 'replace', index = False)
    conn.close()

def read_query(query_statement, conn):
    conn = sqlite3.connect(database_name)
    output = pd.read_sql(query_statement, conn)
    print(output)






    
