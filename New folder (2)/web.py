import requests
import sqlite3
import pandas as pd
from bs4 import BeautifulSoup

# Initialization of Known Entities

url = 'https://web.archive.org/web/20230902185655/https://en.everybodywiki.com/100_Most_Highly-Ranked_Films'
db_name = 'Movies.db'
table_name = 'Top_50'
csv_path = r'C:\Users\moufi\OneDrive\Python IBM project\Web scraping\Internet_data.csv'
df = pd.DataFrame(columns=["Average Rank","Film","Year"])
count = 0

# the requests library will load websites as HTML documents.

html_page = requests.get(url).text
data = BeautifulSoup(html_page, "html.parser")

tables = data.find_all("tbody")
rows = tables[0].find_all('tr')

# Iterating over the rows to get the required data

for row in rows:
    if count < 50:
        col = row.find_all('td')
        if len(col) != 0:
            data_dict = {'Average Rank': col[0].contents[0],
                        'Film': col[1].contents[0],
                        'Year': col[2].contents[0]}
            df1 = pd.DataFrame(data_dict, index= [0])
            df = pd.concat([df, df1], ignore_index=True)
            count += 1
    else:
        break

print(df)

# Send to a CSV

df.to_csv()

# create a database and send data to it

conn = sqlite3.connect(db_name)
df.to_sql(table_name,conn, if_exists='replace', index=False)
conn.close()