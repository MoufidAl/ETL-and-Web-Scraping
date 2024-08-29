import sqlite3
import pandas as pd 

conn = sqlite3.connect('STAFF.db')

table_name = 'INSTRUCTORS'
attr_list = ['ID', 'FNAME', 'LNAME', 'CITY', 'CCODE']

file_path = r'C:\Users\moufi\OneDrive\Python IBM project\db code\INSTRUCTOR.csv'
df = pd.read_csv(file_path, names=attr_list)

df.to_sql(table_name, conn, if_exists='replace', index = False)

# Running SQL Queries in Python

query_statement = f'SELECT * FROM {table_name}'  # Think here, you can do any query for any column and aggregate functions too
query_output = pd.read_sql(query_statement, conn)
print (query_statement)
print(query_output)

# Appending Data to the database

# Let us say we have the dictonary

data_dict = {'ID': [100], 'FNAME': ['John'], 'LNAME' : ['Doe'], 'CITY' : ['Paris'], 'CCODE': ['FR']}

# we append this data to a dataframe 

data_append = pd.DataFrame(data_dict)

# now we append to the sql table 

data_append.to_sql(table_name, conn, if_exists='replace', index=False)

conn.close()
