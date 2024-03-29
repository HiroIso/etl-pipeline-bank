# Import libraries
import requests
from bs4 import BeautifulSoup
import pandas as pd
import sqlite3
import numpy as np
from datetime import datetime

# Set display options to show all rows and columns
pd.set_option('display.max_rows', None)  # Show all rows
pd.set_option('display.max_columns', None)  # Show all columns


# Initialize variables

url = 'https://web.archive.org/web/20230908091635%20/https://en.wikipedia.org/wiki/List_of_largest_banks'
csv_path = 'https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMSkillsNetwork-PY0221EN-Coursera/labs/v2/exchange_rate.csv'
table_attribs = ['Name', 'MC_USD_Billion']
output_path = './Largest_banks_data.csv'
db_name = 'Banks.db'
table_name = 'Largest_banks'
log_file = 'code_log.txt'


# Code for ETL operations on Country-GDP data

# 1. Logging function
def log_progress(message):
    ''' This function logs the mentioned message of a given stage of the
    code execution to a log file. Function returns nothing'''
    timestamp_format = '%Y-%h-%d-%H:%M:%S'
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open('code_log.txt' , 'a') as f:
        f.write(f"{timestamp}: {message}" + '\n')        

def extract(url, table_attribs):
    ''' This function aims to extract the required
    information from the website and save it to a data frame. The
    function returns the data frame for further processing. '''
    # Extract the webpage as text
    html_page = requests.get(url).text

    # Parse the text into an html object
    data = BeautifulSoup(html_page, 'html.parser')

    # Create an empty dataframe witht columns
    df = pd.DataFrame(columns = table_attribs)

    # Extarct all attributes
    tables = data.find_all('tbody')
    rows = tables[0].find_all('tr')
    for row in rows:
        if row.find('td') is not None:
            col = row.find_all('td')
            bank_name = col[1].find_all('a')[1]['title']
            market_cap = col[2].contents[0][:-1]
            data_dict = {"Name": bank_name,
                         "MC_USD_Billion": float(market_cap)}
            df1 = pd. DataFrame(data_dict, index=[0])
            df = pd.concat([df,df1], ignore_index=True)

    return df


def transform(df, csv_path):
    ''' This function accesses the CSV file for exchange rate
    information, and adds three columns to the data frame, each
    containing the transformed version of Market Cap column to
    respective currencies'''

    # Create a dictionaly from exchange_rate.csv file
    exchange_rate = pd.read_csv(csv_path)
    exchange_rate = exchange_rate.set_index('Currency').to_dict()['Rate']

    # Add 3 another currency columns and scaled by the corresponding echange rate factor (round with 2 decimal places).
    df['MC_GBP_Billion'] = [np.round(x*exchange_rate['GBP'],2) for x in df['MC_USD_Billion']]
    df['MC_EUR_Billion'] = [np.round(x*exchange_rate['EUR'],2) for x in df['MC_USD_Billion']]
    df['MC_INR_Billion'] = [np.round(x*exchange_rate['INR'],2) for x in df['MC_USD_Billion']]

    return df


def load_to_csv(df, output_path):
    ''' This function saves the final data frame as a CSV file in
    the provided path. Function returns nothing.'''
    df.to_csv(output_path)


def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final data frame to a database
    table with the provided name. Function returns nothing.'''
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)


def run_query(query_statement, sql_connection):
    ''' This function runs the query on the database table and
    prints the output on the terminal. Function returns nothing. '''
    print(query_statement)
    query_output  = pd.read_sql(query_statement, sql_connection)
    print(query_output)

''' Here, you define the required entities and call the relevant
functions in the correct order to complete the project. Note that this
portion is not inside any function.'''

# Declaring known values
log_progress('Preliminaries complete. Initiating ETL process')


# Call extract() function
df = extract(url, table_attribs)
log_progress('Data extraction complete. Initiating Transformation process')


# Call transform() function
df = transform(df, csv_path)
log_progress('Data transformation complete. Initiating Loading process')


# Call load_to_csv()
load_to_csv(df, output_path)
log_progress('Data saved to CSV file')


# Initiate SQLite3 connection
sql_connection = sqlite3.connect(db_name)
log_progress('SQL Connection initiated')


# Call load_to_db()
load_to_db(df, sql_connection, table_name)
log_progress('Data loaded to Database as a table, Executing queries')


# Call run_query()
# 1. Print the contents of the entire table
query_statement1 = f"SELECT * FROM Largest_banks"
run_query(query_statement1, sql_connection)

# 2. Print the average market capitalization of all the banks in Billion USD.
query_statement2 = f"SELECT AVG(MC_GBP_Billion) FROM Largest_banks"
run_query(query_statement2, sql_connection)

# 3. Print only the names of the top 5 banks
query_statement3 = f"SELECT Name FROM Largest_banks LIMIT 5"
run_query(query_statement3, sql_connection)

log_progress('Process Complete')


# Close SQLite3 connection
sql_connection.close()
log_progress('Server Connection closed')
             

