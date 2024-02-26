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
table_attribs_add = ['MC_GBP_Billion', 'MC_EUR_Billion', 'MC_INR_Billion']
output_path = 'Largest_banks_data.csv'
db_name = 'Banks.db'
table_name = './Largest_banks'
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
        cols = row.find_all('td')
        for col in cols:
            a_tags = col.find_all('a')
            if len(cols) and len(a_tags) != 0:
                    data_dict = {'Name': a_tags[1].contents[0],
                                'MC_USD_Billion': cols[2].contents[0]}
                    df1 = pd.DataFrame(data_dict, index=[0])
                    df = pd.concat([df, df1], ignore_index=True)
                    df['MC_USD_Billion'] = df['MC_USD_Billion'].str.replace('\n', '')
    
    df['MC_USD_Billion'] = df['MC_USD_Billion'].astype(float)

    return df


def transform(df, csv_path):
    ''' This function accesses the CSV file for exchange rate
    information, and adds three columns to the data frame, each
    containing the transformed version of Market Cap column to
    respective currencies'''

    # Create a dictionaly from exchange_rate.csv file
    with open('exchange_rate.csv', 'r') as f:
        next(f)
        exchange_rate = {}
        for line in f:
            rate_list = line.split(',')
            rate_list[1] = float(rate_list[1])
            exchange_rate[rate_list[0]] = rate_list[1]

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

def run_query(query_statement, sql_connection):
    ''' This function runs the query on the database table and
    prints the output on the terminal. Function returns nothing. '''

''' Here, you define the required entities and call the relevant
functions in the correct order to complete the project. Note that this
portion is not inside any function.'''

# Declaring known values
# log_progress('Preliminaries complete. Initiating ETL process')

# Call extract() function
df = extract(url, table_attribs)
# log_progress('Data extraction complete. Initiating Transformation process')

# Call transform() function
df = transform(df, csv_path)
# print(df)
# print(f"The 5th largest bank in billion EUR: {df['MC_EUR_Billion'][4]}") #value for the finalquiz
# log_progress('Data transformation complete. Initiating Loading process')

# Call load_to_csv()
load_to_csv(df, output_path)
# log_progress('Data saved to CSV file')

# Initiate SQLite3 connection
# log_progress('SQL Connection initiated')

# Call load_to_db()
# log_progress('Data loaded to Database as a table, Executing queries')

# Call run_query()
# log_progress('Process Complete')

# Close SQLite3 connection
# log_progress('Server Connection closed')
             

