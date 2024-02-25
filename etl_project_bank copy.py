# Import libraries
import requests
from bs4 import BeautifulSoup
import pandas as pd
import sqlite3
import numpy as np
from datetime import datetime

# Code for ETL operations on Country-GDP data
url = 'https://web.archive.org/web/20230902185326/https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29'
table_attribs = ['Country',
                 'GDP_USD_millions']
db_name = 'World_Economies.db'
table_name = 'Countries_by_GDP'
csv_path = 'Countries_by_GDP.csv'


# ------ 1. Extracting information ------
def extract(url, table_attribs):
    ''' This function extracts the required
    information from the website and saves it to a dataframe. The
    function returns the dataframe for further processing. '''

    # Extract the web page as text
    html_page = requests.get(url).text

    # Parse the text into an HTML object
    data = BeautifulSoup(html_page, 'html.parser')

    # Create an empty DataFrame with columns as the table_attribs
    df = pd.DataFrame(columns = table_attribs)

    # Extract all 'tbody' attributes of the HTML object.
    # Then extract all the rows of the index 2 table using the 'tr attribute.
    tables = data.find_all('tbody')
    rows = tables[2].find_all('tr')

    # Check the contents of each row, having attribute 'td', for the following conditions.
    #  a. The row should not be empty.
    #  b. The first column should contain a hyperlink.
    #  c. The third column should not be '—'.
    # Then Store all entries matching the conditions a dictionary with keys the same as entries of table_attribs. 
    # Append all these dictionaries one by one to the dataframe.
    for row in rows:
        col = row.find_all('td')
        if len(col)!=0:
            if col[0].find('a') is not None and '—' not in col[2]:
                data_dict = {"Country": col[0].a.contents[0],
                             "GDP_USD_millions": col[2].contents[0]}
                df1 = pd.DataFrame(data_dict, index=[0])
                df = pd.concat([df,df1], ignore_index=True)

    return df


# ------ 2. Transform information ------
def transform(df):
    ''' This function converts the GDP information from Currency
    format to float value, transforms the information of GDP from
    USD (Millions) to USD (Billions) rounding to 2 decimal places.
    The function returns the transformed dataframe.'''

    # Create a list of the target column and onvert 'GDP_USD' values into float
    GDP_list = df["GDP_USD_millions"].tolist()
    GDP_list = [float("".join(x.split(','))) for x in GDP_list]
    # Divide all values by 1000 and round it to 2 decimal places
    GDP_list = [np.round(x/1000,2) for x in GDP_list]
    df["GDP_USD_millions"] = GDP_list
    # Modify the name of the column from 'GDP_USD_millions' to 'GDP_USD_billions'.
    df=df.rename(columns = {"GDP_USD_millions":"GDP_USD_billions"})

    return df


# ------ 3. Loading information ------

def load_to_csv(df, csv_path):
    ''' This function saves the final dataframe as a `CSV` file 
    in the provided path. Function returns nothing.'''
    df.to_csv(csv_path)


def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final dataframe as a database table
    with the provided name. Function returns nothing.'''
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)


# ------ 4. Querying the database table ------
def run_query(query_statement, sql_connection):
    ''' This function runs the stated query on the database table and
    prints the output on the terminal. Function returns nothing. '''
    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)


# ------ 5. Logging progress ------
def log_progress(message):
    ''' This function logs the mentioned message at a given stage of the code execution to a log file. Function returns nothing. '''
    timestamp_format = '%Y-%h-%d-%H:%M:%S'
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open('./etl_project_log.txt', 'a') as f:
        f.write(f"{timestamp}: {message}" + '\n')


# ------ 6. Execution function ------

def etl_pipeline_gdp():
    ''' All entities for the ETL pipleline. '''
    # Declaring known values
    log_progress("Preliminaries complete. Initiating ETL process.")

    # Call extract() function
    df = extract(url, table_attribs)
    log_progress("Data extraction complete. Initiating Transformation process.")


    # Call transform() function
    df = transform(df)
    log_progress("Data transformation complete. Initiating loading process.")


    # Call load_to_csv()
    load_to_csv(df, csv_path)
    log_progress("Data saved to CSV file.")


    # Initiate SQLite3 connection
    sql_connection = sqlite3.connect(db_name)
    log_progress("SQL Connection initiated.")


    # Call load_to_db()
    load_to_db(df, sql_connection, table_name)
    log_progress("Data loaded to Database as table. Running the query.")


    # Call run_query() *
    query_statement = f"SELECT * FROM {table_name} WHERE GDP_USD_billions >= 100"
    run_query(query_statement, sql_connection)
    log_progress("Process Complete.")


    # Close SQLite3 connection
    sql_connection.close()
    log_progress("-")


etl_pipeline_gdp()