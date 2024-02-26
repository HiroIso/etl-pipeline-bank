# ETL pipeline for the World's Largest Banks

### Project Scenario:
This is a project to develop an ETL (Extract, Transform, Load) pipeline geared towards compiling a list of the top 10 largest banks globally, ranked by market capitalization in billion USD. Additionally, it aims to transform and store this data into GBP, EUR, and INR currencies, leveraging exchange rate information available in CSV format. The processed data will be stored both locally as a CSV file and within a database table.

The goal is to establish an automated system capable of generating this vital information reliably, enabling to execute the process seamlessly every financial quarter to produce the requisite report.



**Data source URL**:
https://web.archive.org/web/20230908091635%20/https://en.wikipedia.org/wiki/List_of_largest_banks


To installed all required packages 

```
pip install -r requirements.txt
```

Run the command in the shell to download exchange_rate.csv file
```
wget https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMSkillsNetwork-PY0221EN-Coursera/labs/v2/exchange_rate.csv
```


Run the following command to execute the pipeline:


```
python3 banks_project.py
```


The ETL pipeline detail:

1. **Logging function**:
It will be called multiple times throughout the execution of the pipeline and will add a log entry in "code_log.txt.

2. **Extracting information**: 
Using the web scraping process to extract information from [the specified web page](https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks) and save it to a dataframe. 


3. **Transform information**:
Transform the dataframe by adding columns for Market Capitalization in GBP, EUR and INR, rounded to 2 decimal places, based on the exchange rate information in the **exchange_rate.csv** file.

4. **Loading information**:
It saves the transformed dataframe to **Largest_banks_data.csv** file.

5. **Loading to Database**:
It saves the transformed dataframe to an SQL database server as a table

6. **Querying the database table**:
It runs the query statement on the table and retrieve the output as a filtered dataframe. This dataframe will be printed.

7. **Execution**:
It execute all the functions to process the pipeline and the log entries will be stored at all stages.

