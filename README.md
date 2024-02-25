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

Run the following command to execute the pipeline:


```
python3 etl_project_gdp.py
```


The ETL pipeline detail:

1. **Extracting information**: 
Using the web scraping process to extract information from the specified web page. 

2. **Transform information**:
It modifies the values in million USDs to billion USDs (rounded to 2 decimal places) as required.

3. **Loading information**:
It saves the transformed dataframe to "Countries_by_GDP.csv" file and the transformed dataframe as a table in the database "World_Economies.db".

4. **Querying the database table**:
It runs the query statement on the table and retrieve the output as a filtered dataframe. This dataframe will be printed.

5. **Logging progress**:
It will be called multiple times throughout the execution of this code and will add a log entry in "etl_project_log.txt" file.

6. **Execution function**:
It execute all the functions to process the pipeline.

