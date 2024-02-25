# ETL pipeline for GDP data

This is a practice project from IBM Data Engineering Professional Certificate course. I have created a complete ETL pipeline for accessing data from a website and processing it to meet the requirements. 

### Project Scenario:

This project is to create an automated script that can extract the list of all countries in order of their GDPs in billion USDs (rounded to 2 decimal places), as logged by the International Monetary Fund (IMF). Since IMF releases this evaluation twice a year, this code will be used by the organization to extract the information as it is updated.

**GDP data source source URL**:
https://web.archive.org/web/20230902185326/https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29


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

