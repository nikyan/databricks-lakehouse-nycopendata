# Delta Architecture: Poc using NYC Open Data
## Udacity: Data Engineering Capstone Project

## Summary
This project is focused on evaluating delta architecture and how continuous data flow model can be used in practice. In the current POC, I have created processed and analytics delta table. The analytics table can be leveraged by data scientist/business analyst for further analysis.

The project follows the following steps:

 - Step 1: Scope the Project and Gather Data
 - Step 2: Explore and Assess the Data
 - Step 3: Define the Data Model
 - Step 4: Run ETL to Model the Data
 - Step 5: Complete Project Write Up

## Step 1: Scope the project and gather data

### Scope
The project will read data from NYC Open Data API. Create a delta table using raw data. Transform and join data and finally create an analytics delta table.
The analytics delta table can be used Data Scientist to create predictive models.

I am currently storing data in cluster driver but the data can be stored in S3 bucket or Azure Data Lake. The delta table scales easily on an object store like data lakes. The delta tables allows for many features and takes away the need for expensive data warehouse solutions.

### Data used

 - 2020 Yellow Taxi Trip Data
The data consists of daily taxi trips with pickup and dropoff locations and trip costs. The data can used to predict taxi usage at each location or revenue forecasting. The data can be accessed by SOCRATA API which is used by NYC Open Data to publish dataset.
https://data.cityofnewyork.us/Transportation/2020-Yellow-Taxi-Trip-Data/kxp8-n2sj

 - Location Dimension Data
This is meta data that consists of location data. This can be used to identify borough and zone for each pickup and dropoff location.

More details about the data tables are available in the Data Dictionary:
https://github.com/nikyan/databricks-lakehouse-nycopendata/blob/main/Data_Dictionary.xlsx

### Tools used

 - Azure Databricks: Use PySpark to create ETL Notebook.


## Step2: Explore and assess data

- Explore data
1. The data from the API is clean. 
2. Perform data quality checks. Check if API returned any data. Check for nulls in the data.
3. Add raw data to delta table.

- Cleansing steps
1. Apply consumption schema before creating analytics delta table.
2. Join taxi trip data and location dimension data.



## Step 3: Define Data Model
Since the end purpose of the ETL pipeline is to support Machine Learning use case, it's more useful to have one fact table so that data scientists can easily create their models without having to do any complex joins.

The taxi trip data is joined with location dimension table to provide full view of the trip data which can used for number of ML use cases.

## Step 4: ETL Notebook

### ETL Pipeline

The ETL Notebook is a Databricks Notebook. It's exported as a Jupyter Notebook in the repo here:
https://github.com/nikyan/databricks-lakehouse-nycopendata/blob/main/ETL%20Process%20and%20Data%20Exploration.ipynb

The Notebook follows the following steps:
1. Parametrize Notebook: Use widgets to parameterize notebooks. Makes it easy to execute using different variables.
2. Import dependencies. This includes configuration file and utilities notebook.
3. Read data from the API.
4. Perform data quality checks.
5. Create a Delta table to store raw data from API.
6. Enforce schema
7. Join fact and dimension tables and create an analytics delta table.

### Folder Structure

-- ETL Process and Data Exploration.py
-- configuration.py (store API keys, location etc)
-- utilities.py (functions used in ETL)

## Step 5: Conclusion

### Tools and Technologies
- Databricks (PySpark) for compute and storage

### Data Update Frequency
- The ETL Notebook can be executed at any frequency. Best is to execute daily using Notebook parameter which can query the data for any day from the API.
- The lookup table can be read once unless there is a change.
- After first time creation of delta tables, the batch job should run in append-only mode.

### Future Design Considerations

The Notebook needs to be optmized for batch processing.

1. The data was increased by 100x.
Databricks is PAS service that can be easily scaled horizontally. It allows for setup of min and max nodes in the cluster with auto-scale feature. Databricks is available in all major cloud platforms.

2. The data populates a dashboard that must be updated on a daily basis by 7am every day.
Use Databrick's Jobs feature to execute ETL Notebook before dashboard update.

3. The database needed to be accessed by 100+ people.
Delta tables are designed to scale with data and users. It has number of features which allows for faster and efficient queries such as data indexing, data skipping, caching and compaction.
More details available here:
https://caserta.com/data-blog/databricks-delta/

### References

https://github.com/databricks/tech-talks 

