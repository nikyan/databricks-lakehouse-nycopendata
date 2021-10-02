# Databricks notebook source
# MAGIC %md
# MAGIC ## Lakehouse Architecture using NYC Yellow Taxi Data
# MAGIC 
# MAGIC Steps:
# MAGIC 1. Parametrize Notebook: Use widgets to parameterize notebooks. Makes it easy to execute using different variables.
# MAGIC 2. Import dependencies. This includes configuration file and utilities notebook.
# MAGIC 3. Read data from the API.
# MAGIC 4. Perform data quality checks.
# MAGIC 5. Create a Delta table to store raw data from API.
# MAGIC 6. Enforce schema 
# MAGIC 7. Join fact and dimension tables and create an analytics delta table.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### 1. Parameterize Notebook

# COMMAND ----------

#dbutils.widgets.removeAll()

# COMMAND ----------

dbutils.widgets.dropdown("date", "2020-09-21", ["2020-09-21", "2020-09-22", "2020-09-23", "2020-09-24"])

# COMMAND ----------

batch_date = dbutils.widgets.get("date")
print(batch_date)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### 2. Import Dependencies

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %run ./includes/configuration

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Read data

# COMMAND ----------

# Create API client object
# NYC Open Data uses Socrata Open Data API to publish datasets. 
client = create_client(app_token)

# COMMAND ----------

# Using get method to extract data from the API
client.get?

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3.a. Fact Table

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Get 2020 Yellow Taxi data from NYC Open Data
# MAGIC - https://data.cityofnewyork.us/Transportation/2020-Yellow-Taxi-Trip-Data/kxp8-n2sj
# MAGIC - These records are generated from the trip record submissions made by yellow taxi Technology Service Providers (TSPs). 
# MAGIC - Each row represents a single trip in a yellow taxi in 2020. The trip records include fields capturing pick-up and drop-off dates/times, pick-up and drop-off locations, trip distances, itemized fares, rate types, payment types, and driver-reported passenger counts.

# COMMAND ----------

# MAGIC %md
# MAGIC  - Create a dynamic query to extract data from API.
# MAGIC  - Use Notebook parameter to dynamically update API query to execute for certain date.

# COMMAND ----------

# create a dynamic query using Notebook parameter. The notebook can be executed daily as a batch job with parameter updated for each run.
query = f"':created_at' > '{batch_date}'"

# COMMAND ----------

# The get_data function in utilities.py uses **kwargs to provide optional keywords to filter data. 
# This allows the function to be dynamic and can used for multiplte use cases.
taxi_df = get_data(client, "kxp8-n2sj", exclude_system_fields = False, limit=100, where = query)

# COMMAND ----------

display(taxi_df)

# COMMAND ----------

# Look at the Schema
taxi_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Data Quality Check:

# COMMAND ----------

assert row_check(taxi_df), "API did not return any data"
print("Assertion Passed.")

# COMMAND ----------

# check for nans and nulls in the dataframe
null_check(taxi_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3.b. Zone and Borough Lookup Table

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Get Lookup Table to Augment data
# MAGIC  - The table is available as a CSV file on a webpage
# MAGIC  - The lookup table contains borough and zone information
# MAGIC  - The lookup table can be joined with Taxi data using LocationID 

# COMMAND ----------

lookup_table = get_lookup_data()

# COMMAND ----------

display(lookup_table)

# COMMAND ----------

assert row_check(lookup_table), "API did not return any data"
print("Assertion Passed.")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### 4. Write data to a Delta table

# COMMAND ----------

# MAGIC %md
# MAGIC - To make the Notebook Idempotent, delete table first
# MAGIC - Register the Delta table in the Metastore

# COMMAND ----------

dbutils.fs.rm(taxi_data + "processed", recurse=True)

spark.sql(
    f"""
DROP TABLE IF EXISTS taxi_data_processed
"""
)

# COMMAND ----------

(taxi_df.write
 .mode("overwrite")
 .format("delta")
 .save(taxi_data + "processed"))

# COMMAND ----------

#Register the Table in the Metastore
spark.sql(f"""
DROP TABLE IF EXISTS taxi_data_processed
""")

spark.sql(f"""
CREATE TABLE taxi_data_processed
USING DELTA
LOCATION "{taxi_data}/processed" 
""")

# COMMAND ----------

taxi_data_processed = spark.read.table("taxi_data_processed")

# COMMAND ----------

assert taxi_data_processed.count() == taxi_df.count(), "Row count does not match between raw and processed."
print("Assertion passed.")

# COMMAND ----------

dbutils.fs.rm(taxi_data + "dim_location", recurse=True)

spark.sql(
    f"""
DROP TABLE IF EXISTS dim_location
"""
)

# COMMAND ----------

(lookup_table.write
 .mode("overwrite")
 .format("delta")
 .save(taxi_data + "dim_location"))

# COMMAND ----------

#Register the Table in the Metastore
spark.sql(f"""
DROP TABLE IF EXISTS dim_location
""")

spark.sql(f"""
CREATE TABLE dim_location
USING DELTA
LOCATION "{taxi_data}/dim_location" 
""")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### 5. Transform Data

# COMMAND ----------

processed_taxi_df = (spark.read
                     .format("delta")
                     .load(file_path + "/processed"))

# COMMAND ----------

# MAGIC %md
# MAGIC Apply schema to the extracted data.

# COMMAND ----------

# apply schema
processed_taxi_df = process_taxi_data(processed_taxi_df)

# COMMAND ----------

processed_taxi_df.printSchema()

# COMMAND ----------

taxi_df.head(1)

# COMMAND ----------

dim_location = (spark.read
                     .format("delta")
                     .load(file_path + "/dim_location"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6. Create Table for Analytics

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Join Taxi fact table and location dimension table to create a table for analytics.

# COMMAND ----------

analytics_taxi_df = processed_taxi_df.join(dim_location, processed_taxi_df.pulocationid == dim_location.LocationID)

# COMMAND ----------

# MAGIC %md
# MAGIC Create a new Delta table for analytics dataframe that can be used by downstream users and apps.

# COMMAND ----------

dbutils.fs.rm(taxi_data + "taxi_analytics", recurse=True)

spark.sql(
    f"""
DROP TABLE IF EXISTS taxi_analytics
"""
)

# COMMAND ----------

(analytics_taxi_df.write
 .mode("overwrite")
 .format("delta")
 .save(taxi_data + "taxi_analytics"))

# COMMAND ----------

#Register the Table in the Metastore
spark.sql(f"""
DROP TABLE IF EXISTS taxi_analytics
""")

spark.sql(f"""
CREATE TABLE taxi_analytics
USING DELTA
LOCATION "{taxi_data}/taxi_analytics" 
""")

# COMMAND ----------

(spark.read
 .format("delta")
 .load(file_path + "taxi_analytics")
 .display())
