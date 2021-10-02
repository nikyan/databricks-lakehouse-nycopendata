# Databricks notebook source
!pip install sodapy

from pyspark.sql.session import SparkSession
from sodapy import Socrata
import pandas as pd


def create_client(app_token):
  '''
  Create Socrata client to point to API
  
  Input: API Secret Key
  Output: Client object that points to the API
  '''
  client = Socrata(domain = "data.cityofnewyork.us", app_token = app_token) 
  return client


def get_data(client, dataset_identifier, **kwargs):
  '''
    Retrieves data through an API call and creates a Spark Dataframe
    
    Input:
      - Client: Socrata client object that points to the API
      - dataset_identifier: Unique data set identifier at NYC Open Data
      - **kwargs: Keyword arguments that can be passed to the Socrata API. API allows for following keyword arguments:
              select : the set of columns to be returned, defaults to *
              where : filters the rows to be returned, defaults to limit
              order : specifies the order of results
              group : column to group results on
              limit : max number of results to return, defaults to 1000
              offset : offset, used for paging. Defaults to 0
              q : performs a full text search for a value
              query : full SoQL query string, all as one parameter
              exclude_system_fields : defaults to true. If set to false, the
                  response will include system fields (:id, :created_at, and
                  :updated_at)
                  
    Output: Spark Dataframe
  '''
  
  df = spark.createDataFrame(client.get(dataset_identifier, content_type = "json", **kwargs))
  
  return df
  
  
lookup_table = "https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv"
def get_lookup_data(url = lookup_table):
  '''
  Get location dimenstion table
  Input: URL
  Output: Dataframe
  '''
  
  df = pd.read_csv(url)
  return spark.createDataFrame(df)


def process_taxi_data(df: DataFrame) -> DataFrame:
  '''
  Apply Schema to the fact table.
  Input: Dataframe
  Output: New Dataframe with Schema applied and ordered columns
  '''
  return (
    df
    .withColumn("vendorid", col("vendorid").cast("integer"))
    .withColumn("tpep_pickup_datetime", to_timestamp(df.tpep_pickup_datetime, "yyyy-MM-dd'T'HH:mm:ss.SSS"))
    .withColumn("tpep_dropoff_datetime", to_timestamp(df.tpep_dropoff_datetime, "yyyy-MM-dd'T'HH:mm:ss.SSS"))
    .withColumn("trip_distance", col("trip_distance").cast("integer"))
    .withColumn("pulocationid", col("pulocationid").cast("integer"))
    .withColumn("dolocationid", col("dolocationid").cast("integer"))
    .withColumn("ratecodeid", col("ratecodeid").cast("integer"))
    .withColumn("store_and_fwd_flag", col("store_and_fwd_flag").cast("string"))
    .withColumn("passenger_count", col("passenger_count").cast("integer"))
    .withColumn("payment_type", col("payment_type").cast("integer"))
    .withColumn("fare_amount", col("fare_amount").cast("double"))
    .withColumn("extra", col("extra").cast("double"))
    .withColumn("mta_tax", col("mta_tax").cast("double"))
    .withColumn("improvement_surcharge", col("improvement_surcharge").cast("double"))
    .withColumn("congestion_surcharge", col("congestion_surcharge").cast("double"))
    .withColumn("tip_amount", col("tip_amount").cast("double"))
    .withColumn("tolls_amount", col("tolls_amount").cast("double"))
    .withColumn("total_amount", col("total_amount").cast("double"))
    .select("vendorid", "tpep_pickup_datetime", "tpep_dropoff_datetime", \
            "trip_distance", "pulocationid", "dolocationid", "ratecodeid", \
            "store_and_fwd_flag", "passenger_count", "payment_type", "fare_amount", \
            "extra", "mta_tax", "improvement_surcharge", "congestion_surcharge", \
            "tip_amount", "tolls_amount", "total_amount")
    )

# Data Quality Checks
def row_check(df):
  '''
  Check if API returned greater than 0 rows
  Input:
  Output: True or False
  '''
  return df.count() > 0


def null_check(df):
  '''
  Check for nans and nulls in the dataframe
  '''
  return df.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in df.columns]).show()

