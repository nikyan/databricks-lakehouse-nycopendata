# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC Database configuration

# COMMAND ----------

taxi_data = f"/nycopendata/taxi-data/"

# COMMAND ----------

spark.sql(f"CREATE DATABASE IF NOT EXISTS nycopendata")
spark.sql(f"USE nycopendata")

# COMMAND ----------

# MAGIC %md
# MAGIC Socrata Credentials

# COMMAND ----------

app_token = 'rWjClpvZRgli4YuSk9xnnm56D'

# COMMAND ----------

# MAGIC %md Import Utility Functions

# COMMAND ----------

# MAGIC %run ./utilities
