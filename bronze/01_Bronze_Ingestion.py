# Databricks notebook source
# MAGIC %md
# MAGIC # 1. Bronze Layer (Data Ingestion)
# MAGIC Reads raw files from Databricks Volumes.

# COMMAND ----------

import os
from pyspark.sql.functions import current_timestamp

catalog = "workspace"
schema = "default"
volume = "raw_real_estate"
table_prefix = f"{catalog}.{schema}.real_estate"
bronze_table = f"{table_prefix}_bronze"

print(f"Bronze Table: {bronze_table}")

# COMMAND ----------

# Note: The raw Parquet files (80 files) were originally loaded iteratively using Pandas 
# to bypass memory limits and schema mismatch errors in Spark.
# Since the Bronze table `workspace.default.real_estate_bronze` is already fully populated, 
# this notebook simply loads the existing table for preview.

df_bronze = spark.read.table(bronze_table)
display(df_bronze)