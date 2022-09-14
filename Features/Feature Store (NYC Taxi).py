# Databricks notebook source
# MAGIC %md
# MAGIC # Feature Store
# MAGIC Based on the [Featore Store txi example dataset notebook](https://docs.microsoft.com/en-us/azure/databricks/_static/notebooks/machine-learning/feature-store-taxi-example.html) from Databrics public documentation.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Inputs

# COMMAND ----------

dbutils.widgets.combobox(name='first_time_execution', choices=['No', 'Yes'], defaultValue='No', label='Is First Run (Create Tables for the First Time)')

# COMMAND ----------

first_time_execution = dbutils.widgets.get('first_time_execution')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Imports

# COMMAND ----------

from databricks import feature_store

from pyspark.sql.functions import *
from pyspark.sql.types import *

from pytz import timezone

from datetime import datetime

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load the data

# COMMAND ----------

raw_data = spark.read \
    .format('delta') \
    .load('dbfs:/databricks-datasets/nyctaxi-with-zipcodes/subsampled')

# COMMAND ----------

display(raw_data.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Feature Selection
# MAGIC We'll compute two groups of features based on trip pickup and drop-off zip codes:
# MAGIC 
# MAGIC **Pickup Features**
# MAGIC 1. Count of trips (time window = 1 hour, sliding window = 15 minutes)
# MAGIC 2. Mean fare amount (time window = 1 hour, sliding window = 15 minutes)
# MAGIC 
# MAGIC **Dropoff Features**
# MAGIC 1. Count of trips (time window = 30 minutes)
# MAGIC 2. Does trip end of the weekend (custom feature using python code)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper Functions
# MAGIC But first, we need the helper functions...

# COMMAND ----------

@udf(returnType=IntegerType())
def is_weekend(dt):
    tz = 'America/New_York'
    return int(dt.astimezone(timezone(tz)).weekday() >= 5) # 5 = Saturday, 7 = Sunday

# COMMAND ----------

@udf(returnType=StringType())
def partition_id(dt):
    # datetime -> "YYYY-MM"
    return f'{dt.year:04d}-{dt.month:02d}'

# COMMAND ----------

def filter_df_by_ts(df, ts_column, start_date, end_date):
    if ts_column and start_date:
        df = df.filter(col(ts_column) >= start_date)
    if ts_column and end_date:
        df = df.filter(col(ts_column) < end_date)
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ## Code to Compute Features

# COMMAND ----------

def pickup_features_fn(df, ts_column, start_date, end_date):
    """
    Computes the pickup_features feature group.
    To restrict features to a time range, pass in ts_column, start_date, and/or end_date as kwargs.
    """
    df = filter_df_by_ts(
        df, ts_column, start_date, end_date
    )
    
    pickupzip_features = (
        df.groupBy(
            'pickup_zip', window('tpep_pickup_datetime', '1 hour', '15 minutes')
        ) # 1 hour window, sliding every 15 minutes
        .agg(
            mean('fare_amount').alias('mean_fare_window_1h_pickup_zip'),
            count('*').alias('count_trips_window_1h_pickup_zip')
        )
        .select(
            col('pickup_zip').alias('zip'),
            unix_timestamp(col('window.end')).alias('ts').cast(IntegerType()),
            partition_id(to_timestamp(col('window.end'))).alias('yyyy_mm'),
            col('mean_fare_window_1h_pickup_zip').cast(FloatType()),
            col('count_trips_window_1h_pickup_zip').cast(IntegerType()),
        )
    )
    
    return pickupzip_features

# COMMAND ----------

def dropoff_features_fn(df, ts_column, start_date, end_date):
    """
Computes the dropoff_features feature group.
To restrict features to a time range, pass in ts_column, start_date, and/or end_date as kwargs.
    """
    df = filter_df_by_ts(
        df, ts_column, start_date, end_date
    )
    
    dropoffzip_features = (
        df.groupBy(
            'dropoff_zip', window('tpep_dropoff_datetime', '30 minutes')
        ) # 30 minute window
        .agg(
            count('*').alias('count_trips_window_30m_dropoff_zip')
        ).select(
            col('dropoff_zip').alias('zip'),
            unix_timestamp(col('window.end')).alias('ts').cast(IntegerType()),
            partition_id(to_timestamp(col('window.end'))).alias('yyyy_mm'),
            col('count_trips_window_30m_dropoff_zip').cast(IntegerType()),
            is_weekend(col('window.end')).alias('dropoff_is_weekend'),
        )
    )
    
    return dropoffzip_features

# COMMAND ----------

pickup_features = pickup_features_fn(
    raw_data, ts_column='tpep_pickup_datetime', start_date=datetime(2016, 1, 1), end_date=datetime(2016, 1, 31)
)

# COMMAND ----------

display(pickup_features.limit(10))

# COMMAND ----------

dropoff_features = dropoff_features_fn(
    raw_data, ts_column='tpep_dropoff_datetime', start_date=datetime(2016, 1, 1), end_date=datetime(2016, 1, 31)
)

# COMMAND ----------

display(dropoff_features.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Feature Store Tables

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS feature_store_taxi;

# COMMAND ----------

fs = feature_store.FeatureStoreClient()

# COMMAND ----------

spark.conf.set('spark.sql.shuffle.partitions', '5')

# COMMAND ----------

if (first_time_execution == 'Yes'):
    fs.create_table(
        name='feature_store_taxi.trip_pickup_features',
        primary_keys=['zip', 'ts'],
        df=pickup_features,
        partition_columns='yyyy_mm',
        description='Taxi Fares. Pickup Features',
    )
else:
    print('Not first run, skipping table creation')

# COMMAND ----------

if (first_time_execution == 'Yes'):
    fs.create_table(
        name='feature_store_taxi.trip_dropoff_features',
        primary_keys=['zip', 'ts'],
        df=dropoff_features,
        partition_columns='yyyy_mm',
        description='Taxi Fares. Dropoff Features',
    )
else:
    print('Not first run, skipping table creation')

# COMMAND ----------

fs.write_table(
    name = 'feature_store_taxi.trip_pickup_features',
    df = pickup_features,
    mode='merge',
)

# COMMAND ----------

fs.write_table(
    name = 'feature_store_taxi.trip_dropoff_features',
    df = dropoff_features,
    mode='merge',
)

# COMMAND ----------


