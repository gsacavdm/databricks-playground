# Databricks notebook source
import dlt

from pyspark.sql.functions import *

# COMMAND ----------

@dlt.table(
    comment='Bike sharing counts aggregated on an hourly basis'
)
def bike_sharing_hourly():
    df = spark.read.csv('dbfs:/databricks-datasets/bikeSharing/data-001/hour.csv', header=True, inferSchema=True)
    return df.withColumn('atempx2', df.atemp * 2)

# COMMAND ----------

@dlt.table(
    comment='Bike sharing counts aggregated on a daily basis'
)
def bike_sharing_daily():
    df = dlt.read('bike_sharing_hourly')
    return df.groupby('dteday').agg(
        avg('temp').alias('temp'),
        avg('atemp').alias('atemp'),
        avg('hum').alias('hum'),
        avg('windspeed').alias('windspeed'), 
        sum('cnt').alias('cnt')
    )
