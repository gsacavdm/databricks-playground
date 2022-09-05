# Databricks notebook source
# MAGIC %md
# MAGIC This is my first notebook

# COMMAND ----------

# MAGIC %md
# MAGIC Now an edit

# COMMAND ----------

import os

from pyspark.sql.functions import *

# COMMAND ----------

df = spark.read.csv(f'file://{os.getcwd()}/random-files/simple.csv', header=True)

# COMMAND ----------

display(df)

# COMMAND ----------

img = spark.read.format('image').load(f'file://{os.getcwd()}/random-files/serious-image.png')

# COMMAND ----------

display(img)

# COMMAND ----------

os.listdir('random-files')

# COMMAND ----------


