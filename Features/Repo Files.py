# Databricks notebook source
# MAGIC %md
# MAGIC # Repo Files
# MAGIC Based on [Work with notebooks and other files in Databricks Repos](https://docs.databricks.com/repos/work-with-notebooks-other-files.html) documentation

# COMMAND ----------

# MAGIC %md
# MAGIC ## Imports

# COMMAND ----------

import os

from pyspark.sql.functions import *

# COMMAND ----------

if os.getcwd() == '/databricks/driver':
  raise Exception('Workspace does not support working with files in repos')

# COMMAND ----------

df = spark.read.csv(f'file://{os.getcwd()}/repo-files/simple.csv', header=True)

# COMMAND ----------

display(df)

# COMMAND ----------

img = spark.read.format('image').load(f'file://{os.getcwd()}/repo-files/serious-image.png')

# COMMAND ----------

display(img)

# COMMAND ----------

os.listdir('repo-files')
