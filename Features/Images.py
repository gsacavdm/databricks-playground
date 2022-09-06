# Databricks notebook source
# MAGIC %md
# MAGIC # Image
# MAGIC Based on [Image documentation](https://docs.databricks.com/data/data-sources/image.html)

# COMMAND ----------

# MAGIC %md
# MAGIC First lets test loading and showing a single image from the Internet

# COMMAND ----------

flower = spark.read.format('image').load('/databricks-datasets/flower_photos/daisy/100080576_f52e8ee070_n.jpg')

# COMMAND ----------

display(flower)

# COMMAND ----------

flower.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC Now lets find ourselves a good image set from `/databricks-datasets`

# COMMAND ----------

# MAGIC %sh ls -R /dbfs/databricks-datasets/flower_photos/

# COMMAND ----------

flowers = spark.read.format('image').load('/databricks-datasets/flower_photos/**/*.jpg')

# COMMAND ----------

display(flowers.limit(5))

# COMMAND ----------

display(flowers.limit(5).select('image.origin'))

# COMMAND ----------

# MAGIC %md
# MAGIC Now trying out what sample notebook included in the documentation
