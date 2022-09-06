# Databricks notebook source
# MAGIC %md
# MAGIC # FileStore
# MAGIC Based on [FileStore documentation](https://docs.databricks.com/dbfs/filestore.html)

# COMMAND ----------

import requests
import os
import pandas as pd

# COMMAND ----------

# MAGIC %md
# MAGIC Lets start by storing some data in the FileStore

# COMMAND ----------

dbutils.fs.put('/FileStore/my-file.txt', 'Hello World', overwrite=True)

# COMMAND ----------

# MAGIC %md
# MAGIC Now lets build the URL to get to the file:

# COMMAND ----------

hostName = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('browserHostName')
url = f'https://{hostName}/files/my-file.txt'

display(url)

# COMMAND ----------

# MAGIC %md
# MAGIC Browse to the URL above from your browser and you'll download the file.
# MAGIC 
# MAGIC Note that the calls only workw when authenticated.
# MAGIC A simple, unauthenticated `requests.get` doesn't work:

# COMMAND ----------

request = requests.get(url)
content = request.content

display(content)

# COMMAND ----------

# MAGIC %md
# MAGIC The `displayHTML` call is authenticated though, and thus works:

# COMMAND ----------

displayHTML('<object data="files/my-file.txt"')

# COMMAND ----------

# MAGIC %md
# MAGIC Now lets try with an image

# COMMAND ----------

image = requests.get('https://github.com/gsacavdm/databricks-playground/raw/master/random-files/serious-image.png').content

with open('/dbfs/FileStore/serious-image.png', 'wb') as f:
    f.write(image)


# COMMAND ----------

print(f'https://{hostName}/files/serious-image.png')

# COMMAND ----------

# MAGIC %md Show using Python

# COMMAND ----------

displayHTML('<img src="files/serious-image.png"/>')

# COMMAND ----------

# MAGIC %md
# MAGIC Show using Markdown
# MAGIC 
# MAGIC ![serious-image](/files/serious-image.png)

# COMMAND ----------

# MAGIC %md
# MAGIC To wrap up, let's call the DBFS API:

# COMMAND ----------

token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().getOrElse(None)

# COMMAND ----------

url = f'https://{hostName}/api/2.0/dbfs/list'
headers = {'Authorization' : f'Bearer {token}'}
           
files = requests.get(
        url,
        headers=headers,
        json={'path': '/FileStore'}
).json()
display(files)

# COMMAND ----------

df = pd.DataFrame(files['files'])
display(df)
