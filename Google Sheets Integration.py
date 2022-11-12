# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks Integration with Google Sheets
# MAGIC This notebook demonstrates how to integrate Databricks with Google Sheets

# COMMAND ----------

# MAGIC %md
# MAGIC ## Installs & Imports

# COMMAND ----------

# MAGIC %pip install --upgrade google-api-python-client google-auth-httplib2 google-auth-oauthlib

# COMMAND ----------

import os.path

import json
import datetime

from google.auth.transport.requests import Request
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC ## Input Widgets

# COMMAND ----------

dbutils.widgets.text('sheetId','', 'Google Sheet Id')
dbutils.widgets.text('rangeRead','A1:Z', 'Range to Read')
dbutils.widgets.text('cellWrite', 'A1', 'Cell to Write')

SHEET_ID = dbutils.widgets.get('sheetId')
RANGE_READ = dbutils.widgets.get('rangeRead')
CELL_WRITE = dbutils.widgets.get('cellWrite')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Auth with Google
# MAGIC For this to work you must have first created a GCP Service Account, created json keys for it and uploaded the json to Databricks with:
# MAGIC * `secret-scope` = `gcp-integration`
# MAGIC * `key` = `gcp-service-account-json`

# COMMAND ----------

service_account_bytes = dbutils.secrets.getBytes('gcp-integration','gcp-service-account-json')

# COMMAND ----------

info = json.loads(service_account_bytes.decode())

# COMMAND ----------

SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

credentials = service_account.Credentials.from_service_account_info(info, scopes=SCOPES)
service = build('sheets', 'v4', credentials=credentials)
sheet = service.spreadsheets()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Sheets
# MAGIC Note: You must have granted the service account access to read these sheets by sharing the sheets with the service account, the same way you'd share a sheet with any other user.

# COMMAND ----------

result = sheet.values().get(spreadsheetId=SHEET_ID, range=RANGE_READ).execute()
data = result.get('values', [])

if not data:
    raise Exception('No data found.')

# We'll assumevthe top-most row is the header
# and the remaining rows contain the data
data_columns = data[0]
data_rows = data[1:]

if not data_rows:
    # If there are no rows, then it's not a proper table.
    # It's just a single row of data, so let's just return the raw results
    data
else:
    # Otherwise, lets convert the table into a Spark dataframe. 
    # We need the code below to handle the fact that empty trailing columns are omitted (https://developers.google.com/sheets/api/samples/reading)
    # so we get rows with fewer columns which trips up Spark
    column_count = len(data_columns)
    data_rows = [row if len(row) == column_count else row + ['']*(column_count-len(row)) for row in data_rows]

    df = spark.createDataFrame(data_rows, schema=data_columns)

# COMMAND ----------

data

# COMMAND ----------

if df:
    display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to a Sheet

# COMMAND ----------

sheet.values().update(
    spreadsheetId = SHEET_ID,
    range=CELL_WRITE,
    valueInputOption='RAW',
    body={'range': CELL_WRITE, 'values': [[f"Hello from Databricks at {datetime.datetime.today()}"]]}).execute()

# COMMAND ----------


