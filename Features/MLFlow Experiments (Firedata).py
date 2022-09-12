# Databricks notebook source
# MAGIC %md
# MAGIC # MLFlow Experiments
# MAGIC Based on Chapter 3: Practicing ML in Databricks from the book [Optimizing Databricks Workloads](https://www.amazon.com/Optimizing-Databricks-Workloads-performance-workloads/dp/1801819076)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Imports & Init
# MAGIC Let's import all the libraries we'll need. We need `os` to pass the `Force Dataset Download` widget parameter to the bash script thta downloads the data. The rest are either standard `pyspark.sql`, `pyspark.ml` or `mlflow` imports.

# COMMAND ----------

# DBTITLE 1,Imports
import os

from pyspark.sql.functions import *

from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer
from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

import mlflow
import mlflow.spark
from mlflow.tracking import MlflowClient

# COMMAND ----------

# DBTITLE 1,Initialize the variables with the path to the data
firedata_base_dir = 'tmp/firedata'
firedata_file_name = 'fire_incidents.csv'
firedata_dir = f'dbfs:/{firedata_base_dir}'
firedata_incidents = f'{firedata_dir}/{firedata_file_name}'

os.environ['FIREDATA_BASE_DIR'] = firedata_base_dir
os.environ['FIREDATA_FILE_NAME'] = firedata_file_name

# COMMAND ----------

# DBTITLE 1,Initialize the Force Dataset Download widget
dbutils.widgets.dropdown(choices=['Yes', 'No'], defaultValue='No', name='force_download', label='Force Dataset Download')

# COMMAND ----------

# DBTITLE 1,Make the widget value available to the bash script
force_download = dbutils.widgets.get('force_download')
os.environ['FORCE_DOWNLOAD'] = force_download

# COMMAND ----------

# MAGIC %md
# MAGIC ## Download the Data
# MAGIC Download the data if it doesn't exist or if `Force Dataset Download` is set to `Yes`.

# COMMAND ----------

# MAGIC %sh
# MAGIC FIREDATA_DIR=/dbfs/${FIREDATA_BASE_DIR}
# MAGIC FIREDATA_INCIDENTS=${FIREDATA_DIR}/${FIREDATA_FILE_NAME}
# MAGIC 
# MAGIC mkdir -p $FIREDATA_DIR
# MAGIC 
# MAGIC if [[ ! -f "$FIREDATA_INCIDENTS" || "$FORCE_DOWNLOAD" == "Yes" ]]; then
# MAGIC   wget https://data.sfgov.org/api/views/nuek-vuh3/rows.csv?accessType=DOWNLOAD -O $FIREDATA_INCIDENTS
# MAGIC fi

# COMMAND ----------

# DBTITLE 1,Show dataset file and file size
# MAGIC %sh ls -alh /dbfs/tmp/firedata

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load the Data

# COMMAND ----------

# DBTITLE 1,Specify the schema
schema_str = """CallNumber INTEGER,
                    UnitID STRING,
                    IncidentNumber INTEGER,
                    CallType STRING,
                    CallDate STRING,
                    WatchDate STRING,
                    ReceivedDtTm STRING,
                    EntryDtTm STRING,
                    DispatchDtTm STRING,
                    ResponseDtTm STRING,
                    OnSceneDtTm STRING,
                    TransportDtTm STRING,
                    HospitalDtTm STRING,
                    CallFinalDisposition STRING,
                    AvailableDtTm STRING,
                    Address STRING,
                    City STRING,
                    ZipcodeofIncident INTEGER,
                    Battalion STRING,
                    StationArea STRING,
                    Box STRING,
                    OriginalPriority STRING,
                    Priority STRING, 
                    FinalPriority INTEGER,
                    ALSUnit BOOLEAN,
                    CallTypeGroup STRING,
                    NumberofAlarms INTEGER,
                    UnitType STRING,
                    Unitsequenceincalldispatch INTEGER,
                    FirePreventionDistrict STRING,
                    SupervisorDistrict STRING,
                    NeighborhoodDistrict STRING,
                    Location STRING,
                    RowID STRING
"""

# COMMAND ----------

# DBTITLE 1,Load the data
fire_incidents = (spark
                  .read
                  .format('csv')
                  .schema(schema_str)
                  .option('header', True)
                  .load(f'{firedata_dir}/fire_incidents.csv')
                 )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prepare the Data

# COMMAND ----------

# DBTITLE 1,Parse the dates and times
spark.conf.set('spark.sql.legacy.timeParserPolicy', 'LEGACY')

pattern1 = 'MM/dd/yyyy'
pattern2 = 'MM/dd/yyyy hh:mm:ss aa'

fire_incidents = (fire_incidents
                  .withColumn('CallDate', unix_timestamp('CallDate', pattern1).cast('timestamp'))
                  .withColumn('WatchDate', unix_timestamp('WatchDate', pattern1).cast('timestamp'))
                  .withColumn('ReceivedDtTm', unix_timestamp('ReceivedDtTm', pattern2).cast('timestamp'))
                  .withColumn('EntryDtTm', unix_timestamp('EntryDtTm', pattern2).cast('timestamp'))
                  .withColumn('DispatchDtTm', unix_timestamp('DispatchDtTm', pattern2).cast('timestamp'))
                  .withColumn('ResponseDtTm', unix_timestamp('ResponseDtTm', pattern2).cast('timestamp'))
                  .withColumn('OnSceneDtTm', unix_timestamp('OnSceneDtTm', pattern2).cast('timestamp'))
                  .withColumn('TransportDtTm', unix_timestamp('TransportDtTm', pattern2).cast('timestamp'))
                  .withColumn('HospitalDtTm', unix_timestamp('HospitalDtTm', pattern2).cast('timestamp'))
                  .withColumn('AvailableDtTm', unix_timestamp('AvailableDtTm', pattern2).cast('timestamp'))
)

# COMMAND ----------

# DBTITLE 1,Show a sample of the final dataset
display(fire_incidents.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Select the Features
# MAGIC 
# MAGIC Per the book, we'll predict `fire_incidents.ALSUnit` using the following features:
# MAGIC * `CallType`
# MAGIC * `StationArea`
# MAGIC * `FinalPriority`
# MAGIC * `NumberOfAlarms`
# MAGIC * `UnitType` (Categorical)
# MAGIC * `ResponseDtTm` (derived from `DispatchDtTm` - `ReceivedDtTm`)

# COMMAND ----------

selectionDf = (
    fire_incidents.select([
        'CallType',
        col('StationArea').cast('int'),
        'FinalPriority',
        'NumberOfAlarms',
        'UnitType',
        'DispatchDtTm',
        (unix_timestamp('DispatchDtTm') - unix_timestamp('ReceivedDtTm')).alias('ResponseDtTm'),
        col('ALSUnit').cast('int')
    ])
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate the Training & Test Datasets

# COMMAND ----------

seed = 16
training, test = selectionDf.randomSplit([0.80, 0.20], seed)

training_count = training.count()
test_count = test.count()

print(f'Training Count: {training_count:,.2f} | Test Count: {test_count:,.2f}')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Train the Model (using MLFlow)

# COMMAND ----------

# DBTITLE 1,Specify the run name
run_name = 'Data partitioned Randomly 80/20 Seed 16'

# COMMAND ----------

# DBTITLE 1,Train the Model
with mlflow.start_run(run_name=run_name) as run:
    mlflow.autolog()
    
    indexerCallType = StringIndexer(inputCol='CallType', outputCol='CallTypeIndex', handleInvalid='skip')
    indexerUnitType = StringIndexer(inputCol='UnitType', outputCol='UnitTypeIndex', handleInvalid='skip')
    assembler = VectorAssembler(inputCols=['CallTypeIndex', 'UnitTypeIndex', 'StationArea', 'FinalPriority', 'ResponseDtTm'], outputCol='features', handleInvalid='skip')
    dt = DecisionTreeClassifier(maxDepth=3, labelCol='ALSUnit', featuresCol='features')
    pipeline = Pipeline(stages=[indexerCallType, indexerUnitType, assembler, dt])
    
    model = pipeline.fit(training)
    predictions = model.transform(test)
    
    evaluator = MulticlassClassificationEvaluator(metricName='accuracy', predictionCol='prediction', labelCol='ALSUnit')
    accuracy = evaluator.evaluate(predictions)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Query MLFlow Metadata

# COMMAND ----------

notebook_path = dbutils.entry_point.getDbutils().notebook().getContext().notebookPath().get()

experiment_id = mlflow.get_experiment_by_name(notebook_path).experiment_id
experiment_data = spark.read.format('mlflow-experiment').load(experiment_id)

experiment_data.createOrReplaceTempView('mlflow_experiments')

# COMMAND ----------

display(experiment_data)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM mlflow_experiments
# MAGIC ORDER BY start_time DESC
# MAGIC LIMIT 1
