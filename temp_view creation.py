# Databricks notebook source
display(dbutils.fs.ls("dbfs:/FileStore/tables/daiwt_mlops/"))

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS ibm_telco_churn;
# MAGIC USE SCHEMA ibm_telco_churn;

# COMMAND ----------

file_location = "dbfs:/FileStore/tables/daiwt_mlops/Telco_customer_churn.csv"

df = spark.read.format("csv")\
      .option("inferSchema", "true")\
      .option("header", "true")\
      .option("sep", ",")\
      .load(file_location)

display(df)

# COMMAND ----------

display(df.describe())

# COMMAND ----------

df.groupBy("Churn Label").count().show()

# COMMAND ----------


df2 = df.withColumnRenamed('CustomerID', 'customerID')\
      .withColumnRenamed('Count', 'count')\
      .withColumnRenamed('Country', 'country')\
      .withColumnRenamed('State', 'state')\
      .withColumnRenamed('City', 'city')\
      .withColumnRenamed('Zip Code', 'zipCode')\
      .withColumnRenamed('Lat Long', 'latLong')\
      .withColumnRenamed('Latitude', 'latitude')\
      .withColumnRenamed('Longitude', 'longitude')\
      .withColumnRenamed('Gender', 'gender')\
      .withColumnRenamed('Partner', 'partner')\
      .withColumnRenamed('Dependents', 'dependents')\
      .withColumnRenamed('Tenure Months', 'tenureMonths')\
      .withColumnRenamed('Phone Service', 'phoneService')\
      .withColumnRenamed('Multiple Lines', 'multipleLines')\
      .withColumnRenamed('Internet Service', 'internetService')\
      .withColumnRenamed('online Security', 'onlineSecurity')\
      .withColumnRenamed('Online Backup', 'onlineBackup')\
      .withColumnRenamed('Device Protection', 'deviceProtection')\
      .withColumnRenamed('Tech Support', 'techSupport')\
      .withColumnRenamed('Streaming TV', 'streamingTV')\
      .withColumnRenamed('Streaming Movies', 'streamingMovies')\
      .withColumnRenamed('Contract', 'contract')\
      .withColumnRenamed('Paperless Billing', 'paperlessBilling')\
      .withColumnRenamed('Payment Method', 'paymentMethod')\
      .withColumnRenamed('Monthly Charges', 'monthlyCharges')\
      .withColumnRenamed('Total Charges', 'totalCharges')\
      .withColumnRenamed('Churn Label', 'churnString')\
      .withColumnRenamed('Churn Value', 'churnValue')\
      .withColumnRenamed('Churn Score', 'churnScore')\
      .withColumnRenamed('Churn Reason', 'churnReason')\
      .drop('Senior Citizen', 'churnValue')

display(df2)

# COMMAND ----------

df3 = df2.select('customerId', 'count', 'country', 'State', 'city', 'zipCode', 'latLong', 'latitude', 'longitude', 'gender')
display(df3)

# COMMAND ----------

# df3 = df2.select('gender', 'partner', 'dependents','phoneService', 'multipleLines', 'internetService','onlineSecurity', 'onlineBackup', 'deviceProtection','techSupport', 'streamingTV', 'streamingMovies','contract', 'paperlessBilling', 'paymentMethod', 'churnString')
# display(df3)

# COMMAND ----------

# df2.createOrReplaceTempView('ibm_telco_churn.bronze_customers')

# df2.columns

# COMMAND ----------

dbutils.fs.rm('/user/hive/warehouse/ibm_telco_churn.db', True)

# COMMAND ----------

# writing the file into DBFS

df2.write.option('path','/user/hive/warehouse/ibm_telco_churn.db').saveAsTable('bronze_customers')

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM ibm_telco_churn.bronze_customers

# COMMAND ----------

import mlflow

# Set an experiment name, which must be unique and case-sensitive.
experiment = mlflow.set_experiment('/Shared/daiwt_mlops/prod/telco_churn_experiment_prod')

# Get Experiment Details
print("Experiment_id: {}".format(experiment.experiment_id))
print("Artifact Location: {}".format(experiment.artifact_location))
print("Tags: {}".format(experiment.tags))
print("Lifecycle_stage: {}".format(experiment.lifecycle_stage))

# COMMAND ----------

display(dbutils.fs.ls('/Shared/daiwt_mlops/prod'))

# COMMAND ----------

import mlflow

# Set an experiment name, which must be unique and case-sensitive.
experiment = mlflow.set_experiment("Social NLP Experiments")

# Get Experiment Details
print("Experiment_id: {}".format(experiment.experiment_id))
print("Artifact Location: {}".format(experiment.artifact_location))
print("Tags: {}".format(experiment.tags))
print("Lifecycle_stage: {}".format(experiment.lifecycle_stage))

# COMMAND ----------


