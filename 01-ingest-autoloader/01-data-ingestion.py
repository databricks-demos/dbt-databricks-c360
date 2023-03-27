# Databricks notebook source
# MAGIC %md
# MAGIC # 1.1: Load the raw data using Databricks Autoloader
# MAGIC 
# MAGIC Our first step is to extract messages from external system into our Lakehouse.
# MAGIC 
# MAGIC This is typically done consuming a message queue (kafka), or files being uploaded in a blob storage in an incremental fashion.
# MAGIC 
# MAGIC We want to be able to ingest the new data so that our dbt pipeline can do the remaining steps.
# MAGIC 
# MAGIC In this example, we'll consume files from a blob storage. However we could easily have consume from any other system like a kafka queue.
# MAGIC 
# MAGIC We'll be using Databricks Autoloader (`cloudFile` format) to incrementally load new data and append them to our raw tables. Re-running this job will only consume new data, handling all schema inference, evolution and scalability for us. 
# MAGIC 
# MAGIC For more details on Autoloader, install `dbdemos.install('auto-loader')`
# MAGIC 
# MAGIC <img width="1px" src="https://www.google-analytics.com/collect?v=1&gtm=GTM-NKQ8TT7&tid=UA-163989034-1&cid=555&aip=1&t=event&ec=field_demos&ea=display&dp=%2F42_field_demos%2Ffeatures%2Fdbt%2Fnotebook_01&dt=FEATURE_DBT" />

# COMMAND ----------

# MAGIC %run ./_resources/00-setup $reset_all_data=true

# COMMAND ----------

# DBTITLE 1,Incrementally ingest all folders
def incrementally_ingest_folder(path, format, table):
    (spark.readStream
              .format("cloudFiles")
              .option("cloudFiles.format", format)
              .option("cloudFiles.inferColumnTypes", "true")
              .option("cloudFiles.schemaLocation", f"/dbdemos/dbt-retail/_schemas/{table}")
              .load(path)
           .writeStream
              .format("delta")
              .option("checkpointLocation", f"/dbdemos/dbt-retail/_checkpoints/{table}")
              .trigger(availableNow = True)
              .outputMode("append")
              .toTable(table))

spark.sql('create database if not exists mchan_dbt_demo_db;')
incrementally_ingest_folder('/dbdemos/dbt-retail/users', 'json', 'mchan_dbt_demo_db.t1_bronze_users')
incrementally_ingest_folder('/dbdemos/dbt-retail/orders', 'json', 'mchan_dbt_demo_db.t1_bronze_orders')
incrementally_ingest_folder('/dbdemos/dbt-retail/events', 'csv', 'mchan_dbt_demo_db.t1_bronze_events')

print('Congrats, our new data has been consumed and incrementally added to our bronze tables')

# COMMAND ----------

# MAGIC %md
# MAGIC ### END OF STEP 1: INGESTION 
