# Databricks notebook source
# MAGIC %md
# MAGIC ### 1.1: Load the raw data using Databricks Autoloader

# COMMAND ----------

users_sample = spark.read.json('/demos/retail/churn/users/part-00000-tid-3774691364401406604-10e6ac3b-76a9-4727-9f77-3546911f8265-756-1-c000.json')
orders_sample = spark.read.json('/demos/retail/churn/orders/part-00000-tid-4377310562469993626-d03a7579-e891-45dc-b552-a16cd330c2af-440-1-c000.json')
users_schema = users_sample.schema
orders_schema = orders_sample.schema

# COMMAND ----------

users_df = (spark.readStream
          .format("cloudFiles")
          .option("cloudFiles.format", "json")
          .option("cloudFiles.inferColumnTypes", "true")
          .schema(users_schema)
          .load('/demos/retail/churn/users/')
)

orders_df = (spark.readStream
          .format("cloudFiles")
          .option("cloudFiles.format", "json")
          .option("cloudFiles.inferColumnTypes", "true")
          .schema(orders_schema)
          .load('/demos/retail/churn/orders/')
)

# COMMAND ----------

clickstream_df = (
  spark.readStream
       .format("cloudFiles")
       .option("cloudFiles.format", "csv")
       .option("cloudFiles.format", "csv")
       .option("cloudFiles.schemaLocation", "/demos/retail/churn/events-schema/")
       .load('/demos/retail/churn/events')
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.2: Write the data as Delta Lake Tables

# COMMAND ----------

(users_df.writeStream
         .format("delta")
         .option("checkpointLocation", "/demos/retail/churn/users-checkpoint/")
         .trigger(availableNow = True)
         .outputMode("append")
         .toTable("field_eng_dbt_demo.dbt_c360.t1_bronze_users")
)

# COMMAND ----------

(orders_df.writeStream
         .format("delta")
         .option("checkpointLocation", "/demos/retail/churn/orders-checkpoint/")
         .trigger(availableNow = True)
         .outputMode("append")
         .toTable("field_eng_dbt_demo.dbt_c360.t1_bronze_orders")
)

# COMMAND ----------

(clickstream_df.writeStream
         .format("delta")
         .option("checkpointLocation", "/demos/retail/churn/clickstream-checkpoint/")
         .trigger(availableNow = True)
         .outputMode("append")
         .toTable("field_eng_dbt_demo.dbt_c360.t1_bronze_events")
)
