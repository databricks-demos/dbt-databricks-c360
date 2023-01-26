# Databricks notebook source
# MAGIC %md
# MAGIC ## 3.1: Load the model from MlFlow

# COMMAND ----------

from mlflow.store.artifact.models_artifact_repo import ModelsArtifactRepository
import os

model_name = "dbdemos_customer_churn"
model_uri = f"models:/{model_name}/10"
local_path = ModelsArtifactRepository(model_uri).download_artifacts("") # download model from remote registry

requirements_path = os.path.join(local_path, "requirements.txt")
if not os.path.exists(requirements_path):
  dbutils.fs.put("file:" + requirements_path, "", True)

# COMMAND ----------

# MAGIC %pip install -r $requirements_path

# COMMAND ----------

# MAGIC %pip install jinja2==3.0.3

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.2: Load the model as a SQL Function

# COMMAND ----------

# redefining key variables here because %pip and %conda restarts the Python interpreter
model_name = "dbdemos_customer_churn"
model_uri = f"models:/{model_name}/Production"
input_table_name = "field_eng_dbt_demo.dbt_c360.t3_gold_churn_features"
table = spark.table(input_table_name)

# COMMAND ----------

import mlflow
from pyspark.sql.functions import struct
predict = mlflow.pyfunc.spark_udf(spark, model_uri, result_type="double", env_manager="conda")

# COMMAND ----------

spark.udf.register("predict_churn", predict)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE t3_gold_churn_predictions
# MAGIC AS 
# MAGIC SELECT predict_churn(struct(user_id, age_group, canal, country, gender, order_count, total_amount, total_item, platform, event_count, session_count, days_since_creation, days_since_last_activity, days_last_event)) as churn_prediction, * FROM field_eng_dbt_demo.dbt_c360.t3_gold_churn_features

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.3: Examine the churn prediction results!

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   user_id,
# MAGIC   platform,
# MAGIC   country,
# MAGIC   firstname,
# MAGIC   lastname,
# MAGIC   churn_prediction
# MAGIC FROM t3_gold_churn_predictions
# MAGIC LIMIT 10;
