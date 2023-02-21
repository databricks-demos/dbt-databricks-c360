# Databricks notebook source
# MAGIC %md
# MAGIC ## 3.1: Load the model from MlFlow

# COMMAND ----------

from mlflow.store.artifact.models_artifact_repo import ModelsArtifactRepository
import os
import mlflow.pyfunc
import mlflow
mlflow.autolog(disable=True)
from mlflow import MlflowClient

try:
    model_name = "dbdemos_churn_dbt_model"
    model_uri = f"models:/{model_name}/Production"
    local_path = ModelsArtifactRepository(model_uri).download_artifacts("") # download model from remote registry
except:
    print("Model doesn't exist, will create a dummy one for the demo. Please install dbdemos.install('lakehouse-retail-c360') to get a real model")
    class dummyModel(mlflow.pyfunc.PythonModel):
        def predict(self, context, model_input):
            return model_input.apply(lambda x: 1)
    model = dummyModel()
    with mlflow.start_run(run_name="dummy_model_for_dbt") as mlflow_run:
        m = mlflow.sklearn.log_model(model, "dummy_model")
    model_registered = mlflow.register_model(f"runs:/{ mlflow_run.info.run_id }/dummy_model", model_name)
    client = mlflow.tracking.MlflowClient()
    client.transition_model_version_stage(model_name, model_registered.version, stage = "Production", archive_existing_versions=True)

    local_path = ModelsArtifactRepository(model_uri).download_artifacts("")

requirements_path = os.path.join(local_path, "requirements.txt")
if not os.path.exists(requirements_path):
  dbutils.fs.put("file:" + requirements_path, "", True)

# COMMAND ----------

# MAGIC %pip install -r $requirements_path

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.2: Load the model as a SQL Function

# COMMAND ----------

# redefining key variables here because %pip and %conda restarts the Python interpreter
input_table_name = "dbdemos.dbt_c360_gold_churn_features"
table = spark.table(input_table_name)

# COMMAND ----------

import mlflow
from pyspark.sql.functions import struct
predict = mlflow.pyfunc.spark_udf(spark, model_uri, result_type="double", env_manager="conda")

# COMMAND ----------

spark.udf.register("predict_churn", predict)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE dbdemos.dbt_c360_gold_churn_predictions
# MAGIC AS 
# MAGIC SELECT predict_churn(struct(user_id, age_group, canal, country, gender, order_count, total_amount, total_item, platform, event_count, session_count, days_since_creation, days_since_last_activity, days_last_event)) as churn_prediction, * FROM dbdemos.dbt_c360_gold_churn_features

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
# MAGIC FROM dbdemos.dbt_c360_gold_churn_predictions
# MAGIC LIMIT 10;
