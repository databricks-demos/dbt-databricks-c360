# Databricks notebook source
# MAGIC %md
# MAGIC ## 3.1: Load the model from MlFlow & predict Churn
# MAGIC
# MAGIC The last step of our workflow will load the model our DS team created. 
# MAGIC
# MAGIC Our final gold table `dbt_c360_gold_churn_predictions` containing the model prediction will be available for us to start building a BI Analysis and take actions to reduce churn.
# MAGIC
# MAGIC *Note that the churn model creation is outside of this demo scope - we'll create a dummy one. You can install `dbdemos.install('lakehouse-retail-c360')` to get a real model.*

# COMMAND ----------

dbutils.widgets.text("catalog", "main", "Catalog")
dbutils.widgets.text("schema", "dbdemos_dbt_retail", "Schema")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

# COMMAND ----------

# DBTITLE 1,Model creation steps (model creation is usually done directly with AutoML)
from mlflow import MlflowClient
from databricks import automl
import mlflow
from pyspark.sql.functions import struct
import os 
from mlflow.store.artifact.models_artifact_repo import ModelsArtifactRepository
import mlflow.pyfunc

# We'll limit to 10 000 row to accelerate the learning process
input_pdf = spark.table(f"{catalog}.{schema}.dbt_c360_gold_churn_features").limit(10000).toPandas()

# Keep only the valuable column
input_pdf = input_pdf.drop(
    columns=[
        "email",
        "creation_date",
        "last_activity_date",
        "firstname",
        "lastname",
        "address",
        "last_transaction",
        "last_event",
    ]
)
# Display the input dataframe
display(input_pdf)

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from datetime import datetime

model_name = "churn-prediction-dbt"
model_full_name = f"{catalog}.{schema}.{model_name}"

mlflow.autolog(disable=False)
model_version_uri = f"models:/{model_full_name}@Champion"
mlflow.set_registry_uri('databricks-uc')

try:
    local_path = ModelsArtifactRepository(model_version_uri).download_artifacts("") # download model from remote registry
except Exception as e:
  print("Model doesn't exist "+str(e)+", will create a automl experirement for the demo.")

  from databricks import automl
  xp_path = "/Shared/dbdemos/experiments/dbt"
  xp_name = f"churn_prediction_dbt_{datetime.now().strftime('%Y-%m-%d_%H:%M:%S')}"
  summary = automl.classify(
      experiment_name = xp_name,
      experiment_dir = xp_path,
      dataset = input_pdf,
      target_col = "churn",
      timeout_minutes = 5
  )
  r = mlflow.register_model(
      model_uri=f"runs:/{summary.best_trial.mlflow_run_id}/model",
      name=model_full_name
  )
  client = mlflow.tracking.MlflowClient()
  client.set_registered_model_alias(name=model_full_name, alias="Champion", version=r.version)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.2: Load the model as a SQL Function

# COMMAND ----------

local_path = ModelsArtifactRepository(model_version_uri).download_artifacts("")
requirements_path = os.path.join(local_path, "requirements.txt")
if not os.path.exists(requirements_path):
  dbutils.fs.put("file:" + requirements_path, "", True)

# COMMAND ----------

# MAGIC %pip install -r $requirements_path

# COMMAND ----------

import mlflow
from pyspark.sql.functions import struct
predict = mlflow.pyfunc.spark_udf(spark, model_version_uri, result_type="double") #, env_manager="conda"

# COMMAND ----------

spark.udf.register("predict_churn", predict)

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {catalog}.{schema}.dbt_c360_gold_churn_predictions
AS 
SELECT predict_churn(struct(user_id, age_group, canal, country, gender, order_count, total_amount, total_item, platform, event_count, session_count, days_since_creation, days_since_last_activity, days_last_event)) as churn_prediction, * 
FROM {catalog}.{schema}.dbt_c360_gold_churn_features
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.3: Examine the churn prediction results!

# COMMAND ----------

display(spark.table(f"{catalog}.{schema}.dbt_c360_gold_churn_predictions"))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Next step: Leverage inferences and automate actions to increase revenue
# MAGIC
# MAGIC ## Automate action to reduce churn based on predictions
# MAGIC
# MAGIC We now have an end 2 end data pipeline analizing and predicting churn. We can now easily trigger actions to reduce the churn based on our business:
# MAGIC
# MAGIC - Send targeting email campaign to the customer the most likely to churn
# MAGIC - Phone campaign to discuss with our customers and understand what's going
# MAGIC - Understand what's wrong with our line of product and fixing it
# MAGIC
# MAGIC These actions are out of the scope of this demo and simply leverage the Churn prediction field from our ML model.
# MAGIC
# MAGIC ## Track churn impact over the next month and campaign impact
# MAGIC
# MAGIC Of course, this churn prediction can be re-used in our dashboard to analyse future churn and measure churn reduction. 
# MAGIC
# MAGIC install the `lakehouse-retail-c360` demo for more example.
# MAGIC
# MAGIC <img width="800px" src="https://raw.githubusercontent.com/QuentinAmbard/databricks-demo/main/retail/resources/images/lakehouse-retail/lakehouse-retail-churn-dbsql-prediction-dashboard.png">
