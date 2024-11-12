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

catalog = "dbdemos"
schema = "`dbt-retail`"
table = "dbt_c360_gold_churn_features"

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
input_pdf = spark.table(f"{catalog}.{schema}.{table}").limit(10000).toPandas()

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

dir_path = "/Shared/dbdemos/dbt"
model_name = "03-churn-prediction-dbt"
model_version_uri = f"models:/{catalog}.dbt-retail.{model_name}@Champion"

mlflow.autolog(disable=False)
#force the experiment to the field demos one. Required to launch as a batch
def init_experiment_for_batch(path, experiment_name):
  #You can programatically get a PAT token with the following
  pat_token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
  url = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().get()
  import requests
  requests.post(f"{url}/api/2.0/workspace/mkdirs", headers = {"Accept": "application/json", "Authorization": f"Bearer {pat_token}"}, json={ "path": path})
  xp = os.path.join(path, experiment_name)
  print(f"Using common experiment under {xp}")
  mlflow.set_experiment(xp)
  
# try:
#   init_experiment_for_batch(dir_path, model_name)
# except Exception as e:
#   print("directory already exists " +str(e))

model_version_uri = f"models:/{catalog}.{schema}.{model_name}@Champion"

try:
    local_path = ModelsArtifactRepository(model_version_uri).download_artifacts("") # download model from remote registry
except Exception as e:
  print("Model doesn't exist "+str(e)+", will create a automl experirement for the demo.")

  summary = automl.classify(
      input_pdf,
      target_col="churn",
      timeout_minutes=5,
      experiment_name=model_name,
      experiment_dir=dir_path
  )  # Perform classification and predict churn

  mlflow.set_registry_uri("databricks-uc")
  r = mlflow.register_model(
      model_uri=f"runs:/{summary.best_trial.mlflow_run_id}/model",
      name=f"{catalog}.dbt-retail.{model_name}"
  )
  client = mlflow.tracking.MlflowClient()
  client.set_registered_model_alias(name=f"{catalog}.dbt-retail.{model_name}", alias="Champion", version=r.version)
  local_path = ModelsArtifactRepository(model_version_uri).download_artifacts("")


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

import mlflow
from pyspark.sql.functions import struct
predict = mlflow.pyfunc.spark_udf(spark, model_version_uri, result_type="double") #, env_manager="conda"

# COMMAND ----------

spark.udf.register("predict_churn", predict)

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TEMPORARY VIEW dbt_c360_gold_churn_predictions
AS 
SELECT predict_churn(struct(user_id, age_group, canal, country, gender, order_count, total_amount, total_item, platform, event_count, session_count, days_since_creation, days_since_last_activity, days_last_event)) as churn_prediction, * 
FROM {catalog}.{schema}.dbt_c360_gold_churn_features
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.3: Examine the churn prediction results!

# COMMAND ----------

display(spark.sql(f"""
SELECT 
  user_id,
  platform,
  country,
  firstname,
  lastname,
  churn_prediction
FROM {catalog}.{schema}.dbt_c360_gold_churn_predictions
LIMIT 10;
"""))

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
