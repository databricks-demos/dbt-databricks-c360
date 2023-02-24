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

# DBTITLE 1,Model creation steps (model creation is usually done directly with AutoML)
from mlflow.store.artifact.models_artifact_repo import ModelsArtifactRepository
import os
import mlflow.pyfunc
import mlflow
from mlflow import MlflowClient
import random
mlflow.autolog(disable=True)
#force the experiment to the field demos one. Required to launch as a batch
def init_experiment_for_batch(path, experiment_name):
  #You can programatically get a PAT token with the following
  pat_token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
  url = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().get()
  import requests
  requests.post(f"{url}/api/2.0/workspace/mkdirs", headers = {"Accept": "application/json", "Authorization": f"Bearer {pat_token}"}, json={ "path": path})
  xp = f"{path}/{experiment_name}"
  print(f"Using common experiment under {xp}")
  mlflow.set_experiment(xp)
  

init_experiment_for_batch("/Shared/dbdemos/dbt", "03-churn-prediction")


try:
    model_name = "dbdemos_churn_dbt_model"
    model_uri = f"models:/{model_name}/Production"
    local_path = ModelsArtifactRepository(model_uri).download_artifacts("") # download model from remote registry
except Exception as e:
    print("Model doesn't exist "+str(e)+", will create a dummy one for the demo. Please install dbdemos.install('lakehouse-retail-c360') to get a real model")
    class dummyModel(mlflow.pyfunc.PythonModel):
        def predict(self, model_input):
            #Return a random value for the churn
            return model_input['user_id'].map(lambda x: random.randint(0, 1))
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

# MAGIC %pip install jinja2==3.0.3

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.2: Load the model as a SQL Function

# COMMAND ----------

import mlflow
from pyspark.sql.functions import struct
predict = mlflow.pyfunc.spark_udf(spark, f"models:/dbdemos_churn_dbt_model/Production", result_type="double") #, env_manager="conda"

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
