# Databricks notebook source
dbutils.widgets.dropdown("reset_all_data", "false", ["true", "false"], "Reset all data")

# COMMAND ----------

reset_all_data = dbutils.widgets.get("reset_all_data") == "true"
raw_data_location = "dbdemos/dbt-retail"
folder = "/dbdemos/dbt-retail"

#Return true if the folder is empty or does not exists
def is_folder_empty(folder):
  try:
    return len(dbutils.fs.ls(folder)) == 0
  except:
    return True

if reset_all_data or is_folder_empty(folder+"/orders") or is_folder_empty(folder+"/users") or is_folder_empty(folder+"/events"):
  #data generation on another notebook to avoid installing libraries (takes a few seconds to setup pip env)
  print(f"Generating data under {folder} , please wait a few sec...")
  dbutils.notebook.run("./01-load-data", 600)
else:
  print("data already existing. Run with reset_all_data=true to force a data cleanup for your local demo.")
