# Databricks notebook source
dbutils.widgets.dropdown("reset_all_data", "false", ["true", "false"], "Reset all data")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE CATALOG IF NOT EXISTS `dbdemos`;
# MAGIC CREATE SCHEMA IF NOT EXISTS `dbdemos`.`dbt-retail`;
# MAGIC
# MAGIC CREATE VOLUME IF NOT EXISTS `dbdemos`.`dbt-retail`.`orders`;
# MAGIC CREATE VOLUME IF NOT EXISTS `dbdemos`.`dbt-retail`.`users`;
# MAGIC CREATE VOLUME IF NOT EXISTS `dbdemos`.`dbt-retail`.`events`

# COMMAND ----------

reset_all_data = dbutils.widgets.get("reset_all_data") == "true"
raw_data_location = "Volumes/dbdemos/dbt-retail"
folder = "/Volumes/dbdemos/dbt-retail"

#Return true if the folder is empty or does not exists
def is_folder_empty(folder):
  try:
    return len(dbutils.fs.ls(folder)) == 0
  except:
    return True

if reset_all_data or is_folder_empty(folder+"/orders") or is_folder_empty(folder+"/users") or is_folder_empty(folder+"/events"):
  #data generation on another notebook to avoid installing libraries (takes a few seconds to setup pip env)
  print(f"Generating data under {folder} , please wait a few secs...")
  path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
  parent_count = path[path.rfind("01-ingest-autoloader"):].count('/') - 1
  prefix = "./" if parent_count == 0 else parent_count*"../"
  prefix = f'{prefix}_resources/'
  dbutils.notebook.run(prefix+"01-load-data", 600)
else:
  print("Data already exists. Run with reset_all_data=true to force a data cleanup for your local demo.")
