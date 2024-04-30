# Databricks notebook source
# MAGIC %md
# MAGIC # Data initialization notebook. 
# MAGIC Do not run outside of the main notebook. This will automatically be called based on the reste_all widget value to setup the data required for the demo.

# COMMAND ----------

# MAGIC %pip install Faker

# COMMAND ----------

#dbutils.widgets.text("raw_data_location", "/demos/retail/churn/", "Raw data location (stating dir)")
dbutils.widgets.dropdown("reset_all_data", "false", ["true", "false"], "Reset all data")
reset_all_data = dbutils.widgets.get("reset_all_data") == "true"

def cleanup_folder(path):
  #Cleanup to have something nicer
  for f in dbutils.fs.ls(path):
    if f.name.startswith('_committed') or f.name.startswith('_started') or f.name.startswith('_SUCCESS') :
      dbutils.fs.rm(f.path)

# COMMAND ----------

folder = "/Volumes/dbdemos/dbt-retail"
if reset_all_data:
  print("resetting all data...")
  if folder.count('/') > 2:
    dbutils.fs.rm(folder, True)

data_exists = False
try:
  dbutils.fs.ls(folder)
  dbutils.fs.ls(folder+"/orders")
  dbutils.fs.ls(folder+"/users")
  dbutils.fs.ls(folder+"/events")
  generate_data = True
  print("data already exists")
except:
  print("folder doesn't exists, generating the data...")

if data_exists:
  dbutils.notebook.exit("data already exists")

# COMMAND ----------

# DBTITLE 1,users data
from pyspark.sql import functions as F
from faker import Faker
from collections import OrderedDict 
import uuid
fake = Faker()
import random
from datetime import datetime, timedelta


fake_firstname = F.udf(fake.first_name)
fake_lastname = F.udf(fake.last_name)
fake_email = F.udf(fake.ascii_company_email)

def fake_date_between(months=0):
  start = datetime.now() - timedelta(days=30*months)
  return F.udf(lambda: fake.date_between_dates(date_start=start, date_end=start + timedelta(days=30)).strftime("%m-%d-%Y %H:%M:%S"))

fake_date = F.udf(lambda:fake.date_time_this_month().strftime("%m-%d-%Y %H:%M:%S"))
fake_date_old = F.udf(lambda:fake.date_between_dates(date_start=datetime(2012,1,1), date_end=datetime(2015,12,31)).strftime("%m-%d-%Y %H:%M:%S"))
fake_address = F.udf(fake.address)
canal = OrderedDict([("WEBAPP", 0.5),("MOBILE", 0.1),("PHONE", 0.3),(None, 0.01)])
fake_canal = F.udf(lambda:fake.random_elements(elements=canal, length=1)[0])
fake_id = F.udf(lambda: str(uuid.uuid4()) if random.uniform(0, 1) < 0.98 else None)
countries = ['FR', 'USA', 'SPAIN']
fake_country = F.udf(lambda: countries[random.randint(0,2)])

def get_df(size, month):
  df = spark.range(0, size).repartition(10)
  df = df.withColumn("id", fake_id())
  df = df.withColumn("firstname", fake_firstname())
  df = df.withColumn("lastname", fake_lastname())
  df = df.withColumn("email", fake_email())
  df = df.withColumn("address", fake_address())
  df = df.withColumn("canal", fake_canal())
  df = df.withColumn("country", fake_country())  
  df = df.withColumn("creation_date", fake_date_between(month)())
  df = df.withColumn("last_activity_date", fake_date())
  df = df.withColumn("gender", F.round(F.rand()+0.2))
  return df.withColumn("age_group", F.round(F.rand()*10))

df_customers = get_df(133, 12*30).withColumn("creation_date", fake_date_old())

from concurrent.futures import ThreadPoolExecutor

def gen_data(i):
    return get_df(2000+i*200, 24-i)

with ThreadPoolExecutor(max_workers=3) as executor:
    for n in executor.map(gen_data, range(1, 24)):
        df_customers = df_customers.union(n)

df_customers = df_customers.cache()

ids = df_customers.select("id").collect()
ids = [r["id"] for r in ids]


# COMMAND ----------

#Number of order per customer to generate a nicely distributed dataset
import numpy as np
np.random.seed(0)
mu, sigma = 3, 2 # mean and standard deviation
s = np.random.normal(mu, sigma, int(len(ids)))
s = [i if i > 0 else 0 for i in s]

#Most of our customers have ~3 orders
import matplotlib.pyplot as plt
count, bins, ignored = plt.hist(s, 30, density=False)
plt.show()
s = [int(i) for i in s]

order_user_ids = list()
action_user_ids = list()
for i, id in enumerate(ids):
  for j in range(1, s[i]):
    order_user_ids.append(id)
    #Let's make 5 more actions per order (5 click on the website to buy something)
    for j in range(1, 5):
      action_user_ids.append(id)
      
print(f"Generated {len(order_user_ids)} orders and  {len(action_user_ids)} actions for {len(ids)} users")

# COMMAND ----------

# DBTITLE 1,order data
orders = spark.createDataFrame([(i,) for i in order_user_ids], ['user_id'])
orders = orders.withColumn("id", fake_id())
orders = orders.withColumn("transaction_date", fake_date())
orders = orders.withColumn("item_count", F.round(F.rand()*2)+1)
orders = orders.withColumn("amount", F.col("item_count")*F.round(F.rand()*30+10))
orders = orders.cache()
orders.repartition(10).write.format("json").mode("overwrite").save(folder+"/orders")
cleanup_folder(folder+"/orders")  


# COMMAND ----------

# DBTITLE 1,website actions
#Website interaction
import re

platform = OrderedDict([("ios", 0.5),("android", 0.1),("other", 0.3),(None, 0.01)])
fake_platform = F.udf(lambda:fake.random_elements(elements=platform, length=1)[0])

action_type = OrderedDict([("view", 0.5),("log", 0.1),("click", 0.3),(None, 0.01)])
fake_action = F.udf(lambda:fake.random_elements(elements=action_type, length=1)[0])
fake_uri = F.udf(lambda:re.sub(r'https?:\/\/.*?\/', "https://databricks.com/", fake.uri()))


actions = spark.createDataFrame([(i,) for i in order_user_ids], ['user_id']).repartition(20)
actions = actions.withColumn("event_id", fake_id())
actions = actions.withColumn("platform", fake_platform())
actions = actions.withColumn("date", fake_date())
actions = actions.withColumn("action", fake_action())
actions = actions.withColumn("session_id", fake_id())
actions = actions.withColumn("url", fake_uri())
actions = actions.cache()
actions.write.format("csv").option("header", True).mode("overwrite").save(folder+"/events")
cleanup_folder(folder+"/events")  


# COMMAND ----------

# DBTITLE 1,Compute churn and save users
#Let's generate the Churn information. We'll fake it based on the existing data & let our ML model learn it
from pyspark.sql.functions import col
import pyspark.sql.functions as F

churn_proba_action = actions.groupBy('user_id').agg({'platform': 'first', '*': 'count'}).withColumnRenamed("count(1)", "action_count")
#Let's count how many order we have per customer.
churn_proba = orders.groupBy('user_id').agg({'item_count': 'sum', '*': 'count'})
churn_proba = churn_proba.join(churn_proba_action, ['user_id'])
churn_proba = churn_proba.join(df_customers, churn_proba.user_id == df_customers.id)

#Customer having > 5 orders are likely to churn

churn_proba = (churn_proba.withColumn("churn_proba", 5 +  F.when(((col("count(1)") >=5) & (col("first(platform)") == "ios")) |
                                                                 ((col("count(1)") ==3) & (col("gender") == 0)) |
                                                                 ((col("count(1)") ==2) & (col("gender") == 1) & (col("age_group") <= 3)) |
                                                                 ((col("sum(item_count)") <=1) & (col("first(platform)") == "android")) |
                                                                 ((col("sum(item_count)") >=10) & (col("first(platform)") == "ios")) |
                                                                 (col("action_count") >=4) |
                                                                 (col("country") == "USA") |
                                                                 ((F.datediff(F.current_timestamp(), col("creation_date")) >= 90)) |
                                                                 ((col("age_group") >= 7) & (col("gender") == 0)) |
                                                                 ((col("age_group") <= 2) & (col("gender") == 1)), 80).otherwise(20)))

churn_proba = churn_proba.withColumn("churn", F.rand()*100 < col("churn_proba"))
churn_proba = churn_proba.drop("user_id", "churn_proba", "sum(item_count)", "count(1)", "first(platform)", "action_count")
churn_proba.repartition(100).write.format("json").mode("overwrite").save(folder+"/users")
cleanup_folder(folder+"/users")
