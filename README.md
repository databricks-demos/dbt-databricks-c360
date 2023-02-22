# dbt on Databricks demo
---

## Databricks setup

This content demo how Databricks can run dbt pipelines, integrated with Databricks Workflow.


This demo is part of [dbdemos.ai](http://www.dbdemos.ai) dbt bundle. <br/>
To install the full demo with the worfklow properly setup, you can run `dbdemos.install('dbt-on-databricks')`

This demo replicate the DLT pipeline in the lakehouse c360 databricks demo available in `dbdemos.install('lakehouse-retail-c360')`

Here is an overiew of the workflow created by dbdemos:

<img width="800px" src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/main/images/partners/dbt/dbt-databricks-workflow.png" />


## How dbt + Databricks is working

### Project structure




This demo is broken up into the following building blocks. View the sub-folders in in the sequence indicated below to help you understand the overall flow:


- ```01-ingest-autoloader``` <br/>

    * This contains the notebook to ingest raw data incrementally into our Lakehouse 
    * The goal is to ingest the new data once it is uploaded into cloud storage, so our dbt pipeline can do the transformations 
    * It is worth noting that while dbt has a functionality called ```seed``` that allows files to be loaded, it is currently limited to ```CSV``` files 
    
- ```dbt_project.yml```
    * Every dbt project requires a ```dbt_project.yml``` file - this is how dbt knows a directory is a dbt project
    * It contains information such as connection configurations to Databricks SQL Warehouses and where SQL transformation files are stored 

- ```profiles.yml```
    * This file stores profile configuration which dbt needs to connect to Databricks compute resources
    * Connection details such as the server hostname, HTTP path, catalog, db/schema information are configured here 
    
- ```models```
    * A model in dbt refers to a single ```.sql``` file containing a modular data transformation block 
    * In this demo, we have modularized our transformations into 4 files in accordance with the Medallion Architecture 
    * Within each file, we can configure how the transformation will be materialized - either as a table or a view

- ```tests```
    * Tests are assertions you make about your dbt models 
    * They are typically used for data quality and validation purposes
    * We also have the ability to quarantine and isolate records that fail a particular assertion
    

- ```03-ml-predict-churn```
   * This contains the notebook to load our churn prediction ML model from MLFlow after the dbt transformations are complete
   * The model is loaded as a SQL function, then applied to the ```dbt_c360_gold_churn_features``` that will be materialized at the end of the second dbt task in our workflow

- ```seeds```
    * This is an optional folder used to store sample, adhoc CSV files to be loaded into the Lakehouse



<br>

<img src="https://mchanstorage2.blob.core.windows.net/mchan-images/databricksDbtHeader.png" width="525px" />

<img width="1px" src="https://www.google-analytics.com/collect?v=1&gtm=GTM-NKQ8TT7&tid=UA-163989034-1&cid=555&aip=1&t=event&ec=field_demos&ea=display&dp=%2F42_field_demos%2Ffeatures%2Fdbt%2Freadme&dt=FEATURE_DBT" />



### Feedback
---
Got comments and feedback? <br/>
Feel free to reach out to ```mendelsohn.chan@databricks.com``` or ```quentin.ambard.databricks.com```









