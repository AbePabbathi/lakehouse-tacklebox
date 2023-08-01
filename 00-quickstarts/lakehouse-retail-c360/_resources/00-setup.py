# Databricks notebook source
dbutils.widgets.dropdown("reset_all_data", "false", ["true", "false"], "Reset all data")

# COMMAND ----------

# MAGIC %run ./00-global-setup $reset_all_data=$reset_all_data $db_prefix=retail

# COMMAND ----------

import mlflow
if "evaluate" not in dir(mlflow):
    raise Exception("ERROR - YOU NEED MLFLOW 2.0 for this demo. Select DBRML 12+")
    
import json
import time
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

reset_all_data = dbutils.widgets.get("reset_all_data") == "true"
raw_data_location = cloud_storage_path+"/retail/churn"

import json
import time
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, sha1, col, initcap, to_timestamp

folder = "/demos/retail/churn"

if reset_all_data or is_folder_empty(folder+"/orders") or is_folder_empty(folder+"/users") or is_folder_empty(folder+"/events"):
  #data generation on another notebook to avoid installing libraries (takes a few seconds to setup pip env)
  print(f"Generating data under {folder} , please wait a few sec...")
  path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
  parent_count = path[path.rfind("lakehouse-retail-c360"):].count('/') - 1
  prefix = "./" if parent_count == 0 else parent_count*"../"
  prefix = f'{prefix}_resources/'
  dbutils.notebook.run(prefix+"01-load-data", 600, {"reset_all_data": dbutils.widgets.get("reset_all_data")})
else:
  print("data already existing. Run with reset_all_data=true to force a data cleanup for your local demo.")
