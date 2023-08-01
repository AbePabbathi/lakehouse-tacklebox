# Databricks notebook source
dbutils.widgets.dropdown("reset_all_data", "false", ["true", "false"], "Reset all data")
reset_all_data = dbutils.widgets.get("reset_all_data") == "true"

# COMMAND ----------

# MAGIC %run ./00-global-setup $reset_all_data=$reset_all_data $db_prefix=retail $catalog=dbdemos $db=lakehouse_c360

# COMMAND ----------

catalog = "dbdemos"
database = 'lakehouse_c360'

# COMMAND ----------

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
  dbutils.notebook.run(prefix+"02-create-churn-tables", 600, {"catalog": catalog, "cloud_storage_path": "/demos/", "reset_all_data": reset_all_data, "db": database})
else:
  print("data already existing. Run with reset_all_data=true to force a data cleanup for your local demo.")

# COMMAND ----------

for table in spark.sql("SHOW TABLES").collect():
    try:
        spark.sql(f"alter table {table['tableName']} owner to `account users`")
    except Exception as e:
        print(f"couldn't set table {table} ownership to account users")
