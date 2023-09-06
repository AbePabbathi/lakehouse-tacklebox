# Databricks notebook source
# DBTITLE 1,Display Widgets
dbutils.widgets.text("Catalog Name", "hive_metastore")
dbutils.widgets.text("Schema Prefix", "tpcds")
dbutils.widgets.dropdown("Number of GB of Data", "1", ["1", "10", "100", "500", "1000"])
dbutils.widgets.text("Concurrency", "1")
dbutils.widgets.dropdown("Query Repetition Count", "1", [str(x) for x in range(1,101)])
dbutils.widgets.dropdown("Warehouse Size", "Small", ["2X-Small","X-Small","Small","Medium","Large","X-Large","2X-Large","3X-Large","4X-Large"])
dbutils.widgets.dropdown("Maximum Number of Clusters", "10", [str(x) for x in range(1,41)])

# COMMAND ----------

# DBTITLE 1,Import Constants
# MAGIC %run ./constants

# COMMAND ----------

# DBTITLE 1,Pull Variables from Notebook Widgets
constants = Constants(
  **get_widget_values(dbutils)
)

# COMMAND ----------

# DBTITLE 1,Create and Run TPC-DS Benchmark
from utils.run import run

run(spark, dbutils, constants)
