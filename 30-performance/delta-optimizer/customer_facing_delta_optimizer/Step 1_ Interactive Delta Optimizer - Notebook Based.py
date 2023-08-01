# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## This notebook imports the Delta Optimizer and does the following: 
# MAGIC
# MAGIC <ul> 
# MAGIC   
# MAGIC   <li> Poll Query History API and get List of Queries for a set of SQL Warehouses (this is incremental, so you just define a lookback period for the first time you poll)
# MAGIC   <li> Analyze transaction logs for tables in set of databases (all by default) -- file size, partitions, merge predicates
# MAGIC   <li> Rank unified strategy
# MAGIC     
# MAGIC </ul>
# MAGIC
# MAGIC
# MAGIC ### KEY USER NOTES: 
# MAGIC 1. Database names should be fully qualified (catalog.database.table)
# MAGIC 2. Table Filter List must be fully qualified (catalog.database.table)
# MAGIC 3. If table filter mode is all, then the filter list can be blank, otherwise ensure that it is correct
# MAGIC
# MAGIC ### Depedencies
# MAGIC <li> Ensure that you either get a token as a secret or use a cluster with the env variable called DBX_TOKEN to authenticate to DBSQL

# COMMAND ----------

from deltaoptimizer import DeltaProfiler, QueryProfiler, DeltaOptimizer
import os

# COMMAND ----------

# DBTITLE 1,Register and Retrieve DBX Auth Token
DBX_TOKEN = "dapixxxxxxxxxxxxx"

# COMMAND ----------

# DBTITLE 1,Set up params before running
## Assume running in a Databricks notebook
dbutils.widgets.dropdown("Query History Lookback Period (days)", defaultValue="3",choices=["1","3","7","14","30","60","90"])
dbutils.widgets.text("SQL Warehouse Ids (csv list)", "")
dbutils.widgets.text("Server Hostname:", "")
dbutils.widgets.text("Database Names (csv) - fully qualified or defaults to hive_metastore catalog:", "")
dbutils.widgets.dropdown("Start Over?","No", ["Yes","No"])
dbutils.widgets.text("Optimizer Output Database:", "hive_metastore.delta_optimizer")
dbutils.widgets.dropdown("Table Filter Mode", "all", ["all", "include_list", "exclude_list"])
dbutils.widgets.text("Table Filter List (catalog.database.table) (Csv List)", "")

# COMMAND ----------

lookbackPeriod = int(dbutils.widgets.get("Query History Lookback Period (days)"))
warehouseIdsList = [i.strip() for i in dbutils.widgets.get("SQL Warehouse Ids (csv list)").split(",")]
workspaceName = dbutils.widgets.get("Server Hostname:").strip()
warehouse_ids = dbutils.widgets.get("SQL Warehouse Ids (csv list)")
start_over = dbutils.widgets.get("Start Over?")
table_filter_mode = dbutils.widgets.get("Table Filter Mode")
table_filter_list = [i.strip() for i in dbutils.widgets.get("Table Filter List (catalog.database.table) (Csv List)").split(",")]

# COMMAND ----------

database_output = dbutils.widgets.get("Optimizer Output Database:").strip()
databases_raw = dbutils.widgets.get("Database Names (csv) - fully qualified or defaults to hive_metastore catalog:").split(",")

delta_optimizer = DeltaOptimizer(database_name=database_output)

# COMMAND ----------

if start_over == "Yes":
  delta_optimizer.drop_delta_optimizer()

# COMMAND ----------

# DBTITLE 1,Build Query History Profile
####### Step 1: Build Profile #######
## Initialize Profiler

## catalogs_to_check_views should include ALL catalogs where views could live that you want to optimize underlying tables for
## Ideally they are just the same catalogs are your database names defined in the params so we try to parse for you to start there, but if you need to add, change the list here. 

## Assume running on Databricks notebooks if not imported
databases_raw = dbutils.widgets.get("Database Names (csv) - fully qualified or defaults to hive_metastore catalog:").split(",")
clean_catalogs = list(set([i.split(".")[0].strip() if len(i.split(".")) == 2 else 'hive_metastore' for i in databases_raw]))


query_profiler = QueryProfiler(workspaceName, warehouseIdsList, database_name=database_output, catalogs_to_check_views=clean_catalogs, scrub_views=True, table_filter_mode=table_filter_mode, table_filter_list=table_filter_list)

query_profiler.build_query_history_profile(dbx_token = DBX_TOKEN, mode='auto', lookback_period_days=lookbackPeriod)

# COMMAND ----------

# DBTITLE 1,Run Delta Profiler
####### Step 2: Build stats from transaction logs/table data #######

## Assume running on Databricks notebooks if not imported
databases_raw = dbutils.widgets.get("Database Names (csv) - fully qualified or defaults to hive_metastore catalog:")


## Initialize class and pass in database csv string
profiler = DeltaProfiler( monitored_db_csv= databases_raw, database_name=database_output, table_filter_mode=table_filter_mode, table_filter_list=table_filter_list) ## examples include 'default', 'mydb1,mydb2', 'all' or leave blank

## Get tables
profiler.get_all_tables_to_monitor()

## Get predicate analysis for tables
profiler.parse_stats_for_tables()

## Build final table output
profiler.build_all_tables_stats()

## Generate cardinality stats
profiler.build_cardinality_stats()


# COMMAND ----------

# DBTITLE 1,Run Delta Optimizer
####### Step 3: Build Strategy and Rank #######
## Build Strategy

delta_optimizer = DeltaOptimizer(database_name=database_output)

delta_optimizer.build_optimization_strategy()


# COMMAND ----------

# DBTITLE 1,Return most up to date results!
df = delta_optimizer.get_results()

# COMMAND ----------

df.display()
