# Databricks notebook source
# MAGIC %md ### Run TPC-DS Benchmarks

# COMMAND ----------

pip install databricks-sdk -q

# COMMAND ----------

# DBTITLE 1,Configuration Variables
import time
from databricks.sdk import WorkspaceClient

# Host and PAT for beaker authentication
HOST = spark.conf.get('spark.databricks.workspaceUrl')
PAT = WorkspaceClient().tokens.create(comment='temp use', lifetime_seconds=60*30).token_value

# ID of the warehouse to run benchmarks with
WAREHOUSE_ID = dbutils.widgets.get("warehouse_id")
WAREHOUSE_HTTP_PATH = f"/sql/1.0/warehouses/{WAREHOUSE_ID}"

# Name of the catalog to read/write to
CATALOG_NAME = dbutils.widgets.get("catalog_name")

# Name of the schema to read/write to
SCHEMA_NAME = dbutils.widgets.get("schema_name")

# Location of query files
QUERY_PATH = dbutils.widgets.get("query_path").lstrip('/').replace('dbfs:','/dbfs')

# Number of procs in beaker
CONCURRENCY = int(dbutils.widgets.get("concurrency"))

# Number of procs in beaker
QUERY_REPETITION_COUNT = int(dbutils.widgets.get("query_repetition_count"))

# Id of the job, which is used to create the schema
try:
  job_id = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().get("jobId").get()
  METRICS_TABLE_NAME = f"benchmark_metrics_for_job_{job_id}"
except AttributeError as e:
  print("This notebook must be run within a Databricks workflow.")
  raise e

# COMMAND ----------

# DBTITLE 1,Start Warehouse
print(time.time())

w = WorkspaceClient()
w.warehouses.start(WAREHOUSE_ID)
w.wait_get_cluster_running(WAREHOUSE_ID)

print(time.time())

# COMMAND ----------

# DBTITLE 1,Run Benchmark
from beaker import benchmark
from functools import reduce
from pyspark.sql import DataFrame
import pyspark.sql.functions as F

# Create beaker benchmark object
bm = benchmark.Benchmark(results_cache_enabled=False)

# # Set benchmarking parameters
bm.setName(name=f"TPC-DS Benchmark {SCHEMA_NAME}")
bm.setHostname(hostname=HOST)
bm.setWarehouse(http_path=WAREHOUSE_HTTP_PATH)
bm.setConcurrency(concurrency=CONCURRENCY)
bm.setWarehouseToken(token=PAT)
bm.setCatalog(catalog=CATALOG_NAME)
bm.setSchema(schema=SCHEMA_NAME)
bm.setQueryFileDir(QUERY_PATH)
bm.setQueryRepeatCount(QUERY_REPETITION_COUNT)

# Execute run
start_time = time.time()
result = bm.execute()
duration = time.time() - start_time

# Store run metrics
metrics_df = spark.createDataFrame(result)

# COMMAND ----------

# DBTITLE 1,Write Metrics to a Delta Table
# write output dataframe to delta for analysis/consumption
metrics_full_path = f"{CATALOG_NAME}.{SCHEMA_NAME}.{METRICS_TABLE_NAME}"
print(f"Writing to delta table: {metrics_full_path}")
metrics_df.write.mode('overwrite').saveAsTable(metrics_full_path)

# Display the table for reference
metrics_df.display()

# COMMAND ----------

# DBTITLE 1,Throughput
sql_files = [x for x in dbutils.fs.ls(QUERY_PATH.replace('/dbfs','dbfs:')) if x.name.endswith('.sql')]
n_sql_files = len(sql_files)
print(f"TPC-DS queries per minute: {n_sql_files / (duration / 60)}")

# COMMAND ----------


