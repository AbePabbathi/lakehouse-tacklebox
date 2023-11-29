# Databricks notebook source
# MAGIC %run ./00-Functions

# COMMAND ----------

# SETUP
test_name = "<TEST NAME>"
result_catalog = "<CATALOG>"
result_schema = "<SCHEMA>"
token = "<DB TOKEN>"

# SETUP SOURCE WAREHOUSE ID AND START AND END TIME
source_warehouse_id = "<WAREHOUSE ID>"
source_start_time = "2023-12-01 00:00:00"
source_end_time = "2023-12-01 00:05:00"

replay_test = QueryReplayTest(
    test_name=test_name,
    result_catalog=result_catalog,
    result_schema=result_schema,
    token=token,
    source_warehouse_id=source_warehouse_id,
    source_start_time=source_start_time,
    source_end_time=source_end_time,
)

test_id = replay_test.run()

# COMMAND ----------

# MAGIC %md
# MAGIC ### `query_df` is the list of source queries we are going to use

# COMMAND ----------

replay_test.query_df.orderBy('start_time').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### `show_run` gives the test details

# COMMAND ----------

replay_test.show_run.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### `show_run_details` gives the corresponding statement_id's for all the queries we ran

# COMMAND ----------

replay_test.show_run_details.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### `query_results` gives the result comparing source details against the test output

# COMMAND ----------

# Recreate test object by providing test_id will allow to us to retrieve the query results later (since system tables might not have all the results immediately)

replay_test = QueryReplayTest(
    test_name=test_name,
    result_catalog=result_catalog,
    result_schema=result_schema,
    token=token,
    source_warehouse_id=source_warehouse_id,
    source_start_time=source_start_time,
    source_end_time=source_end_time,
    test_id=test_id
)

replay_test.query_results.display()
