# Databricks notebook source
# MAGIC %md
# MAGIC # Create churn table
# MAGIC
# MAGIC Create tables in the given catalog (uc or legacy)

# COMMAND ----------

dbutils.widgets.dropdown("reset_all_data", "false", ["true", "false"], "Reset all data")
dbutils.widgets.text("root_folder", "lakehouse-retail-c360", "Demo root folder")
dbutils.widgets.text("catalog", "", "UC catalog")
dbutils.widgets.text("db", "", "Database")

reset_all_data = dbutils.widgets.get("reset_all_data") == "true"
root_folder = dbutils.widgets.get("root_folder")

# COMMAND ----------

# MAGIC %run ./00-global-setup $reset_all_data=$reset_all_data $db_prefix=retail $catalog=$catalog $db=$db

# COMMAND ----------

from databricks.feature_store import FeatureStoreClient
fs = FeatureStoreClient()
database = dbName
def display_automl_churn_link(dataset, model_name, force_refresh = False): 
  if force_refresh:
    reset_automl_run("lakehouse_churn_auto_ml")
  display_automl_link("lakehouse_churn_auto_ml", model_name, dataset, "churn", 5, move_to_production=False)

def get_automl_churn_run(force_refresh = False): 
  if force_refresh:
    reset_automl_run("lakehouse_churn_auto_ml")
  from_cache, r = get_automl_run_or_start("lakehouse_churn_auto_ml", "dbdemos_customer_churn", fs.read_table(f'{database}.churn_user_features'), "churn", 5, move_to_production=False)
  return r

# COMMAND ----------

import json
import time
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, sha1, col, initcap, to_timestamp

raw_data_location = cloud_storage_path
folder = "/demos/retail/churn"

if reset_all_data or is_folder_empty(folder+"/orders") or is_folder_empty(folder+"/users") or is_folder_empty(folder+"/events"):
  #data generation on another notebook to avoid installing libraries (takes a few seconds to setup pip env)
  print(f"Generating data under {folder} , please wait a few sec...")
  path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
  parent_count = path[path.rfind(root_folder):].count('/') - 1
  prefix = "./" if parent_count == 0 else parent_count*"../"
  prefix = f'{prefix}_resources/'
  dbutils.notebook.run(prefix+"01-load-data", 600)
else:
  print("data already existing. Run with reset_all_data=true to force a data cleanup for your local demo.")

# COMMAND ----------

def ingest_folder(folder, data_format, table, hints = None):
  bronze_products = (spark.readStream
                              .format("cloudFiles")
                              .option("cloudFiles.format", data_format)
                              .option("cloudFiles.inferColumnTypes", "true")
                              .option("cloudFiles.schemaHints", None)
                              .option("cloudFiles.schemaLocation", f"{raw_data_location}/schema/{table}") #Autoloader will automatically infer all the schema & evolution
                              .load(folder))

  return (bronze_products.writeStream
                    .option("checkpointLocation", f"{raw_data_location}/checkpoint/{table}") #exactly once delivery on Delta tables over restart/kill
                    .option("mergeSchema", "true") #merge any new column dynamically
                    .trigger(once = True) #Remove for real time streaming
                    .table(table))

if not spark._jsparkSession.catalog().tableExists(f"`{catalog}`.`{database}`.`churn_orders_bronze`") or \
   not spark._jsparkSession.catalog().tableExists(f"`{catalog}`.`{database}`.`churn_app_events`") or \
   not spark._jsparkSession.catalog().tableExists(f"`{catalog}`.`{database}`.`churn_users_bronze`") or \
   not spark._jsparkSession.catalog().tableExists(f"`{catalog}`.`{database}`.`churn_features`"):  
  #One of the table is missing, let's rebuild them all
  spark.sql(f"drop table if exists `{catalog}`.`{database}`.`churn_orders_bronze`")
  spark.sql(f"drop table if exists `{catalog}`.`{database}`.`churn_app_events`")
  spark.sql(f"drop table if exists `{catalog}`.`{database}`.`churn_users_bronze`")
  spark.sql(f"drop table if exists `{catalog}`.`{database}`.`churn_features`")
  #drop the checkpoints 
  if raw_data_location.count('/') > 3:
    dbutils.fs.rm(raw_data_location, True)  
  qo = ingest_folder('/demos/retail/churn/orders', 'json', 'churn_orders_bronze')
  qe = ingest_folder('/demos/retail/churn/events', 'csv', 'churn_app_events')
  qu = ingest_folder('/demos/retail/churn/users', 'json',  'churn_users_bronze')

  qo.awaitTermination()
  qe.awaitTermination()
  qu.awaitTermination()

  q = (spark.readStream 
          .table("churn_users_bronze")
            .withColumnRenamed("id", "user_id")
            .withColumn("email", sha1(col("email")))
            .withColumn("creation_date", to_timestamp(col("creation_date"), "MM-dd-yyyy H:mm:ss"))
            .withColumn("last_activity_date", to_timestamp(col("last_activity_date"), "MM-dd-yyyy HH:mm:ss"))
            .withColumn("firstname", initcap(col("firstname")))
            .withColumn("lastname", initcap(col("lastname")))
            .withColumn("age_group", col("age_group").cast('int'))
            .withColumn("gender", col("gender").cast('int'))
            .withColumn("churn", col("churn").cast('int'))
            .drop(col("_rescued_data"))
       .writeStream
          .option("checkpointLocation", f"{raw_data_location}/checkpoint/churn_users")
          .trigger(once=True)
          .table("churn_users"))


  (spark.readStream 
          .table("churn_orders_bronze")
            .withColumnRenamed("id", "order_id")
            .withColumn("amount", col("amount").cast('int'))
            .withColumn("item_count", col("item_count").cast('int'))
            .withColumn("creation_date", to_timestamp(col("transaction_date"), "MM-dd-yyyy H:mm:ss"))
            .drop(col("_rescued_data"))
       .writeStream
          .option("checkpointLocation", f"{raw_data_location}/checkpoint/orders")
          .trigger(once=True)
          .table("churn_orders")).awaitTermination()
  q.awaitTermination()

  spark.sql("""
      CREATE OR REPLACE TABLE churn_features AS
      WITH 
          churn_orders_stats AS (SELECT user_id, count(*) as order_count, sum(amount) as total_amount, sum(item_count) as total_item, max(creation_date) as last_transaction
            FROM churn_orders GROUP BY user_id),  
          churn_app_events_stats as (
            SELECT first(platform) as platform, user_id, count(*) as event_count, count(distinct session_id) as session_count, max(to_timestamp(date, "MM-dd-yyyy HH:mm:ss")) as last_event
              FROM churn_app_events GROUP BY user_id)
        SELECT *, 
           datediff(now(), creation_date) as days_since_creation,
           datediff(now(), last_activity_date) as days_since_last_activity,
           datediff(now(), last_event) as days_last_event
           FROM churn_users
             INNER JOIN churn_orders_stats using (user_id)
             INNER JOIN churn_app_events_stats using (user_id) """)
