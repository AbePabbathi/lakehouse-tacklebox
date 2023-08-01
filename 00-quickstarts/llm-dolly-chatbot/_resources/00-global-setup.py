# Databricks notebook source
# MAGIC %md 
# MAGIC
# MAGIC # Technical Setup notebook. Hide this cell results
# MAGIC Initialize dataset to the current user and cleanup data when reset_all_data is set to true
# MAGIC
# MAGIC Do not edit

# COMMAND ----------

dbutils.widgets.dropdown("reset_all_data", "false", ["true", "false"], "Reset all data")
dbutils.widgets.text("min_dbr_version", "9.1", "Min required DBR version")
#Empty value will try default: dbdemos with a fallback to hive_metastore
#Specifying a value will not have fallback and fail if the catalog can't be used/created
dbutils.widgets.text("catalog", "", "Catalog")
#Empty value will be set to a database scoped to the current user using db_prefix
dbutils.widgets.text("db", "", "Database")
#ignored if db is set (we force the databse to the given value in this case)
dbutils.widgets.text("db_prefix", "retail", "Database prefix")

# COMMAND ----------

from delta.tables import *
import pandas as pd
import logging
from pyspark.sql.functions import to_date, col, regexp_extract, rand, to_timestamp, initcap, sha1
from pyspark.sql.types import *
from pyspark.sql.functions import pandas_udf, PandasUDFType, input_file_name, col
import pyspark.sql.functions as F
import re
import time


# VERIFY DATABRICKS VERSION COMPATIBILITY ----------

try:
  min_required_version = dbutils.widgets.get("min_dbr_version")
except:
  min_required_version = "9.1"

version_tag = spark.conf.get("spark.databricks.clusterUsageTags.sparkVersion")
version_search = re.search('^([0-9]*\.[0-9]*)', version_tag)
assert version_search, f"The Databricks version can't be extracted from {version_tag}, shouldn't happen, please correct the regex"
current_version = float(version_search.group(1))
assert float(current_version) >= float(min_required_version), f'The Databricks version of the cluster must be >= {min_required_version}. Current version detected: {current_version}'
assert "ml" in version_tag.lower(), f"The Databricks ML runtime must be used. Current version detected doesn't contain 'ml': {version_tag} "


#python Imports for ML...
from sklearn.compose import ColumnTransformer
from sklearn.feature_extraction.text import CountVectorizer, TfidfTransformer
from sklearn.preprocessing import LabelEncoder
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.ensemble import RandomForestClassifier
from sklearn import metrics
from sklearn.model_selection import GridSearchCV
import mlflow
import mlflow.sklearn
from mlflow.tracking.client import MlflowClient
from hyperopt import fmin, hp, tpe, STATUS_OK, Trials
from hyperopt.pyll.base import scope
from hyperopt import SparkTrials
from sklearn.model_selection import GroupKFold
from pyspark.sql.functions import pandas_udf, PandasUDFType
import os
import pandas as pd
from hyperopt import space_eval
import numpy as np
from time import sleep


from sklearn.preprocessing import LabelBinarizer, LabelEncoder
from sklearn.metrics import confusion_matrix

#force the experiment to the field demos one. Required to launch as a batch
def init_experiment_for_batch(demo_name, experiment_name):
  #You can programatically get a PAT token with the following
  pat_token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
  url = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().get()
  #current_user = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user')
  import requests
  xp_root_path = f"/dbdemos/experiments/{demo_name}"
  requests.post(f"{url}/api/2.0/workspace/mkdirs", headers = {"Accept": "application/json", "Authorization": f"Bearer {pat_token}"}, json={ "path": xp_root_path})
  xp = f"{xp_root_path}/{experiment_name}"
  print(f"Using common experiment under {xp}")
  mlflow.set_experiment(xp)
  return mlflow.get_experiment_by_name(xp)

# COMMAND ----------

def get_cloud_name():
  return spark.conf.get("spark.databricks.clusterUsageTags.cloudProvider").lower()

# COMMAND ----------

spark.conf.set("spark.databricks.cloudFiles.schemaInference.sampleSize.numFiles", "10")

current_user = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user')
if current_user.rfind('@') > 0:
  current_user_no_at = current_user[:current_user.rfind('@')]
else:
  current_user_no_at = current_user
current_user_no_at = re.sub(r'\W+', '_', current_user_no_at)

db = dbutils.widgets.get("db")
db_prefix = dbutils.widgets.get("db_prefix")
if len(db) == 0:
  dbName = db_prefix+"_"+current_user_no_at
else:
  dbName = db
  
cloud_storage_path = f"/Users/{current_user}/demos/{db_prefix}"
reset_all = dbutils.widgets.get("reset_all_data") == "true"

#Try to use the UC catalog "dbdemos" when possible. IF not will fallback to hive_metastore
catalog = dbutils.widgets.get("catalog")

def use_and_create_db(catalog, dbName, cloud_storage_path = None):
  print(f"USE CATALOG `{catalog}`")
  spark.sql(f"USE CATALOG `{catalog}`")
  if cloud_storage_path is None or catalog not in ['hive_metastore', 'spark_catalog']:
    spark.sql(f"""create database if not exists `{dbName}` """)
  else:
    spark.sql(f"""create database if not exists `{dbName}` LOCATION '{cloud_storage_path}/tables' """)

if reset_all:
  spark.sql(f"DROP DATABASE IF EXISTS `{dbName}` CASCADE")
  dbutils.fs.rm(cloud_storage_path, True)

if catalog == "spark_catalog":
  catalog = "hive_metastore"
  
#If the catalog is defined, we force it to the given value and throw exception if not.
if len(catalog) > 0:
  current_catalog = spark.sql("select current_catalog()").collect()[0]['current_catalog()']
  if current_catalog != catalog:
    catalogs = [r['catalog'] for r in spark.sql("SHOW CATALOGS").collect()]
    if catalog not in catalogs and catalog not in ['hive_metastore', 'spark_catalog']:
      spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
  use_and_create_db(catalog, dbName)
else:
  #otherwise we'll try to setup the catalog to DBDEMOS and create the database here. If we can't we'll fallback to legacy hive_metastore
  print("Try to setup UC catalog")
  try:
    catalogs = [r['catalog'] for r in spark.sql("SHOW CATALOGS").collect()]
    if len(catalogs) == 1 and catalogs[0] in ['hive_metastore', 'spark_catalog']:
      print(f"UC doesn't appear to be enabled, will fallback to hive_metastore (spark_catalog)")
      catalog = "hive_metastore"
    else:
      if "dbdemos" not in catalogs:
        spark.sql("CREATE CATALOG IF NOT EXISTS dbdemos")
      catalog = "dbdemos"
    use_and_create_db(catalog, dbName)
  except Exception as e:
    print(f"error with catalog {e}, do you have permission or UC enabled? will fallback to hive_metastore")
    catalog = "hive_metastore"
    use_and_create_db(catalog, dbName)

print(f"using cloud_storage_path {cloud_storage_path}")
print(f"using catalog.database `{catalog}`.`{dbName}`")

#Add the catalog to cloud storage path as we could have 1 checkpoint location different per catalog
if catalog not in ['hive_metastore', 'spark_catalog']:
  cloud_storage_path+="_"+catalog
  try:
    spark.sql(f"GRANT CREATE, USAGE on DATABASE {catalog}.{dbName} TO `account users`")
    spark.sql(f"ALTER SCHEMA {catalog}.{dbName} OWNER TO `account users`")
  except Exception as e:
    print("Couldn't grant access to the schema to all users:"+str(e))
  
#with parallel execution this can fail the time of the initialization. add a few retry to fix these issues
for i in range(10):
  try:
    spark.sql(f"""USE `{catalog}`.`{dbName}`""")
    break
  except Exception as e:
    time.sleep(1)
    if i >= 9:
      raise e

# COMMAND ----------

def display_slide(slide_id, slide_number):
  displayHTML(f'''
  <div style="width:1150px; margin:auto">
  <iframe
    src="https://docs.google.com/presentation/d/{slide_id}/embed?slide={slide_number}"
    frameborder="0"
    width="1150"
    height="683"
  ></iframe></div>
  ''')

# COMMAND ----------

def get_active_streams(start_with = ""):
    return [s for s in spark.streams.active if len(start_with) == 0 or (s.name is not None and s.name.startswith(start_with))]
  
# Function to stop all streaming queries 
def stop_all_streams(start_with = "", sleep_time=0):
  import time
  time.sleep(sleep_time)
  streams = get_active_streams(start_with)
  if len(streams) > 0:
    print(f"Stopping {len(streams)} streams")
    for s in streams:
        try:
            s.stop()
        except:
            pass
    print(f"All stream stopped {'' if len(start_with) == 0 else f'(starting with: {start_with}.)'}")
    
def wait_for_all_stream(start = ""):
  import time
  actives = get_active_streams(start)
  if len(actives) > 0:
    print(f"{len(actives)} streams still active, waiting... ({[s.name for s in actives]})")
  while len(actives) > 0:
    spark.streams.awaitAnyTermination()
    time.sleep(1)
    actives = get_active_streams(start)
  print("All streams completed.")
  
def wait_for_table(table_name, timeout_duration=120):
  import time
  i = 0
  while not spark._jsparkSession.catalog().tableExists(table_name) or spark.table(table_name).count() == 0:
    time.sleep(1)
    if i > timeout_duration:
      raise Exception(f"couldn't find table {table_name} or table is empty. Do you have data being generated to be consumed?")
    i += 1

# COMMAND ----------

from pyspark.sql.functions import col
import mlflow

import databricks
from datetime import datetime

def get_automl_run(name):
  #get the most recent automl run
  df = spark.table("hive_metastore.dbdemos_metadata.automl_experiment").filter(col("name") == name).orderBy(col("date").desc()).limit(1)
  return df.collect()

#Get the automl run information from the hive_metastore.dbdemos_metadata.automl_experiment table. 
#If it's not available in the metadata table, start a new run with the given parameters
def get_automl_run_or_start(name, model_name, dataset, target_col, timeout_minutes, move_to_production = False):
  spark.sql("create database if not exists hive_metastore.dbdemos_metadata")
  spark.sql("create table if not exists hive_metastore.dbdemos_metadata.automl_experiment (name string, date string)")
  result = get_automl_run(name)
  if len(result) == 0:
    print("No run available, start a new Auto ML run, this will take a few minutes...")
    start_automl_run(name, model_name, dataset, target_col, timeout_minutes, move_to_production)
    return (False, get_automl_run(name))
  return (True, result[0])


#Start a new auto ml classification task and save it as metadata.
def start_automl_run(name, model_name, dataset, target_col, timeout_minutes = 5, move_to_production = False):
  from databricks import automl
  automl_run = databricks.automl.classify(
    dataset = dataset,
    target_col = target_col,
    timeout_minutes = timeout_minutes
  )
  experiment_id = automl_run.experiment.experiment_id
  path = automl_run.experiment.name
  data_run_id = mlflow.search_runs(experiment_ids=[automl_run.experiment.experiment_id], filter_string = "tags.mlflow.source.name='Notebook: DataExploration'").iloc[0].run_id
  exploration_notebook_id = automl_run.experiment.tags["_databricks_automl.exploration_notebook_id"]
  best_trial_notebook_id = automl_run.experiment.tags["_databricks_automl.best_trial_notebook_id"]

  cols = ["name", "date", "experiment_id", "experiment_path", "data_run_id", "best_trial_run_id", "exploration_notebook_id", "best_trial_notebook_id"]
  spark.createDataFrame(data=[(name, datetime.today().isoformat(), experiment_id, path, data_run_id, automl_run.best_trial.mlflow_run_id, exploration_notebook_id, best_trial_notebook_id)], schema = cols).write.mode("append").option("mergeSchema", "true").saveAsTable("hive_metastore.dbdemos_metadata.automl_experiment")
  #Create & save the first model version in the MLFlow repo (required to setup hooks etc)
  model_registered = mlflow.register_model(f"runs:/{automl_run.best_trial.mlflow_run_id}/model", model_name)
  set_experiment_permission(path)
  if move_to_production:
    client = mlflow.tracking.MlflowClient()
    print("registering model version "+model_registered.version+" as production model")
    client.transition_model_version_stage(name = model_name, version = model_registered.version, stage = "Production", archive_existing_versions=True)
  return get_automl_run(name)

#Generate nice link for the given auto ml run
def display_automl_link(name, model_name, dataset, target_col, timeout_minutes = 5, move_to_production = False):
  from_cache, r = get_automl_run_or_start(name, model_name, dataset, target_col, timeout_minutes, move_to_production)
  if from_cache:
    html = f"""For exploratory data analysis, open the <a href="/#notebook/{r["exploration_notebook_id"]}">data exploration notebook</a><br/><br/>"""
    html += f"""To view the best performing model, open the <a href="/#notebook/{r["best_trial_notebook_id"]}">best trial notebook</a><br/><br/>"""
    html += f"""To view details about all trials, navigate to the <a href="/#mlflow/experiments/{r["experiment_id"]}/s?orderByKey=metrics.%60val_f1_score%60&orderByAsc=false">MLflow experiment</>"""
    displayHTML(html)

def reset_automl_run(model_name):
  if spark._jsparkSession.catalog().tableExists('hive_metastore.dbdemos_metadata.automl_experiment'):
      spark.sql(f"delete from hive_metastore.dbdemos_metadata.automl_experiment where name='{model_name}'")

#Once the automl experiment is created, we assign CAN MANAGE to all users as it's shared in the workspace
def set_experiment_permission(experiment_path):
  url = dbutils.notebook.entry_point.getDbutils().notebook().getContext().extraContext().apply("api_url")
  import requests
  pat_token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
  headers =  {"Authorization": "Bearer " + pat_token, 'Content-type': 'application/json'}
  status = requests.get(url+"/api/2.0/workspace/get-status", params = {"path": experiment_path}, headers=headers).json()
  #Set can manage to all users to the experiment we created as it's shared among all
  params = {"access_control_list": [{"group_name": "users","permission_level": "CAN_MANAGE"}]}
  permissions = requests.patch(f"{url}/api/2.0/permissions/experiments/{status['object_id']}", json = params, headers=headers)
  if permissions.status_code != 200:
    print("ERROR: couldn't set permission to all users to the autoML experiment")

  #try to find the experiment id
  result = re.search(r"_([a-f0-9]{8}_[a-f0-9]{4}_[a-f0-9]{4}_[a-f0-9]{4}_[a-f0-9]{12})_", experiment_path)
  if result is not None and len(result.groups()) > 0:
    ex_id = result.group(0)
  else:
    print(experiment_path)
    ex_id = experiment_path[experiment_path.rfind('/')+1:]

  path = experiment_path
  path = path[:path.rfind('/')]+"/"
  #List to get the folder with the notebooks from the experiment
  folders = requests.get(url+"/api/2.0/workspace/list", params = {"path": path}, headers=headers).json()
  for f in folders['objects']:
    if f['object_type'] == 'DIRECTORY' and ex_id in f['path']:
        #Set the permission of the experiment notebooks to all
        permissions = requests.patch(f"{url}/api/2.0/permissions/directories/{f['object_id']}", json = params, headers=headers)
        if permissions.status_code != 200:
          print("ERROR: couldn't set permission to all users to the autoML experiment notebooks")

# COMMAND ----------

#Deprecated. TODO: remove all occurence with is_folder_empty
def test_not_empty_folder(folder):
  try:
    return len(dbutils.fs.ls(folder)) > 0
  except:
    return False

#Return true if the folder is empty or does not exists
def is_folder_empty(folder):
  try:
    return len(dbutils.fs.ls(folder)) == 0
  except:
    return True

# COMMAND ----------

import warnings

with warnings.catch_warnings():
    warnings.simplefilter('ignore', SyntaxWarning)
    warnings.simplefilter('ignore', DeprecationWarning)
    warnings.simplefilter('ignore', UserWarning)

# COMMAND ----------

import requests
import collections
import os
def download_file(url, destination):
    local_filename = url.split('/')[-1]
    # NOTE the stream=True parameter below
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        print('saving '+destination+'/'+local_filename)
        with open(destination+'/'+local_filename, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192): 
                # If you have chunk encoded response uncomment if
                # and set chunk_size parameter to None.
                #if chunk: 
                f.write(chunk)
    return local_filename
  
def download_file_from_git(dest, owner, repo, path):
    if not os.path.exists(dest):
      os.makedirs(dest)
    from concurrent.futures import ThreadPoolExecutor
    files = requests.get(f'https://api.github.com/repos/{owner}/{repo}/contents{path}').json()
    files = [f['download_url'] for f in files if 'NOTICE' not in f['name']]
    def download_to_dest(url):
         download_file(url, dest)
    with ThreadPoolExecutor(max_workers=10) as executor:
        collections.deque(executor.map(download_to_dest, files))
