# Databricks notebook source
# MAGIC %md 
# MAGIC ### A cluster has been created for this demo
# MAGIC To run this demo, just select the cluster `dbdemos-lakehouse-retail-c360-abraham_pabbathi` from the dropdown menu ([open cluster configuration](https://e2-demo-field-eng.cloud.databricks.com/#setting/clusters/0728-224149-hobul2i5/configuration)). <br />
# MAGIC *Note: If the cluster was deleted after 30 days, you can re-create it with `dbdemos.create_cluster('lakehouse-retail-c360')` or re-install the demo: `dbdemos.install('lakehouse-retail-c360')`*

# COMMAND ----------

# MAGIC %md
# MAGIC # Churn Prediction Inference - Batch or serverless real-time
# MAGIC
# MAGIC
# MAGIC With AutoML, our best model was automatically saved in our MLFlow registry.
# MAGIC
# MAGIC All we need to do now is use this model to run Inferences. A simple solution is to share the model name to our Data Engineering team and they'll be able to call this model within the pipeline they maintained. That's what we did in our Delta Live Table pipeline!
# MAGIC
# MAGIC Alternatively, this can be schedule in a separate job. Here is an example to show you how MLFlow can be directly used to retriver the model and run inferences.
# MAGIC
# MAGIC <!-- Collect usage data (view). Remove it to disable collection. View README for more details.  -->
# MAGIC <img width="1px" src="https://www.google-analytics.com/collect?v=1&gtm=GTM-NKQ8TT7&tid=UA-163989034-1&aip=1&t=event&ec=dbdemos&ea=VIEW&dp=%2F_dbdemos%2Flakehouse%2Flakehouse-retail-c360%2F04-Data-Science-ML%2F04.3-running-inference&cid=1444828305810485&uid=553895811432007">

# COMMAND ----------

# MAGIC %run ../_resources/02-create-churn-tables $reset_all_data=false $catalog="hive_metastore" $db=""

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##Deploying the model for batch inferences
# MAGIC
# MAGIC <img style="float: right; margin-left: 20px" width="600" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/retail/resources/images/churn_batch_inference.gif" />
# MAGIC
# MAGIC Now that our model is available in the Registry, we can load it to compute our inferences and save them in a table to start building dashboards.
# MAGIC
# MAGIC We will use MLFlow function to load a pyspark UDF and distribute our inference in the entire cluster. If the data is small, we can also load the model with plain python and use a pandas Dataframe.
# MAGIC
# MAGIC If you don't know how to start, Databricks can generate a batch inference notebook in just one click from the model registry: Open MLFlow model registry and click the "User model for inference" button!

# COMMAND ----------

# MAGIC %md ### Scaling inferences using Spark 
# MAGIC We'll first see how it can be loaded as a spark UDF and called directly in a SQL function:

# COMMAND ----------

#                                                                                        Stage/version
#                                                                      Model name              |
#                                                                           |                  |
predict_churn_udf = mlflow.pyfunc.spark_udf(spark, model_uri="models:/dbdemos_customer_churn/Production")
#We can use the function in SQL
spark.udf.register("predict_churn", predict_churn_udf)

# COMMAND ----------

# DBTITLE 1,Run inferences
columns = predict_churn_udf.metadata.get_input_schema().input_names()
import pyspark.sql.functions as F
spark.table(f'{database}.churn_features').withColumn("churn_prediction", predict_churn_udf(*columns)).display()

# COMMAND ----------

# MAGIC %md ### Pure pandas inference
# MAGIC If we have a small dataset, we can also compute our segment using a single node and pandas API:

# COMMAND ----------

model = mlflow.pyfunc.load_model("models:/dbdemos_customer_churn/Production")
df = spark.table(f'{database}.churn_features').select(*columns).limit(10).toPandas()
df['churn_prediction'] = model.predict(df)
df

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## Realtime model serving with Databricks serverless serving
# MAGIC
# MAGIC <img style="float: right; margin-left: 20px" width="800" src="https://raw.githubusercontent.com/QuentinAmbard/databricks-demo/main/retail/resources/images/lakehouse-retail/lakehouse-retail-churn-model-serving.gif" />
# MAGIC
# MAGIC Databricks also provides serverless serving.
# MAGIC
# MAGIC Click on model Serving, enable realtime serverless and your endpoint will be created, providing serving over REST api within a Click.
# MAGIC
# MAGIC Databricks Serverless offer autoscaling, including downscaling to zero when you don't have any traffic to offer best-in-class TCO while keeping low-latencies model serving.

# COMMAND ----------

dataset = spark.table(f'{database}.churn_features').select(*columns).limit(3).toPandas()
dataset

# COMMAND ----------

# DBTITLE 1,Call the REST API deployed using standard python
import os
import requests
import numpy as np
import pandas as pd
import json

def create_tf_serving_json(data):
  return {'inputs': {name: data[name].tolist() for name in data.keys()} if isinstance(data, dict) else data.tolist()}


def score_model(dataset):
  url = f'https://{dbutils.notebook.entry_point.getDbutils().notebook().getContext().browserHostName().get()}/model-endpoint/dbdemos_customer_churn/Production/invocations'
  headers = {'Authorization': f'Bearer {dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()}', 'Content-Type': 'application/json'}
  ds_dict = {'dataframe_split': dataset.to_dict(orient='split')} if isinstance(dataset, pd.DataFrame) else create_tf_serving_json(dataset)
  data_json = json.dumps(ds_dict, allow_nan=True)
  response = requests.request(method='POST', headers=headers, url=url, data=data_json)
  if response.status_code != 200:
    raise Exception(f'Request failed with status {response.status_code}, {response.text}')
  return response.json()

#score_model(dataset)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Next step: Leverage inferences and automate actions to increase revenue
# MAGIC
# MAGIC ## Automate action to reduce churn based on predictions
# MAGIC
# MAGIC We now have an end 2 end data pipeline analizing and predicting churn. We can now easily trigger actions to reduce the churn based on our business:
# MAGIC
# MAGIC - Send targeting email campaign to the customer the most likely to churn
# MAGIC - Phone campaign to discuss with our customers and understand what's going
# MAGIC - Understand what's wrong with our line of product and fixing it
# MAGIC
# MAGIC These actions are out of the scope of this demo and simply leverage the Churn prediction field from our ML model.
# MAGIC
# MAGIC ## Track churn impact over the next month and campaign impact
# MAGIC
# MAGIC Of course, this churn prediction can be re-used in our dashboard to analyse future churn and measure churn reduction. 
# MAGIC
# MAGIC The pipeline created with the Lakehouse will offer a strong ROI: it took us a few hours to setup this pipeline end 2 end and we have potential gain for $129,914 / month!
# MAGIC
# MAGIC <img width="800px" src="https://raw.githubusercontent.com/QuentinAmbard/databricks-demo/main/retail/resources/images/lakehouse-retail/lakehouse-retail-churn-dbsql-prediction-dashboard.png">
# MAGIC
# MAGIC <a href='/sql/dashboards/c559e07e-d338-4072-b37f-6f3854cce706'>Open the Churn prediction DBSQL dashboard</a> | [Go back to the introduction]($../00-churn-introduction-lakehouse)
