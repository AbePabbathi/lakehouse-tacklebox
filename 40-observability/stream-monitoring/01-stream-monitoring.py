# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime

# COMMAND ----------

#Date format
fmt = '%Y-%m-%d %H:%M:%S'

# Setup the paths for source and target
source_path = f"/databricks-datasets/definitive-guide/data/activity-data/"

target_table_path = f"/tmp/stream_monitoring/activity_data/"

target_table_checkpoint = f"/tmp/stream_monitoring/checkpoint_activity_data"

# COMMAND ----------



# Capture the data schema using a sample file
schema_df = spark.read.json(sourcePath+"part-00000-tid-730451297822678341-1dda7027-2071-4d73-a0e2-7fb6a91e1d1f-0-c000.json")
data_schema = schema_df.schema

# Create streaming dataframe
delta_stream_df = (spark
                  .readStream
                  .format("delta")
                  .option("maxFilesPerTrigger", 1)
                  .schema(data_schema)
                  .json(source_path)
                  .withColumnRenamed('Index', 'User_ID')
                  .selectExpr("*","cast(cast(Arrival_Time as double)/1000 as timestamp) as event_time")
                )

# Add audit fields                
final_stream_df = delta_stream_df.withColumn("ingested_dt",lit(datetime.now().strftime(fmt)))

display(final_stream_df)

# COMMAND ----------

from delta import tables as DeltaTables

# Delete the paths if they exist
reset_target = True
if reset_target:
  dbutils.fs.rm(target_table_path,True)
  dbutils.fs.rm(target_table_checkpoint,True)

  
if not DeltaTables.DeltaTable.isDeltaTable(spark, target_table_path):
  spark.createDataFrame(spark.sparkContext.emptyRDD(), target_table_schema)\
      .write\
      .format("delta")\
      .partitionBy("Device")\
      .mode("overwrite")\
      .save(target_table_path)
  
target_table = DeltaTables.DeltaTable.forPath(spark, target_table_path)

# COMMAND ----------

def foreach_batch_target(df, epoch_id):
    """Form a complex number.

    Keyword arguments:
    real -- the real part (default 0.0)
    imag -- the imaginary part (default 0.0)
    """
    global silver_table
    
    df = df.dropDuplicates(['Arrival_Time','Creation_Time','Device','User_ID'])
    
    df = df.withColumn("inserted_dt",lit(datetime.now().strftime(fmt)))
    (
      target_table
     .alias("oldData") 
     .merge(df.alias("newData"), "oldData.{0} = newData.{0} AND oldData.{1} = newData.{1} AND oldData.{2} = newData.{2}  AND oldData.{3} = newData.{3}"\
                    .format("Arrival_Time","Creation_Time", "Device", "User_ID"))
     .whenMatchedUpdateAll()
     .whenNotMatchedInsertAll()
     .execute()
    )
    return
    

# COMMAND ----------

foreach_batch_target.__doc__

# COMMAND ----------

from pyspark.sql.streaming import StreamingQueryListener
stream_event_list = []
def logEvent(event):
  columns = ["time","event"]
  lst = [(datetime.now().strftime(fmt),event)]
  stream_event_list.extend(lst)

# COMMAND ----------

from pyspark.sql.streaming import StreamingQueryListener
stream_event_list = []
def logEvent(event):
  columns = ["time","event"]
  lst = [(datetime.now().strftime(fmt),event)]
  stream_event_list.extend(lst)

# COMMAND ----------

from pyspark.sql.streaming import StreamingQueryListener


class ActivityStreamListener(StreamingQueryListener):
    def onQueryStarted(self, event):
   
        #print(datetime.now().strftime(fmt)+" Query Start logEvent executed")
        logEvent("Query Started")


    def onQueryProgress(self, event):
   
        #print(datetime.now().strftime(fmt)+" Query Running logEvent executed")
        logEvent(event.progress.prettyJson)

    def onQueryTerminated(self, event):
        
        #print(datetime.now().strftime(fmt)+" Query Terminated logEvent executed")
        logEvent("Query Terminated")


activity_listener = ActivityStreamListener()

# COMMAND ----------

spark.streams.addListener(activity_listener)

# COMMAND ----------

streaming_load = deltaStreamDF.writeStream.foreachBatch(foreach_batch_target).queryName("acitivty_streaming_02").start()   
