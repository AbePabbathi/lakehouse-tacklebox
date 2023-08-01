# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # High Concurrency Testing Notebook with SQL Execution API
# MAGIC (Questions and Feedback to kyle.hale@databricks.com)
# MAGIC
# MAGIC ### Instructions
# MAGIC
# MAGIC 1. Install **httpx** library to handle async HTTP requests in Cmd 2.
# MAGIC 2. Set host, warehouse_id, and token parameters in Cmd 3. Set username to email of user whose token is running queries to pull data for analysis. 
# MAGIC 3. Set the new_queries_table in Cmd 3 to a table name in which you want to capture the query results. NOTE: this table is overwrite with each run of this notebook
# MAGIC 4. Set the query list in Cmd 4. Ideas: random parameterization, non-deterministic queries to avoid QRC, different levels of complexity ...
# MAGIC 5. Set the testing requirements like number of *loops* and *users* in Cmd 5, or leave as defaults.
# MAGIC 6. Run all.
# MAGIC
# MAGIC
# MAGIC Cmd 8 runs the queries concurrently
# MAGIC
# MAGIC Once Cmd 8 completes, the outputs of commands 14 and 15 should give you:
# MAGIC
# MAGIC * Average duration and Standard Deviation of each query in seconds
# MAGIC * Total Run time of all queries
# MAGIC * Average throughput to endpoint (queries per minute)
# MAGIC * Duration over time (basically showing disk cache (+QRC) + scaleout)
# MAGIC
# MAGIC ### Troubleshooting
# MAGIC
# MAGIC Occasionally the async HTTP calls throw exceptions that cascade outside of the asyncio exeption catcher. You can ignore these and just re-run, still investigating a permanent fix.

# COMMAND ----------

pip install httpx

# COMMAND ----------

import requests
from datetime import datetime
import json,  urllib3
from pyspark.sql import functions as F
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Setup the connection to Databricks Workspace and SQL Warehouse 
host = "adb-xxxxxxxxxxxxxxxxxx.azuredatabricks.net" # dbutils.entry_point.getDbutils().notebook().getContext().apiUrl().get() # command to get current workspace
warehouse_id = "aaxxxxxxxxxxxx" # Shared Endpoint on F-E-E
token = "dapixxxxxxxxxxxxxxxxxxx"
new_queries_table = "default.concurrency_test_queries"
username = 'user.name@example.com' # user whose token is running the queries


# COMMAND ----------

# Setup your test queries

async def get_queries_to_run():
  """
  Add all the queries you wanted executed concurrently in the list below   
  """
  qs = [
  #Query 1
  "select count(*) from samples.nyctaxi.trips where pickup_zip = '10110'" 
  #Query 2
  ,"select count(*) from samples.nyctaxi.trips where pickup_zip = '10009'"
  #Query 3
  ,"select count(*) from samples.nyctaxi.trips where pickup_zip = '10023'"
  ]
  return qs

# Collect queries into a list for testing purposes  
queries = await get_queries_to_run()

# COMMAND ----------

# Set the Testing loops and users variables based on your test requirements

arguments = { 
  "loops": 2, # inceases duration of test, helps show QRC and scaleout
  "users" : 6, # each loop kicks off users * number of queries in get_queries_to_run() list
  "host" : host, 
  "warehouse_id" : warehouse_id, 
  "token" : token
}

# COMMAND ----------

##
## Do Not Change
##
# Get the internal ID associated with the User Name

response = requests.get(
  f"https://{host}/api/2.0/preview/scim/v2/Users?filter=userName+eq+{username}",
  headers={"Authorization": f"Bearer {token}"}
)
USER_ID = json.loads(response.text)["Resources"][0]["id"]


# Set the start time of the test, to help with capturing query run statistics
MIN_DATE = datetime.now()


# If the above errors out, check the token to ensure its valid

# COMMAND ----------

##
## Do Not Change
##
# Functions to run queries async

import asyncio
import httpx

# Run async query function
async def runquery_async( query, query_id, arguments, client):
  """
    Run async query function

    Params :
    query : Query string to be run
    query_id : Id created using query sequence number and user number
    arguments : Dict containing test requirements like user count etc.
    client : httpx async client
  """
  # Setup client parameters    
  headers = {"Authorization": f"Bearer {arguments['token']}"}
  json = {
    "statement" : query,
    "warehouse_id" : arguments["warehouse_id"],
    "wait_timeout" : "30s",
    "on_wait_timeout": "CONTINUE"
  }
  r = await client.post(f"https://{arguments['host']}/api/2.0/sql/statements", headers=headers, json=json)

# Run all queries for each user
async def runuser_async( userid, arguments, client):
  """
    Run async query or each user

    Params :
    userid : Loop number - User number
    arguments : Dict containing test requirements like user count etc.
    client : httpx async client
  """
  queries = await get_queries_to_run()
  tasks = []
  for query_num in range(len(queries)):
    query_id = f"q {query_num} - u {userid}"
    tasks.append(asyncio.create_task(runquery_async(queries[query_num], query_id, arguments, client)))
  r = await asyncio.gather(*tasks, return_exceptions=True)
  print(f'User: {userid} is done running all queries')

# Run all queries for all users in each loop
async def query_loops(arguments):
  """
    Run query loops function

    Params :
    arguments : Dict containing test requirements like user count etc.
  """
  # Create httpx client
  client = httpx.AsyncClient()

  #Loop through each loop and user
  for loopnum in range(1, (arguments["loops"] + 1)):
    userlist = []
    for users in range(1, (arguments["users"] + 1) ): 
      userid = f"{loopnum}-{users}"
      userlist.append(runuser_async( userid, arguments, client))
    u = await asyncio.gather(*userlist)
    print(f'Loop: {loopnum} is done')
  await client.aclose()

# COMMAND ----------

##
## Do Not Change
##
# Run the concurrency test

await query_loops(arguments)

# COMMAND ----------

##
## Do Not Change
##
# Functions to extract query run history

# Calculate Start and End Time of the test
def calc_start_end(hydrate=False):
  """
    Calculate Start and End Time of the test

    Params :
    hydrate : Flag
  """
  if hydrate:
      start = MIN_DATE
  else:
      df = spark.sql(
          f"SELECT MAX(query_start_time_ms) AS latest FROM {new_queries_table}"
      )
      start = datetime.fromtimestamp(df.toPandas()["latest"][0] / 1000)

  end = datetime.now()
  return start, end

# Return the query filter for each user
def calc_query_filter(start, end, user_id):
  """
    Calculate Start and End Time of each equery

    Params :
    start : start time
    end : end time
    user_id : user id
  """
  query_filter = {
      "filter_by": {
          "query_start_time_range": {
              "start_time_ms": str(int(start.timestamp() * 1000)),
              "end_time_ms": str(int(end.timestamp() * 1000)),
          },
          "user_ids" : [user_id] ,
          "warehouse_ids" : [warehouse_id]
      },
      "max_results": 1000
    }
  return query_filter

# Get the first page of query run details
def get_first_response():
  """
    Get the first page of query run details
  """
  response = requests.get(
      f"https://{host}/api/2.0/sql/history/queries",
      headers={"Authorization": f"Bearer {token}"},
      json=query_filter,
  )

  if response.status_code == 200:
      print("Query successful, returning results")
  else:
      print("Error performing query")
  return response

# Paginate through the rest of the query results
def paginate(res):
  """
    Paginate through the rest of the pages
    Params:
    res: response from the first page results
  """
  results = []
  count = 0
  #Check if there's a second page of results
  while res.json().get("has_next_page"):
    page_token = res.json()["next_page_token"]
    new_query_filter = dict(page_token=page_token, max_results=1000)

    res = requests.get(
      f"https://{host}/api/2.0/sql/history/queries",
      headers={"Authorization": f"Bearer {token}"},
      json=new_query_filter
    )
    results.extend(res.json()["res"])
    count += 1
    if count == 20:
      print(f"Stopping after {(count + 1) * 1000} results")
      break

  return results

# Convert json results into spark dataframe
def results_to_df(res):
  try:
      rdd = sc.parallelize(res).map(json.dumps)
      raw_df = (
          spark.read.json(rdd)
          .withColumn(
              "query_start_ts_pst",
              F.from_utc_timestamp(
                  F.from_unixtime(
                      F.col("query_start_time_ms") / 1000, "yyyy-MM-dd HH:mm:ss"
                  ),
                  "PST",
              ),
          )
      )
      print("Dataframe of all queries ready")
      return raw_df
  except Exception as aex:
      raise Exception("Error parsing JSON response to DataFrame") from aex
  
def write_table(df, table_name, mode):
  print(f"Writing table: {table_name}")
  df.write.format("delta").mode(mode).option("mergeSchema", "true").saveAsTable(
      table_name
  )
  print("Done")

# COMMAND ----------

##
## Do Not Change
##
# Run the SQL Query history extraction process

# Set the start and end time of test
start, end = calc_start_end(hydrate=True)
# Set the query filter
query_filter = calc_query_filter(start, end, USER_ID)

# Get the first page results
r = get_first_response()
results = r.json()["res"]

# Get the rest of the results pages
res = paginate(r)

# Add the paginated results to the initial results
results.extend(res)
print("Number of query result rows: "+str(len(results)))

# Create Spark Dataframe
df = results_to_df(results)

# Persist the dataframe to a delta lake table
# NOTE: table is overwritten each time you run this notebook
write_table(df, new_queries_table, mode="overwrite")

# COMMAND ----------

# Ensure all results have been collected
# This number should match the number shown by the above cell
print("Number of query results rows should be equal to :"+str(arguments["users"]*arguments["loops"]*len(queries)))

# COMMAND ----------

# Assign the query history table name to a parameter to be used in the queries below
spark.conf.set('da.table_name', new_queries_table)
spark.conf.get("da.table_name")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Exploratory Analysis of the Data
# MAGIC SELECT * FROM ${da.table_name}

# COMMAND ----------

# MAGIC %sql
# MAGIC -- High level metrics by query
# MAGIC WITH q AS (
# MAGIC SELECT 
# MAGIC    date_format(query_start_ts_pst, 'yyyy-MM-dd') query_run_date,
# MAGIC    concat(hour(query_start_ts_pst),':',minute(query_start_ts_pst)) as hour_mins,
# MAGIC    left(replace(replace(query_text, " ", ""), chr(13), ""),378) as query_string,
# MAGIC    query_end_time_ms,
# MAGIC    query_start_time_ms,
# MAGIC    rows_produced,
# MAGIC    round(duration/1000) duration_secs
# MAGIC FROM ${da.table_name}
# MAGIC WHERE
# MAGIC   status = 'FINISHED'
# MAGIC   and is_final = 'true'
# MAGIC   and statement_type = 'SELECT'
# MAGIC   and query_start_ts_pst > timestampadd(MINUTE, -100, from_utc_timestamp(now(), 'PST')))
# MAGIC SELECT 
# MAGIC query_run_date,
# MAGIC query_string,
# MAGIC count(1) as query_run_count,
# MAGIC cast(avg(duration_secs) as int) as avg_duration_secs,
# MAGIC cast(stddev(duration_secs) as int) as stddev_duration_secs,
# MAGIC cast(max(cast(query_end_time_ms/1000 as timestamp)) - min(cast(query_start_time_ms/1000 as timestamp)) as numeric) as total_run_secs
# MAGIC FROM 
# MAGIC q
# MAGIC GROUP BY 1,2

# COMMAND ----------

# MAGIC %sql
# MAGIC -- High level metrics for the complete test
# MAGIC WITH q AS (
# MAGIC SELECT 
# MAGIC    date_format(query_start_ts_pst, 'yyyy-MM-dd') query_run_date,
# MAGIC    concat(hour(query_start_ts_pst),':',minute(query_start_ts_pst)) as hour_mins,
# MAGIC    left(replace(replace(query_text, " ", ""), chr(13), ""),378) as query_string,
# MAGIC    query_end_time_ms,
# MAGIC    query_start_time_ms,
# MAGIC    rows_produced,
# MAGIC    round(duration/1000) duration_secs
# MAGIC FROM ${da.table_name}
# MAGIC WHERE
# MAGIC   status = 'FINISHED'
# MAGIC   and is_final = 'true'
# MAGIC   and statement_type = 'SELECT'
# MAGIC   and query_start_ts_pst > timestampadd(MINUTE, -100, from_utc_timestamp(now(), 'PST')))
# MAGIC SELECT 
# MAGIC hour_mins,
# MAGIC count(1) as total_run_count,
# MAGIC cast(avg(duration_secs) as int) as avg_duration_secs,
# MAGIC cast(stddev(duration_secs) as int) as stddev_duration_secs,
# MAGIC cast(max(cast(query_end_time_ms/1000 as timestamp)) - min(cast(query_start_time_ms/1000 as timestamp)) as numeric) as total_run_secs
# MAGIC FROM q
# MAGIC GROUP BY 1
