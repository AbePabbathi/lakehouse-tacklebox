# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # High Concurrency Testing Notebook with SQL Execution API
# MAGIC (Questions and Feedback to ambarish.dongaonkar@databricks.com or kyle.hale@databricks.com)
# MAGIC
# MAGIC This is an extension of Kyle's original notebook built for concurrency testing scenarios using [Databricks SQL Statement Execution REST APIs](https://docs.databricks.com/api/workspace/statementexecution). This notebook should be run on a single user, single node cluster. It runs the test on an existing warehouse.
# MAGIC
# MAGIC This notebook uses [httpx](https://www.python-httpx.org/) library to handle async HTTP requests. Make sure to update all necessary configuratinos in arguments section in cell#5. When running for the first time on a warehouse, please set "warm_up_run" to True. This will warm up the delta disk cache.
# MAGIC
# MAGIC This setup can be used to run any level of concurrency from available set of queries by setting numerical value to "concurrent_count" argument. If this value set to less than total number of queries, it will randomly pick queries from available set for each run. This will allow to replicate mixed workload pattern for concurrency testing.
# MAGIC
# MAGIC If you like to demo this notebook use for concurrency testing, set "demo_run" argument to True. This will use Databrick's public TPCH data set available at '/databricks-datasets/tpch/delta-001/' for building tables and running concurrency tests.
# MAGIC
# MAGIC For actual concurrency testing, you can upload all customer queries to Unity Catalog volume. Make sure to create one file per query and place all queries in one folder and use this folder location to update "db_volume" argument.
# MAGIC
# MAGIC This notebook will execute concurrent tests and load execution details of each test to table specfied in arguments section. This uses "wait_time_sec" duration to wait for all queries to complete. Update this value as per need if you know beforehand or after first test by observing query runtimes from query history that queries need longer time to complete. Once this time is elapsed, next step will gather all necessary details and load it to catalog table. This notebook will create "test_results" on top of results table to make it more easy to analyze results. This view uses 3 to 12 characters from begining of queries to get query id for reporting purpose. You can see example of this in demo queries as "-- Query#01". 
# MAGIC
# MAGIC If warehosue used for concurrency tests is also used for other purpose, please add POC query comment such as "POC-Query" for each query used for this testing. You can refer to demo queries for this. This will filter out any other queries from reporting view for ease of analysis.
# MAGIC
# MAGIC Each test run will create unique batch_id in result table. You can use this id for analysis of same queries in different tests or for reporting purpose.
# MAGIC
# MAGIC Please note, you can disable cacheing by setting use_cached_result to false in query. This notebook executes all queries via API Gateway and it will cache results. To avoid cacheing of such queries please add SELECT NOW(), * FROM _; to make your query non-deterministic and it will never return from cache.
# MAGIC
# MAGIC ### Instructions
# MAGIC
# MAGIC 1. Set all configurations in step#4 argeuments section.
# MAGIC 2. You can click run all option on notebook to run all steps and check last step for result of test run.
# MAGIC 3. If you like to run another test with same configuration, you can do run all or run all steps from step#10 onwards.
# MAGIC
# MAGIC
# MAGIC ### Troubleshooting
# MAGIC * If all steps executed successfully and you do not see any queries from query history, it must be access token is expired. It does not complain for expired tokens, therefore get new token and try again.
# MAGIC * Occasionally the async HTTP calls throw exceptions that cascade outside of the asyncio exeption catcher. You can ignore these and just re-run.
# MAGIC
# MAGIC ## Features
# MAGIC
# MAGIC * added demo functionality for use
# MAGIC * added dry run feature to build delta cache before running actual tests
# MAGIC * added batch_ids for each run which makes it easy to report and analyze
# MAGIC * writing result to table also showing result of last test in notebook
# MAGIC * added option to allow upload of queries to volume instead of putting all in notebook cells.
# MAGIC * automated to capture host url and token for usage

# COMMAND ----------

pip install httpx

# COMMAND ----------

import requests
from datetime import datetime
import json,  urllib3
from pyspark.sql import functions as f
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# COMMAND ----------

databricksURL = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().getOrElse(None)[8:]
myToken = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().getOrElse(None)
username = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()


# COMMAND ----------

# Set all configurations in below arguments as per necessary for concurrency tests

# If you are running this notebook in Databrick's demo workspace, sometimes auto-generated token do not work. In this case, create user access token manually and update it instead of using auto-generated token.

arguments = { 
  "loops": 1, # inceases duration of test, helps scaleout execution
  "users" : 1, # each loop kicks off users * number of queries in get_queries_to_run() list
  "host" : databricksURL, # "e2-demo-field-eng.cloud.databricks.com", # host for given Databrick's workspace
  "warehouse_id" : "xx",  # ID of DBSQL Serverless warehosue used for testing
  "username" : username, # User who's indentity will be used to execute queries using access token 
  "token" : myToken, # This is developer access token used to execute APIs 
  "concurrent_count" : 10, # This is level of concurrency to run test
  "db_catalog" : "ad_demo", # Catalog used to create result table and view
  "db_schema" : "demo", # Schema used to create result table and view
  "db_table" : "concurrency_test_results", # Name of table used to create result table
  "db_volume" : "/Volumes/ad_demo/demo/ad_vol", # Volume used to read all queries used for concurrency testing.
  "wait_time_sec" : 30, # Expected total duration all queries may need to compplete execution
  "warm_up_run" : False, # If true, this will run all queries available for testing to warm up warehouse (create delta/disk cache)
  "demo_run" : True # If true, this will use TPCH data set and queries to demo concurrency testing using this notebook
}

# COMMAND ----------

display(dbutils.fs.ls('/databricks-datasets/tpch/delta-001/'))

# COMMAND ----------

##
## Do Not Change
##
# This is data loading part for demo run. This will create necessary tables and load data.

spark.sql(
          f"""create table if not exists {arguments['db_catalog']}.{arguments['db_schema']}.customer as 
SELECT 
  _c0 AS c_custkey, 
  _c1 AS c_name, 
  _c2 AS c_address, 
  _c3 AS c_nationkey, 
  _c4 AS c_phone, 
  _c5 AS c_acctbal, 
  _c6 AS c_mktsegment, 
  _c7 AS c_comment
FROM read_files(
  '/databricks-datasets/tpch/data-001/customer/customer.tbl',
  format => 'csv',
  header => false,
  delimiter => "|")"""
      )
spark.sql(
    f"""create table if not exists {arguments['db_catalog']}.{arguments['db_schema']}.part AS 
    SELECT 
        _c0 AS p_partkey, 
        _c1 AS p_name, 
        _c2 AS p_mfgr, 
        _c3 AS p_brand, 
        _c4 AS p_type, 
        _c5 AS p_size, 
        _c6 AS p_container, 
        _c7 AS p_retailprice, 
        _c8 AS p_comment
    FROM read_files(
        '/databricks-datasets/tpch/data-001/part/part.tbl',
        format => 'csv',
        header => false,
        delimiter => "|"
    )"""
)
spark.sql(
    f"""create table if not exists {arguments['db_catalog']}.{arguments['db_schema']}.lineitem AS 
    SELECT 
        _c0 AS l_orderkey, 
        _c1 AS l_partkey, 
        _c2 AS l_suppkey, 
        _c3 AS l_linenumber, 
        _c4 AS l_quantity, 
        _c5 AS l_extendedprice, 
        _c6 AS l_discount, 
        _c7 AS l_tax, 
        _c8 AS l_returnflag, 
        _c9 AS l_linestatus, 
        _c10 AS l_shipdate, 
        _c11 AS l_commitdate, 
        _c12 AS l_receiptdate, 
        _c13 AS l_shipinstruct, 
        _c14 AS l_shipmode, 
        _c15 AS l_comment
    FROM read_files(
        '/databricks-datasets/tpch/data-001/lineitem/lineitem.tbl',
        format => 'csv',
        header => false,
        delimiter => "|"
    )"""
)
spark.sql(
    f"""create table if not exists {arguments['db_catalog']}.{arguments['db_schema']}.nation AS 
    SELECT 
        _c0 AS n_nationkey,
        _c1 AS n_name,
        _c2 AS n_regionkey,
        _c3 AS n_comment
    FROM read_files(
        '/databricks-datasets/tpch/data-001/nation/nation.tbl',
        format => 'csv',
        header => false,
        delimiter => "|"
    )"""
)
spark.sql(
    f"""create table if not exists {arguments['db_catalog']}.{arguments['db_schema']}.orders AS 
    SELECT 
        _c0 AS o_orderkey, 
        _c1 AS o_custkey, 
        _c2 AS o_orderstatus, 
        _c3 AS o_totalprice, 
        _c4 AS o_orderdate, 
        _c5 AS o_orderpriority, 
        _c6 AS o_clerk, 
        _c7 AS o_shippriority, 
        _c8 AS o_comment
    FROM read_files(
        '/databricks-datasets/tpch/data-001/orders/orders.tbl',
        format => 'csv',
        header => false,
        delimiter => "|"
    )"""
)
spark.sql(
    f"""create table if not exists {arguments['db_catalog']}.{arguments['db_schema']}.partsupp AS 
    SELECT 
        _c0 AS ps_partkey,
        _c1 AS ps_suppkey,
        _c2 AS ps_availqty,
        _c3 AS ps_supplycost,
        _c4 AS ps_comment
    FROM read_files(
        '/databricks-datasets/tpch/data-001/partsupp/partsupp.tbl',
        format => 'csv',
        header => false,
        delimiter => "|"
    )"""
)
spark.sql(
    f"""create table if not exists {arguments['db_catalog']}.{arguments['db_schema']}.region AS 
    SELECT 
        _c0 AS r_regionkey,
        _c1 AS r_name,
        _c2 AS r_comment
    FROM read_files(
        '/databricks-datasets/tpch/data-001/region/region.tbl',
        format => 'csv',
        header => false,
        delimiter => "|"
    )"""
)
spark.sql(
    f"""create table if not exists {arguments['db_catalog']}.{arguments['db_schema']}.supplier AS 
    SELECT 
    _c0 AS s_suppkey, 
    _c1 AS s_name, 
    _c2 AS s_address, 
    _c3 AS s_nationkey, 
    _c4 AS s_phone, 
    _c5 AS s_acctbal, 
    _c6 AS s_comment
    FROM read_files(
    '/databricks-datasets/tpch/data-001/supplier/supplier.tbl',
    format => 'csv',
    header => false,
    delimiter => "|"
    )"""
)

# COMMAND ----------

##
## Do Not Change
##
# This is query set used for demo run

async def get_demo_queries_to_run():
  """
  Add all the queries you wanted executed concurrently in the list below   
  """
  qs = [
  #Query 1
  """-- Query#01
  -- POC-Query
  -- TPC-H/TPC-R Pricing Summary Report Query (Q1)
  select
    l_returnflag,
    l_linestatus,
    sum(l_quantity) as sum_qty,
    sum(l_extendedprice) as sum_base_price,
    sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
    sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
    avg(l_quantity) as avg_qty,
    avg(l_extendedprice) as avg_price,
    avg(l_discount) as avg_disc,
    count(*) as count_order
  from
    lineitem
  where
    l_shipdate <= date '1998-12-01' - interval '90' day
  group by
    l_returnflag,
    l_linestatus
  order by
    l_returnflag,
    l_linestatus
  """
  #Query 2
  ,"""-- Query#02
  -- POC-Query
  -- TPC-H/TPC-R Minimum Cost Supplier Query (Q2)
  select
    s_acctbal,
    s_name,
    n_name,
    p_partkey,
    p_mfgr,
    s_address,
    s_phone,
    s_comment
  from
    part,
    supplier,
    partsupp,
    nation,
    region
  where
    p_partkey = ps_partkey
    and s_suppkey = ps_suppkey
    and p_size = 15
    and p_type like '%BRASS'
    and s_nationkey = n_nationkey
    and n_regionkey = r_regionkey
    and r_name = 'EUROPE'
    and ps_supplycost = (
      select
        min(ps_supplycost)
      from
        partsupp,
        supplier,
        nation,
        region
      where
        p_partkey = ps_partkey
        and s_suppkey = ps_suppkey
        and s_nationkey = n_nationkey
        and n_regionkey = r_regionkey
        and r_name = 'EUROPE'
    )
  order by
    s_acctbal desc,
    n_name,
    s_name,
    p_partkey
  LIMIT  100
  """
  #Query 3
  ,"""-- Query#03
  -- POC-Query
  -- TPC-H/TPC-R Shipping Priority Query (Q3)
  select
    l_orderkey,
    sum(l_extendedprice * (1 - l_discount)) as revenue,
    o_orderdate,
    o_shippriority
  from
    customer,
    orders,
    lineitem
  where
    c_mktsegment = 'BUILDING'
    and c_custkey = o_custkey
    and l_orderkey = o_orderkey
    and o_orderdate < date '1995-03-15'
    and l_shipdate > date '1995-03-15'
  group by
    l_orderkey,
    o_orderdate,
    o_shippriority
  order by
    revenue desc,
    o_orderdate
  LIMIT  10
  """
  #Query 4
  ,"""-- Query#04
  -- POC-Query
  -- TPC-H/TPC-R Order Priority Checking Query (Q4)
  select
    o_orderpriority,
    count(*) as order_count
  from
    orders
  where
    o_orderdate >= date '1993-07-01'
    and o_orderdate < date '1993-07-01' + interval '3' month
    and exists (
      select
        *
      from
        lineitem
      where
        l_orderkey = o_orderkey
        and l_commitdate < l_receiptdate
    )
  group by
    o_orderpriority
  order by
    o_orderpriority
  """
  #Query 5
  ,"""-- Query#05
  -- POC-Query
  -- TPC-H/TPC-R Local Supplier Volume Query (Q5)
  select
    n_name,
    sum(l_extendedprice * (1 - l_discount)) as revenue
  from
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
  where
    c_custkey = o_custkey
    and l_orderkey = o_orderkey
    and l_suppkey = s_suppkey
    and c_nationkey = s_nationkey
    and s_nationkey = n_nationkey
    and n_regionkey = r_regionkey
    and r_name = 'ASIA'
    and o_orderdate >= date '1994-01-01'
    and o_orderdate < date '1994-01-01' + interval '1' year
  group by
    n_name
  order by
    revenue desc
  """
  #Query 6
  ,"""-- Query#06
  -- POC-Query
  -- TPC-H/TPC-R Forecasting Revenue Change Query (Q6)
  select
    sum(l_extendedprice * l_discount) as revenue
  from
    lineitem
  where
    l_shipdate >= date '1994-01-01'
    and l_shipdate < date '1994-01-01' + interval '1' year
    and l_discount between .06 - 0.01 and .06 + 0.01
    and l_quantity < 24
  """
  #Query 7
  ,"""-- Query#07
  -- TPC-H/TPC-R Volume Shipping Query (Q7)
  select
    supp_nation,
    cust_nation,
    l_year,
    sum(volume) as revenue
  from
    (
      select
        n1.n_name as supp_nation,
        n2.n_name as cust_nation,
        extract(year from l_shipdate) as l_year,
        l_extendedprice * (1 - l_discount) as volume
      from
        supplier,
        lineitem,
        orders,
        customer,
        nation n1,
        nation n2
      where
        s_suppkey = l_suppkey
        and o_orderkey = l_orderkey
        and c_custkey = o_custkey
        and s_nationkey = n1.n_nationkey
        and c_nationkey = n2.n_nationkey
        and (
          (n1.n_name = 'FRANCE' and n2.n_name = 'GERMANY')
          or (n1.n_name = 'GERMANY' and n2.n_name = 'FRANCE')
        )
        and l_shipdate between date '1995-01-01' and date '1996-12-31'
    ) as shipping
  group by
    supp_nation,
    cust_nation,
    l_year
  order by
    supp_nation,
    cust_nation,
    l_year
  """
  #Query 8
  ,"""-- Query#08
  -- POC-Query
  -- TPC-H/TPC-R National Market Share Query (Q8)
  select
    o_year,
    sum(case
      when nation = 'BRAZIL' then volume
      else 0
    end) / sum(volume) as mkt_share
  from
    (
      select
        extract(year from o_orderdate) as o_year,
        l_extendedprice * (1 - l_discount) as volume,
        n2.n_name as nation
      from
        part,
        supplier,
        lineitem,
        orders,
        customer,
        nation n1,
        nation n2,
        region
      where
        p_partkey = l_partkey
        and s_suppkey = l_suppkey
        and l_orderkey = o_orderkey
        and o_custkey = c_custkey
        and c_nationkey = n1.n_nationkey
        and n1.n_regionkey = r_regionkey
        and r_name = 'AMERICA'
        and s_nationkey = n2.n_nationkey
        and o_orderdate between date '1995-01-01' and date '1996-12-31'
        and p_type = 'ECONOMY ANODIZED STEEL'
    ) as all_nations
  group by
    o_year
  order by
    o_year
  """
  #Query 9
  ,"""-- Query#09
  -- POC-Query
  -- TPC-H/TPC-R Product Type Profit Measure Query (Q9)
  select
    nation,
    o_year,
    sum(amount) as sum_profit
  from
    (
      select
        n_name as nation,
        extract(year from o_orderdate) as o_year,
        l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount
      from
        part,
        supplier,
        lineitem,
        partsupp,
        orders,
        nation
      where
        s_suppkey = l_suppkey
        and ps_suppkey = l_suppkey
        and ps_partkey = l_partkey
        and p_partkey = l_partkey
        and o_orderkey = l_orderkey
        and s_nationkey = n_nationkey
        and p_name like '%green%'
    ) as profit
  group by
    nation,
    o_year
  order by
    nation,
    o_year desc
  """
  #Query 10
  ,"""-- Query#10
  -- POC-Query
  -- TPC-H/TPC-R Returned Item Reporting Query (Q10)
  select
    c_custkey,
    c_name,
    sum(l_extendedprice * (1 - l_discount)) as revenue,
    c_acctbal,
    n_name,
    c_address,
    c_phone,
    c_comment
  from
    customer,
    orders,
    lineitem,
    nation
  where
    c_custkey = o_custkey
    and l_orderkey = o_orderkey
    and o_orderdate >= date '1993-10-01'
    and o_orderdate < date '1993-10-01' + interval '3' month
    and l_returnflag = 'R'
    and c_nationkey = n_nationkey
  group by
    c_custkey,
    c_name,
    c_acctbal,
    c_phone,
    n_name,
    c_address,
    c_comment
  order by
    revenue desc
  LIMIT  20
  """
  #Query 11
  ,"""-- Query#11
  -- POC-Query
  -- TPC-H/TPC-R Important Stock Identification Query (Q11)
  select
    ps_partkey,
    sum(ps_supplycost * ps_availqty) as value
  from
    partsupp,
    supplier,
    nation
  where
    ps_suppkey = s_suppkey
    and s_nationkey = n_nationkey
    and n_name = 'GERMANY'
  group by
    ps_partkey having
      sum(ps_supplycost * ps_availqty) > (
        select
          sum(ps_supplycost * ps_availqty) * 0.0001
        from
          partsupp,
          supplier,
          nation
        where
          ps_suppkey = s_suppkey
          and s_nationkey = n_nationkey
          and n_name = 'GERMANY'
      )
  order by
    value desc
  """
  #Query 12
  ,"""-- Query#12
  -- POC-Query
  -- TPC-H/TPC-R Shipping Modes and Order Priority Query (Q12)
  select
    l_shipmode,
    sum(case
      when o_orderpriority = '1-URGENT'
        or o_orderpriority = '2-HIGH'
        then 1
      else 0
    end) as high_line_count,
    sum(case
      when o_orderpriority <> '1-URGENT'
        and o_orderpriority <> '2-HIGH'
        then 1
      else 0
    end) as low_line_count
  from
    orders,
    lineitem
  where
    o_orderkey = l_orderkey
    and l_shipmode in ('MAIL', 'SHIP')
    and l_commitdate < l_receiptdate
    and l_shipdate < l_commitdate
    and l_receiptdate >= date '1994-01-01'
    and l_receiptdate < date '1994-01-01' + interval '1' year
  group by
    l_shipmode
  order by
    l_shipmode
  """
  #Query 13
  ,"""-- Query#13
  -- POC-Query
  -- TPC-H/TPC-R Customer Distribution Query (Q13)
  select
    c_count,
    count(*) as custdist
  from
    (
      select
        c_custkey,
        count(o_orderkey)
      from
        customer left outer join orders on
          c_custkey = o_custkey
          and o_comment not like '%special%requests%'
      group by
        c_custkey
    ) as c_orders (c_custkey, c_count)
  group by
    c_count
  order by
    custdist desc,
    c_count desc
  """
  #Query 14
  ,"""-- Query#14
  -- POC-Query
  -- TPC-H/TPC-R Promotion Effect Query (Q14)
  select
    100.00 * sum(case
      when p_type like 'PROMO%'
        then l_extendedprice * (1 - l_discount)
      else 0
    end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue
  from
    lineitem,
    part
  where
    l_partkey = p_partkey
    and l_shipdate >= date '1995-09-01'
    and l_shipdate < date '1995-09-01' + interval '1' month
  """                        
  #Query 15
  ,"""-- Query#15
  -- POC-Query
  select
    sum(l_extendedprice) / 7.0 as avg_yearly
  from
    lineitem,
    part
  where
    p_partkey = l_partkey
    and p_brand = 'Brand#23'
    and p_container = 'MED BOX'
    and l_quantity < (
      select
        0.2 * avg(l_quantity)
      from
        lineitem
      where
        l_partkey = p_partkey
    )  
  """
  #Query 16
  ,"""-- Query#16
  -- POC-Query
  -- TPC-H/TPC-R Parts/Supplier Relationship Query (Q16)
  select
    p_brand,
    p_type,
    p_size,
    count(distinct ps_suppkey) as supplier_cnt
  from
    partsupp,
    part
  where
    p_partkey = ps_partkey
    and p_brand <> 'Brand#45'
    and p_type not like 'MEDIUM POLISHED%'
    and p_size in (49, 14, 23, 45, 19, 3, 36, 9)
    and ps_suppkey not in (
      select
        s_suppkey
      from
        supplier
      where
        s_comment like '%Customer%Complaints%'
    )
  group by
    p_brand,
    p_type,
    p_size
  order by
    supplier_cnt desc,
    p_brand,
    p_type,
    p_size
  """
  #Query 17
  ,"""-- Query#17
  -- POC-Query
  -- TPC-H/TPC-R Small-Quantity-Order Revenue Query (Q17)
  select
    sum(l_extendedprice) / 7.0 as avg_yearly
  from
    lineitem,
    part
  where
    p_partkey = l_partkey
    and p_brand = 'Brand#23'
    and p_container = 'MED BOX'
    and l_quantity < (
      select
        0.2 * avg(l_quantity)
      from
        lineitem
      where
        l_partkey = p_partkey
    )
  """
  #Query 18
  ,"""-- Query#18
  -- POC-Query
  -- TPC-H/TPC-R Large Volume Customer Query (Q18)
  select
    c_name,
    c_custkey,
    o_orderkey,
    o_orderdate,
    o_totalprice,
    sum(l_quantity) AS sum_l_quantity
  from
    customer,
    orders,
    lineitem
  where
    o_orderkey in (
      select
        l_orderkey
      from
        lineitem
      group by
        l_orderkey having
          sum(l_quantity) > 300
    )
    and c_custkey = o_custkey
    and o_orderkey = l_orderkey
  group by
    c_name,
    c_custkey,
    o_orderkey,
    o_orderdate,
    o_totalprice
  order by
    o_totalprice desc,
    o_orderdate
  LIMIT  100
  """
  #Query 19
  ,"""-- Query#19
  -- POC-Query
  -- TPC-H/TPC-R Discounted Revenue Query (Q19)
  select
    sum(l_extendedprice* (1 - l_discount)) as revenue
  from
    lineitem,
    part
  where
    (
      p_partkey = l_partkey
      and p_brand = 'Brand#12'
      and p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
      and l_quantity >= 1 and l_quantity <= 1 + 10
      and p_size between 1 and 5
      and l_shipmode in ('AIR', 'AIR REG')
      and l_shipinstruct = 'DELIVER IN PERSON'
    )
    or
    (
      p_partkey = l_partkey
      and p_brand = 'Brand#23'
      and p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
      and l_quantity >= 10 and l_quantity <= 10 + 10
      and p_size between 1 and 10
      and l_shipmode in ('AIR', 'AIR REG')
      and l_shipinstruct = 'DELIVER IN PERSON'
    )
    or
    (
      p_partkey = l_partkey
      and p_brand = 'Brand#34'
      and p_container in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
      and l_quantity >= 20 and l_quantity <= 20 + 10
      and p_size between 1 and 15
      and l_shipmode in ('AIR', 'AIR REG')
      and l_shipinstruct = 'DELIVER IN PERSON'
    )
  """
  #Query 20
  ,"""-- Query#20
  -- POC-Query
  -- TPC-H/TPC-R Potential Part Promotion Query (Q20)
  select
    s_name,
    s_address
  from
    supplier,
    nation
  where
    s_suppkey in (
      select
        ps_suppkey
      from
        partsupp
      where
        ps_partkey in (
          select
            p_partkey
          from
            part
          where
            p_name like 'forest%'
        )
        and ps_availqty > (
          select
            0.5 * sum(l_quantity)
          from
            lineitem
          where
            l_partkey = ps_partkey
            and l_suppkey = ps_suppkey
            and l_shipdate >= date '1994-01-01'
            and l_shipdate < date '1994-01-01' + interval '1' year
        )
    )
    and s_nationkey = n_nationkey
    and n_name = 'CANADA'
  order by
    s_name
  """    
  #Query 21
  ,"""-- Query#21
  -- POC-Query
  -- TPC-H/TPC-R Suppliers Who Kept Orders Waiting Query (Q21)
  select
    s_name,
    count(*) as numwait
  from
    supplier,
    lineitem l1,
    orders,
    nation
  where
    s_suppkey = l1.l_suppkey
    and o_orderkey = l1.l_orderkey
    and o_orderstatus = 'F'
    and l1.l_receiptdate > l1.l_commitdate
    and exists (
      select
        *
      from
        lineitem l2
      where
        l2.l_orderkey = l1.l_orderkey
        and l2.l_suppkey <> l1.l_suppkey
    )
    and not exists (
      select
        *
      from
        lineitem l3
      where
        l3.l_orderkey = l1.l_orderkey
        and l3.l_suppkey <> l1.l_suppkey
        and l3.l_receiptdate > l3.l_commitdate
    )
    and s_nationkey = n_nationkey
    and n_name = 'SAUDI ARABIA'
  group by
    s_name
  order by
    numwait desc,
    s_name
  LIMIT  100
  """    
  #Query 22
  ,"""-- Query#22
  -- POC-Query
  -- TPC-H/TPC-R Global Sales Opportunity Query (Q22)
  select
    cntrycode,
    count(*) as numcust,
    sum(c_acctbal) as totacctbal
  from
    (
      select
        substring(c_phone, 0, 2) as cntrycode,
        c_acctbal
      from
        customer
      where
        substring(c_phone, 0, 2) in
          ('13', '31', '23', '29', '30', '18', '17')
        and c_acctbal > (
          select
            avg(c_acctbal)
          from
            customer
          where
            c_acctbal > 0.00
            and substring(c_phone, 0, 2) in
              ('13', '31', '23', '29', '30', '18', '17')
        )
        and not exists (
          select
            *
          from
            orders
          where
            o_custkey = c_custkey
        )
    ) as custsale
  group by
    cntrycode
  order by
    cntrycode
  """    
    ]
  return qs

# Collect queries into a list for testing purposes  
queries = await get_demo_queries_to_run()

# COMMAND ----------

##
## Do Not Change
##
# Setup your test queries

async def get_queries_to_run():
  """
  Add all the queries you wanted executed concurrently in the list below   
  """
  qs = []

  if arguments['demo_run']:
    qs = ""
  else:
    for f in dbutils.fs.ls(arguments['db_volume']):
        #print(f[0])
        df = spark.read.text(f[0], wholetext=True)
        qs.append(df.select('value').collect()[0][0])

  #display(len(qs))
  return qs

# Collect queries into a list for testing purposes  
queries = await get_queries_to_run()

# COMMAND ----------

##
## Do Not Change
##
# Functions to run queries async

import asyncio
import httpx
import random

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
    "on_wait_timeout": "CONTINUE",
    "catalog": arguments['db_catalog'],
    "schema": arguments['db_schema']
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
  queries = []
  query_ids = []
  all_queries = []
  
  if arguments['demo_run']:
    all_queries = await get_demo_queries_to_run() 
  else:
    all_queries = await get_queries_to_run()

  max = len(all_queries)
  print(max)

  if arguments['warm_up_run']:
    queries = all_queries 
  else:
    if max != arguments["concurrent_count"]:
      while len(query_ids) < arguments["concurrent_count"]:
          x = random.randint(1, max - 1) ## error out when only one query in list
          if x not in query_ids:
              query_ids.append(x)
              queries.append(all_queries[x])
      print(query_ids)
    else:
      random.shuffle(all_queries)
      queries = all_queries  

  print(len(queries))

  #print(queries)
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
# Run the concurrency test. You can note batch_id from output of this cell for your analysis purpose.
import time

MIN_DATE = datetime.now()
batch_id = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d-%H-%M-%S')

await query_loops(arguments)

print(f'Batch ID: {batch_id}')

# COMMAND ----------

##
## Do Not Change
##
# This will sleeping time to allow all queries in concurrent test to complete execution.

time.sleep(arguments['wait_time_sec']);

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
          f"SELECT MAX(query_start_time_ms) AS latest FROM {arguments['db_catalog'].arguments['db_schema'].arguments['db_table']}"
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
          #"user_ids" : [user_id] ,
          "warehouse_ids" : arguments['warehouse_id']
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
      f"https://{arguments['host']}/api/2.0/sql/history/queries",
      headers={"Authorization": f"Bearer {arguments['token']}"},
      json=query_filter,
  )

  if response.status_code == 200:
      print("Query successful, returning results")
  else:
      print("Error performing query")
      print(response)
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
      f"https://{arguments['host']}/api/2.0/sql/history/queries",
      headers={"Authorization": f"Bearer {arguments['token']}"},
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
              f.from_utc_timestamp(
                  f.from_unixtime(
                      f.col("query_start_time_ms") / 1000, "yyyy-MM-dd HH:mm:ss"
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
# Run the SQL Query history extraction process and load it to results table

import time

# Set the start and end time of test
start, end = calc_start_end(hydrate=True)
# Set the query filter
query_filter = calc_query_filter(start, end, arguments['username'])

# Get the first page results
r = get_first_response()
#print(r.json())
results = r.json()["res"]

# Get the rest of the results pages
res = paginate(r)

# Add the paginated results to the initial results
results.extend(res)
print("Number of query result rows: "+str(len(results)))

# Create Spark Dataframe
df = results_to_df(results)

#new_df = df.withColumn('batch_id',f.format_string(datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d-%H-%M-%S')))
new_df = df.withColumn('batch_id',f.format_string(batch_id))

# Persist the dataframe to a delta lake table
# NOTE: table is overwritten each time you run this notebook
results_table = arguments['db_catalog'] + "." + arguments['db_schema'] + "." + arguments['db_table']
write_table(new_df, results_table, mode="append") #

# COMMAND ----------

##
## Do Not Change
##
# This step will create view for easy analysis of results

spark.sql(
    f"""create or replace view {arguments['db_catalog']}.{arguments['db_schema']}.test_results as 
    select 
        batch_id, 
        warehouse_id, 
        query_id, 
        executed_as_user_name, 
        trim(substr(query_text, 3, 12)) as query_no, 
        query_start_time_ms, 
        execution_end_time_ms, 
        timestampdiff(SECOND, to_timestamp(query_start_time_ms), to_timestamp(execution_end_time_ms)) as run_time_ms, 
        (timestampdiff(SECOND, to_timestamp(query_start_time_ms), to_timestamp(execution_end_time_ms))/1000)%60 as run_time_seconds  
    from {arguments['db_catalog']}.{arguments['db_schema']}.concurrency_test_results
    -- 
    order by batch_id
    """
)

#where query_text like '% || {arguments['poc_keyword']}%'

# COMMAND ----------

##
## Do Not Change
##
# This will show test results from latest run. If you observe any nulls in time/duration, increase time in "wait_time_sec" and rerrun test

display(spark.sql(f"""select * from {arguments['db_catalog']}.{arguments['db_schema']}.test_results  where batch_id = '{batch_id}'"""))

