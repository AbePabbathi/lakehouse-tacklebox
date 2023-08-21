# Contributing
Contributions are welcome! Feel free to file an issue/PR or reach out to michael.berk@databricks.com.

### Potential Contributions (in order of importance)
* Support UC
  * Support writing raw queries to UC volumes instead of DBFS
  * Modify existing data write to handle conversion to UC-managed tables
* Look to improve data write performance. Some options include:
  * Improve threading of writes.
  * If the data (with a given set of configs) already exists, don't overwrite it.
  * Document baseline runtimes - [template](https://github.com/databricks/spark-sql-perf/blob/master/src/main/notebooks/tpcds_datagen.scala)
* Add dashboarding and further analysis using Nishant's tool(s)
* Determine if spark-sql-perf supports latest LTS DBR version or if we need to hardcode 12.2
* Make Beaker pip-installable within a Databricks notebook - [issue](https://github.com/goodwillpunning/beaker/issues/19)
* Leverage beaker warehouse creation instead of current method