## Contributing
#### Backlog
Required
* Support query duplication. Currently, beaker does not expose the ability to repeatedly run the same query so we just rerun the benchmark. This makes each benchmark run blocking.
  * Once merged, the loop logic will need to be changed also
* Check if there is a warehouse continuation token - I didn't see it in the API docs
* Change dist to reference main instead of nishant branch of beaker. We're waiting on PR merge for this
* Finish the README
  * Quickstart
  * Education about concurrency testing
  * Product notes/limitations

Nice to have
* Improve token authentication. Currently the user has to pass a PAT via a filter. This is not acceptable because the PAT is exposed via the job UI. Potential solutions include but are not limited to...
  1. Add secrets API functionality (instructions for how to pass token, add secret scope/key widgets, pass values as job parameters, read accordingly)
  2. Refactor beaker to support OAuth - [datbaricks-sql-connector example](https://github.com/databricks/databricks-sql-python/blob/b894605ba7fe2525f1cac830a330149b09b1d8c1/examples/interactive_oauth.py). Note that you'll have to migrate all `requests` API calls to the python SDK as well
  * [Relevant slack thread](https://databricks.slack.com/archives/C0J7QJHUJ/p1691539325013799)
* Support writing raw queries to UC volumes instead of DBFS
* determine if spark-sql-perf supports latest LTS DBR version or if we need to hardcode 12.2
* Make Beaker pip-installable within a Databricks notebook - [issue](https://github.com/goodwillpunning/beaker/issues/19)
* Add dashboarding and further analysis using Nishant's tool(s)

Needs Scoping
* Leverage beaker warehouse creation instead of current method

# Databricks TPC-DS Benchmarking Tool

This tool runs the TPC-DS benchmark on a Databricks SQL warehouse. The TPC-DS benchmark is a standardised method of evaluating the performance of decision support solutions, such as databases, data warehouses, and big data systems.

## Quick Start
#### 1 - Open the main notebook
TODO: screenshot
#### 2 - Determine your parameters
TODO: screenshot
TODO: documentation of the params
#### 3 - Click "Run All"

## Core Concepts
- **Concurrency**: The simulated number of users executing concurrent queries. It provides an insight into how well the system can handle multiple users executing queries at the same time.
- **Throughput**: The number of queries that the system can handle per unit of time. It is usually measured in queries per minute (QPM) and provides insignt into the speed and efficiency of the system.
- **TODO**

## Relevant Features
* The tool is cloud agnostic
* Authentication is automatically handled by the python SDK
* Benchmarking will be performed on the latest LTS DBR version
* Result cache enabled is hard-coded to false
* Table format is hard-coded to delta. Data writes are currently hard-coded to DBR 12.2, so if there are updates in Delta with newer DBR versions, they will not be included. This decision was made because spark-sql-perf did not run on > 12.2 DBR as of 2023-08-10.
* A new warehouse will be created based on user parameters. If a warehouse with the same name exists, the benchmarking tool will use that existing warehouse.
* Given Python's Global Processing Lock (GIL), increasing the number of cores will have diminshing returns. To hide complexity from the user while also bounding cost, the concurrency parameter will scale cluster count linearly up to 100 cores, then stop. Concurrency > 100 however is still supported via multithreading - it will just run on a maximum of 100 cores. Based on our default node type, this will be 25 workers.
* We are using [Databricks python-sql-connector](https://docs.databricks.com/en/dev-tools/python-sql-connector.html) to execute queries, but we are not fetching the results. The python-sql-connector has a built-in feature that retries with backoff when rate limit errors occur. Due to this retry mechanism, the actual performance of the system may be slightly faster than what the benchmarking results indicate.

### Limitations
* You must run this tool from a DBR version that supports default auth for the python sdk. This should be all LTS versions



