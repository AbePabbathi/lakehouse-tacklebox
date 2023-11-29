# Databricks SQL Query Replay Tool

This tool is aimed to help users evaluate performance of different warehouses by replaying a set of query history from one warehouse to another.

## Notebooks

* `00-Functions` is the notebook containing the python class
* `01-Query_Replay_Tool` is the notebook that is used to execute the test

## Requirements

User need access to query history system tables `system.query.history` in order to extract the queries and start time for the test.

## Usage

Users need to set the following parameters

* `test_name`: Test Identifier
* `result_catalog` and `result_schema`: The schema where the test results will be written to
* `token`: A Databricks PAT that will be used to launched those queries
* `source_warehouse_id`: The warehouse ID where the origiated queries were submitted
* `source_start_time`: The start time to filter for queries
* `source_end_time`: The end time to filter for queries

And here are a number of optional configuration for the target warehouse where the queries will be replayed to (see [Create Warehouse API doc](https://docs.databricks.com/api/workspace/warehouses/create) for more details).

* `target_warehouse_size`
* `target_warehouse_max_num_clusters`
* `target_warehouse_type`
* `target_warehouse_serverless`
* `target_warehouse_custom_tags`
* `target_warehouse_channel`

The replay can be executed as follows.

```python
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
```

Once the test is completed, it will return the `test_id` which can be used to retrieve the result. 

Here are other functionality within the `QueryReplayTest`

* `replay_test.query_df` returns the queries that were used for the test
* `replay_test.show_run` returns the metadata of the test
* `show_run_details` returns the corresponding statement_id's for all the queries we ran
* `query_results` returnrs the result comparing source details against the test output

All the output data are written to the nominated schema in tables `query_replay_test_run` and `query_replay_test_run_details` if you want to query them directly as well.
