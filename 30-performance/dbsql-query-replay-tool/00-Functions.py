# Databricks notebook source
import uuid
import requests, json
import time
from datetime import datetime
from queue import Queue
from threading import Thread
from dbruntime.databricks_repl_context import get_context
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import col, min
from pyspark.sql.window import Window


class QueryReplayTest:
    def __init__(
        self,
        test_name,
        result_catalog,
        result_schema,
        token,
        source_warehouse_id,
        source_start_time,
        source_end_time,
        target_warehouse_size="Small",
        target_warehouse_max_num_clusters=1,
        target_warehouse_type="PRO",
        target_warehouse_serverless=True,
        target_warehouse_custom_tags=[],
        target_warehouse_channel="CHANNEL_NAME_PREVIEW",
        target_warehouse_auto_stop_mins=3,
        sender_parallelism=200,
        checker_parallelism=10,
        **kwargs,
    ):
        self.token = token
        self.result_catalog = result_catalog
        self.result_schema = result_schema

        self._test_id = kwargs.get("test_id", None)

        if "test_id" in kwargs:
            run = self.show_run.toPandas().to_dict(orient="records")[0]
            self.test_name = run["test_name"]
            self.source_warehouse_id = run["source_warehouse_id"]
            self.source_start_time = run["source_start_time"]
            self.source_end_time = run["source_end_time"]

            self._target_warehouse_name = run["test_name"] + "_" + run["test_id"]
            self._target_warehouse_id = run["target_warehouse_id"]

            self._run_completed = True
        else:
            self.test_name = test_name
            self.source_warehouse_id = source_warehouse_id
            self.source_start_time = source_start_time
            self.source_end_time = source_end_time

            self._target_warehouse_name = None
            self._target_warehouse_id = None

            self._run_completed = False

        self.target_warehouse_size = target_warehouse_size
        self.target_warehouse_max_num_clusters = target_warehouse_max_num_clusters
        self.target_warehouse_type = target_warehouse_type
        self.target_warehouse_serverless = target_warehouse_serverless
        self.target_warehouse_custom_tags = target_warehouse_custom_tags
        self.target_warehouse_channel = target_warehouse_channel
        self.target_warehouse_auto_stop_mins = target_warehouse_auto_stop_mins

        self.sender_parallelism = sender_parallelism
        self.checker_parallelism = checker_parallelism

        self._query_df = None

    @property
    def test_id(self):
        if self._test_id is None:
            self._test_id = str(uuid.uuid4())
        return self._test_id

    @property
    def target_warehouse_name(self):
        if self._target_warehouse_name is None:
            self._target_warehouse_name = self.test_name + "_" + self.test_id
        return self._target_warehouse_name

    @property
    def host(self):
        return get_context().workspaceUrl

    @property
    def query_df(self):
        if self._query_df is None:
            self._query_df = self.get_query()
        return self._query_df

    def create_warehouse(self):
        api = f"https://{self.host}/api/2.0/sql/warehouses"
        payload = {
            "name": self.target_warehouse_name,
            "cluster_size": self.target_warehouse_size,
            "max_num_clusters": self.target_warehouse_max_num_clusters,
            "auto_stop_mins": self.target_warehouse_auto_stop_mins,
            "enable_photon": True,
            "enable_serverless_compute": self.target_warehouse_serverless,
            "warehouse_type": self.target_warehouse_type,
            "tags": {"custom_tags": self.target_warehouse_custom_tags},
            "channel": {"name": self.target_warehouse_channel},
        }
        headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
        }

        try:
            response = requests.post(api, headers=headers, data=json.dumps(payload))
            response = json.loads(response.text)["id"]
        except requests.exceptions.RequestException as e:
            print(f"An error occurred: {e}")

        return response

    def start_warehouse(self):
        api = f"https://{self.host}/api/2.0/sql/warehouses/{self.target_warehouse_id}/start"
        headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
        }

        try:
            response = requests.post(api, headers=headers)
            response = json.loads(response.text)["id"]
        except requests.exceptions.RequestException as e:
            print(f"An error occurred: {e}")

    @property
    def target_warehouse_id(self):
        if self._target_warehouse_id is None:
            self._target_warehouse_id = self.create_warehouse()
        return self._target_warehouse_id

    def get_query(self):
        df = spark.sql(
            f"""
            SELECT unix_timestamp(start_time) as start_time, statement_id, statement_text
            FROM system.query.history
            WHERE statement_type IN ('SELECT') --WHAT OTHER TYPE SHOULD WE CARE ABOUT?
              AND warehouse_id = "{self.source_warehouse_id}"
              AND error_message is null
              AND start_time BETWEEN "{self.source_start_time}" AND "{self.source_end_time}"
            """
        )
        return df

    def send_query(self, statement):
        api = f"https://{self.host}/api/2.0/sql/statements/"
        payload = {
            "warehouse_id": self.target_warehouse_id,
            "statement": statement,
            "wait_timeout": "0s",
            "disposition": "EXTERNAL_LINKS",
        }
        headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
        }

        try:
            response = requests.post(api, headers=headers, data=json.dumps(payload))
            response = json.loads(response.text)["statement_id"]
        except requests.exceptions.RequestException as e:
            print(f"An error occurred: {e}")

        return response

    def wait_and_send_query(self, wait_time, statement):
        if wait_time > 0:
            time.sleep(wait_time)
        else:
            print(f"lagging behind by {wait_time} seconds")
        res = self.send_query(statement)
        return res

    def check_status(self, statement_id):
        api = f"https://{self.host}/2.0/sql/statements/{statement_id}"
        headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
        }

        try:
            response = requests.get(api, headers=headers)
            response = response.json()["status"]["state"]
        except requests.exceptions.RequestException as e:
            print(f"An error occurred: {e}")

        return response

    def replay_queries(self, queries):
        def query_sender(q1, q2):
            while True:
                start_time, offset, qid, q = q1.get()
                tid = self.wait_and_send_query(
                    offset - (datetime.now() - start_time).seconds, q
                )
                print(f"{datetime.now()} - sending query - {tid}")
                q2.put((qid, tid))
                q1.task_done()

        def query_checker(q2, q3):
            while True:
                qid, tid = q2.get()
                status = self.check_status(tid)
                print(f"{datetime.now()} - fetching result - {tid} - {status}")
                if status in ["SUCCEEDED", "FAILED", "CANCELED", "CLOSED"]:
                    q2.put((qid, tid))
                else:
                    q3.put((qid, tid))
                q2.task_done()

        input_q = Queue(maxsize=0)
        submitted_q = Queue(maxsize=0)
        completed_q = Queue(maxsize=0)

        for i in range(self.sender_parallelism):
            worker = Thread(target=query_sender, args=(input_q, submitted_q))
            worker.daemon = True
            worker.start()

        for i in range(self.checker_parallelism):
            worker = Thread(target=query_checker, args=(submitted_q, completed_q))
            worker.daemon = True
            worker.start()

        queries.sort(key=lambda x: x[0])
        first_query_start_time = queries[0][0]
        normalized_queries = [
            (datetime.now(), t - first_query_start_time, i, q) for t, i, q in queries
        ]

        for q in normalized_queries:
            input_q.put(q)

        while input_q.qsize() > 0 or submitted_q.qsize() > 0:
            print(
                f"In Progress - {input_q.qsize()} queries to be sent - {submitted_q.qsize()} queries to be fetched - {completed_q.qsize()} / {len(queries)} queries completed"
            )
            time.sleep(10)

        return list(completed_q.queue)

    def init_schema(self, overwrite=False):
        try:
            spark.sql(f"USE CATALOG {self.result_catalog}")
            if overwrite:
                spark.sql(
                    f"DROP TABLE IF EXISTS {self.result_schema}.query_replay_test_run"
                )
                spark.sql(
                    f"DROP TABLE IF EXISTS {self.result_schema}.query_replay_test_run_details"
                )

            spark.sql(f"CREATE SCHEMA IF NOT EXISTS {self.result_schema}")
            spark.sql(f"USE SCHEMA {self.result_schema}")

            spark.sql(
                """
                CREATE TABLE IF NOT EXISTS query_replay_test_run (
                    test_name STRING,
                    test_start_time TIMESTAMP,
                    test_id STRING,
                    source_warehouse_id STRING,
                    source_start_time TIMESTAMP,
                    source_end_time TIMESTAMP,
                    target_warehouse_id STRING,
                    query_count STRING)
                """
            )

            spark.sql(
                """
                CREATE TABLE IF NOT EXISTS query_replay_test_run_details (
                test_id STRING,
                source_warehouse_id STRING,
                source_statement_id STRING,
                target_warehouse_id STRING,
                target_statement_id STRING)
                """
            )

        except requests.exceptions.RequestException as e:
            print(f"An error occurred: {e}")

        return spark.sql(f"SHOW TABLES IN {self.result_catalog}.{self.result_schema}")

    def log_run(self, query_df):
        insert = spark.sql(
            f"""
        INSERT INTO query_replay_test_run VALUES(
        '{self.test_name}',
        '{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}',
        '{self.test_id}',
        '{self.source_warehouse_id}',
        '{self.source_start_time}',
        '{self.source_end_time}',
        '{self.target_warehouse_id}',
        {query_df.count()}
        );
        """
        )
        return insert

    def log_run_details(self, results):
        result_df = spark.createDataFrame(
            list(
                (
                    self.test_id,
                    self.source_warehouse_id,
                    r[0],
                    self.target_warehouse_id,
                    r[1],
                )
                for r in results
            ),
            StructType(
                [
                    StructField("test_id", StringType(), True),
                    StructField("source_warehouse_id", StringType(), True),
                    StructField("source_statement_id", StringType(), True),
                    StructField("target_warehouse_id", StringType(), True),
                    StructField("target_statement_id", StringType(), True),
                ]
            ),
        )

        insert = result_df.write.insertInto(
            f"{self.result_catalog}.{self.result_schema}.query_replay_test_run_details"
        )
        return insert

    @property
    def show_run(self):
        return spark.sql(
            f"select * from {self.result_catalog}.{self.result_schema}.query_replay_test_run where test_id = '{self.test_id}'"
        )

    @property
    def show_run_details(self):
        return spark.sql(
            f"select * from {self.result_catalog}.{self.result_schema}.query_replay_test_run_details where test_id = '{self.test_id}'"
        )

    def run(self, overwrite_schema=False):
        if self._run_completed:
            print(f"run already completed - test id: {self.test_id}")
        else:
            print(f"starting run - test id: {self.test_id}")
            self.init_schema(overwrite_schema)
            self.log_run(self.query_df)
            queries = self.query_df.collect()
            results = self.replay_queries(queries)
            self.log_run_details(results)
            self._run_completed = True
            print(f"run completed - test id: {self.test_id}")

        return self.test_id

    @property
    def query_results(self):
        run_details = self.show_run_details
        query_history = spark.read.table("system.query.history")

        comparison = (
            run_details.alias("r")
            .join(
                query_history.alias("s"),
                (col("r.source_warehouse_id") == col("s.warehouse_id"))
                & (col("r.source_statement_id") == col("s.statement_id")),
                "leftouter",
            )
            .join(
                query_history.alias("t"),
                (col("r.target_warehouse_id") == col("t.warehouse_id"))
                & (col("r.target_statement_id") == col("t.statement_id")),
                "leftouter",
            )
            .select(
                col("r.*"),
                col("s.start_time").alias("source_start_time"),
                col("s.total_duration_ms").alias("source_execution_time"),
                (
                    col("s.start_time")
                    - min(col("s.start_time")).over(
                        Window.partitionBy("r.source_warehouse_id")
                    )
                ).alias("source_offset"),
                col("t.start_time").alias("target_start_time"),
                col("t.total_duration_ms").alias("target_execution_time"),
                (
                    col("t.start_time")
                    - min(col("t.start_time")).over(
                        Window.partitionBy("r.target_warehouse_id")
                    )
                ).alias("target_offset"),
            )
        ).withColumns(
            {
                "offset_diff": (col("source_offset") - col("target_offset")).cast(
                    "long"
                ),
                "execution_diff": col("source_execution_time")
                - col("target_execution_time"),
            }
        )

        return comparison
