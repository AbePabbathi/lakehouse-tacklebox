import os
import warnings
from typing import Dict

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs


class DatabricksClient:
    def __init__(self, constants: Dict):
        # Instantiate the python SDK client
        self.w = WorkspaceClient()

        # Expose constants that are referenced by methods
        self.constants = constants

        # Define hard-coded properties that are referenced by methods
        self._latest_spark_version: str = None
        self._cloud_specific_cluster_type: str = None
        self._number_of_cores_per_worker: int = None
        self._warehouse_name: str = None

    ################# Class Properties for Cluster Configs ############
    @property
    def latest_spark_version(self) -> str:
        return self.w.clusters.select_spark_version(latest=True, long_term_support=True)

    @property
    def cloud_specific_cluster_type(self) -> str:
        if self.w.config.is_azure:
            return "Standard_DS3_v2"
        elif self.w.config.is_gcp:
            return "n1-highmem-4"
        elif self.w.config.is_aws:
            return "i3.xlarge"
        else:
            raise ValueError(
                f"w.config did not return one of the three supported clouds"
            )

    @property
    def number_of_cores_per_worker(self) -> int:
        return 4

    @property
    def warehouse_name(self):
        return f"[AUTOMATED] {self.constants.current_user_name}'s TPCDS Benchmarking Warehouse - {self.constants.warehouse_size}, {self.constants.max_num_warehouse_clusters} Max Clusters"

    ########### Cluster Configurations ###########
    def _get_data_generator_cluster_config(self) -> Dict:
        return {
            "name": "TPCDS Benchmarking Cluster",
            # Specify autoscaling that can handle a variety of data sizes
            "autoscale": {"min_workers": 1, "max_workers": 10},
            # Install the Databricks TPC-DS Benchmark Kit
            "init_scripts": [
                {"dbfs": {"destination": self.constants.init_script_path}}
            ],
            # Specify singe_user because scala requires a single user mode
            "single_user_name": f"{self.constants.current_user_email}",
            "data_security_mode": "SINGLE_USER",
            "node_type_id": self.cloud_specific_cluster_type,
            "spark_version": "12.2.x-scala2.12",
        }

    def _get_load_testing_cluster_config(self) -> Dict:
        concurrency = self.constants.concurrency
        n_workers = int(concurrency / self.number_of_cores_per_worker) + 1
        if n_workers > 25:
            warnings.warn(
                f"""
                You are requesting a concurrency of {concurrency}, however the default CPU count
                for this tool only has 4 cores per worker. To prevent spinning up very expensive clusters, 
                we have capped the number of workers at 25. TPC-DS only has < 100 queries so this will
                not limit performance.
            """
            )

            n_workers = 25

        return {
            "name": "TPCDS Load Testing Cluster",
            "num_workers": n_workers,
            "spark_version": self.latest_spark_version,
            "node_type_id": self.cloud_specific_cluster_type,
            "single_user_name": f"{self.constants.current_user_email}",
            "data_security_mode": "SINGLE_USER",
        }

    ################# Create Data Functions ###############
    def create_job(self):
        step_1 = {
            "task_key": "create_data_and_queries",
            "notebook_task": {
                "notebook_path": self.constants.create_data_and_queries_notebook_path,
                "source": "WORKSPACE",
                "base_parameters": {
                    "current_user_name": self.constants.current_user_name,
                    "scale_factor": self.constants.scale_factor,
                    "query_directory": self.constants.query_path,
                    "data_directory": self.constants.data_path,
                    "catalog_name": self.constants.catalog_name,
                    "schema_name": self.constants.schema_name,
                },
            },
            "new_cluster": self._get_data_generator_cluster_config(),
        }

        step_2 = {
            "task_key": "TPCDS_benchmarking",
            "depends_on": [{"task_key": "create_data_and_queries"}],
            "notebook_task": {
                "notebook_path": f"/Repos/{self.constants.current_user_email}/TPC-DS Runner/notebooks/run_tpcds_benchmarking",
                "source": "WORKSPACE",
                "base_parameters": {
                    "warehouse_id": self.constants.warehouse_id,
                    "catalog_name": self.constants.catalog_name,
                    "schema_name": self.constants.schema_name,
                    "query_path": self.constants.query_path,
                    "concurrency": self.constants.concurrency,
                    "query_repetition_count": self.constants.query_repetition_count,
                },
            },
            "new_cluster": self._get_load_testing_cluster_config(),
            "libraries": [{"whl": self.constants.beaker_whl_path}],
        }

        return self.w.jobs.create(
            name=self.constants.job_name,
            tasks=[
                jobs.Task.from_dict(step_1),
                jobs.Task.from_dict(step_2),
            ],
        )

    def run_job(self, job_id: str):
        return self.w.jobs.run_now(**{"job_id": job_id})

    ############## Warehouse Creation Functions #################
    def get_warehouse_id_for_name(self, name: str) -> str:
        # TODO handle continuation token
        #  I don't think it's available in the API
        for w in self.w.warehouses.list():
            if w.name == name:
                return w.id

        return None

    def create_warehouse(self):
        warehouse_id = self.get_warehouse_id_for_name(self.warehouse_name)
        if warehouse_id:
            print(
                f"Using existing warehouse:\n\tWarehouse ID: {warehouse_id}\n\tWarehouse Name: {self.warehouse_name}"
            )
            return self.w.warehouses.get(warehouse_id)
        else:
            return self.w.warehouses.create(
                **{
                    "name": self.warehouse_name,
                    "cluster_size": self.constants.warehouse_size,
                    "min_num_clusters": "1",
                    "max_num_clusters": self.constants.max_num_warehouse_clusters,
                    "auto_stop_mins": "720",
                }
            )
