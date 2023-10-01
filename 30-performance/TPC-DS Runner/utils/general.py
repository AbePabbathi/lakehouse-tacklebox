import requests
from typing import List
from databricks.sdk import WorkspaceClient

_TPCDS_TABLE_NAMES = {
    "call_center",
    "catalog_page",
    "catalog_returns",
    "catalog_sales",
    "customer",
    "customer_address",
    "customer_demographics",
    "date_dim",
    "household_demographics",
    "income_band",
    "inventory",
    "item",
    "promotion",
    "reason",
    "ship_mode",
    "store",
    "store_returns",
    "store_sales",
    "time_dim",
    "warehouse",
    "web_page",
    "web_returns",
    "web_sales",
    "web_site",
}

############### Notebook Helpers #############
def _convert_to_int_safe(s: str):
    try: 
        return int(s)
    except ValueError as e:
        if 'invalid literal for int()' in str(e):
            return s
        else: 
          raise
    except:
        raise

def get_widget_values(dbutils):
    widgets_dict = {
        k.lower().replace(" ", "_"): v
        for k, v in dbutils.notebook.entry_point.getCurrentBindings().items()
    }
    
    return {k: _convert_to_int_safe(v) for k,v in widgets_dict.items()}

############### Utils #############
def can_default_authenticate_sdk():
    try:
        _ = WorkspaceClient()
    except Exception as e:
        if "cannot configure default credentials" in str(e):
            raise Exception(
                """\nWe are using the Databricks Python SDK with default authentification. It requires that you run this notebook from a single-user cluster. Please modify the existing cluster or create a new cluster with an `Access Mode` of `Assigned`. Vist the below link for more:\n\thttps://docs.databricks.com/en/clusters/configure.html#what-is-cluster-access-mode\n"""
            )
        else:
            raise


def clean_path_for_native_python(path: str) -> str:
    return "/dbfs/" + path.lstrip("/").replace("dbfs", "").lstrip(":").lstrip("/")


def make_dir_for_file_path(dbutils, path: str):
    path_dir = "/".join(path.split("/")[:-1])
    dbutils.fs.mkdirs(path_dir)


def directory_not_empty(dbutils, path: str) -> bool:
    return len(dbutils.fs.ls(path)) > 0


def add_remote_file_to_dbfs(dbutils, file_url: str, dbfs_path: str) -> bool:
    # Read file from remote
    response = requests.get(file_url, stream=True)

    # Write file
    make_dir_for_file_path(dbutils, dbfs_path)
    with open(clean_path_for_native_python(dbfs_path), "wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            if chunk:
                f.write(chunk)

    # Ensure write
    return directory_not_empty(dbutils, dbfs_path)


def tables_already_exist(spark, catalog: str, schema: str) -> bool:
    if (
        spark.sql("show catalogs").where(f"catalog ILIKE '{catalog}'").limit(1).count()
        > 0
    ):
        if (
            spark.sql(f"show databases in {catalog}")
            .where(f"databaseName ILIKE '{schema}'")
            .limit(1)
            .count()
            > 0
        ):
            tables = set(
                spark.sql(f"show tables in {catalog}.{schema}")
                .where("tableName not ILIKE 'benchmark%'")
                .select("tableName")
                .toPandas()["tableName"]
            )
            return all(x in tables for x in _TPCDS_TABLE_NAMES)

    return False


################## DBFS Writes ####################
def _add_benchmark_kit_jar_to_dbfs(dbutils, dbfs_path: str):
    return add_remote_file_to_dbfs(
        dbutils=dbutils,
        file_url="https://github.com/BlueGranite/tpc-ds-dataset-generator/blob/master/lib/spark-sql-perf_2.12-0.5.1-SNAPSHOT.jar?raw=true",
        dbfs_path=dbfs_path,
    )


def _add_init_script_to_dbfs(dbutils, init_script_path: str, jar_path: str) -> bool:
    """
    Create the BASH init script that will install the Databricks TPC-DS benchmark kit and prequisites.
    Note that this also installs the spark-sql-perf library jar.
    """
    make_dir_for_file_path(dbutils, init_script_path)

    dbutils.fs.put(
        init_script_path,
        f"""
      #!/bin/bash
      sudo apt-get --assume-yes install gcc make flex bison byacc git

      cd /usr/local/bin
      git clone https://github.com/databricks/tpcds-kit.git
      cd tpcds-kit/tools
      make OS=LINUX

      cp {jar_path.replace('dbfs:','/dbfs')} /databricks/jars/
    """,
        True,
    )

    return directory_not_empty(dbutils, init_script_path)


def _add_beaker_whl_to_dbfs(dbutils, dbfs_path) -> bool:
    return add_remote_file_to_dbfs(
        dbutils=dbutils,
        file_url="https://github.com/goodwillpunning/beaker/raw/main/dist/beaker-0.0.3-py3-none-any.whl",
        dbfs_path=dbfs_path,
    )


############# Main #################
def setup_files(dbutils, jar_path: str, init_script_path: str, beaker_whl_path: str):
    jar_created = _add_benchmark_kit_jar_to_dbfs(dbutils, jar_path)
    assert (
        jar_created
    ), f"The jar path '{jar_path}' is empty. There was an error uploading it."

    init_script_created = _add_init_script_to_dbfs(dbutils, init_script_path, jar_path)
    assert (
        init_script_path
    ), f"The init script path '{init_script}' is empty. There was an error uploading it."

    beaker_whl_created = _add_beaker_whl_to_dbfs(dbutils, beaker_whl_path)
    assert (
        beaker_whl_created
    ), f"The init script path '{beaker_whl_path}' is empty. There was an error uploading it."
    
