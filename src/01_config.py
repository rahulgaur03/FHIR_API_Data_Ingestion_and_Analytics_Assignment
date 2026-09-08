# Databricks notebook source
# MAGIC %md
# MAGIC # Configuration
# MAGIC Shared config notebook - all other notebooks run this first via `%run ./01_config`.
# MAGIC Settings are loaded from `config/resource_registry.yml`.

# COMMAND ----------

dbutils.widgets.text("catalog", "fhir_api_data_ingestion_and_analytics_assignment", "Catalog")
dbutils.widgets.text("schema", "dev", "Schema")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load config from YAML

# COMMAND ----------

import yaml, os
from datetime import datetime, timedelta, timezone

# In Databricks bundles, notebooks are deployed under .bundle/.../files/src/
# The config file is at .bundle/.../files/config/resource_registry.yml
# We need to find it relative to the notebook's workspace location.

def _find_config():
    """Try multiple paths to locate resource_registry.yml in the workspace."""
    # When running in Databricks, try to get the notebook path
    try:
        nb_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
        # nb_path is like /Workspace/Users/.../files/src/01_config
        # config is at /Workspace/Users/.../files/config/resource_registry.yml
        workspace_dir = "/Workspace" + str(os.path.dirname(os.path.dirname(nb_path)))
        candidates = [
            os.path.join(workspace_dir, "config", "resource_registry.yml"),
        ]
    except Exception:
        candidates = []

    # Also try relative paths (for local testing)
    if "__file__" in dir():
        script_dir = os.path.dirname(os.path.abspath(__file__))
        candidates.append(os.path.join(script_dir, "..", "config", "resource_registry.yml"))

    # Fallback paths
    candidates.extend([
        "../config/resource_registry.yml",
        "config/resource_registry.yml",
    ])

    for p in candidates:
        resolved = os.path.normpath(p)
        if os.path.exists(resolved):
            return resolved

    raise FileNotFoundError(
        f"Could not find config/resource_registry.yml. Tried: {[os.path.normpath(c) for c in candidates]}"
    )

_config_path = _find_config()
with open(_config_path) as f:
    _cfg = yaml.safe_load(f)
print(f"Loaded config from: {_config_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Global settings

# COMMAND ----------

FHIR_BASE_URL    = _cfg["settings"]["fhir_base_url"]
PAGE_SIZE        = _cfg["settings"]["page_size"]
MAX_PAGES        = _cfg["settings"]["max_pages"]
INCREMENTAL_DAYS = _cfg["settings"]["incremental_days"]

RAW_SCHEMA    = _cfg["schemas"]["raw"]
BRONZE_SCHEMA = _cfg["schemas"]["bronze"]
SILVER_SCHEMA = _cfg["schemas"]["silver"]
GOLD_SCHEMA   = _cfg["schemas"]["gold"]

RESOURCE_REGISTRY = _cfg["resources"]
RESOURCES = list(RESOURCE_REGISTRY.keys())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper functions

# COMMAND ----------

def get_resource_config(resource_type):
    """Look up a resource from the registry and return enriched config dict."""
    if resource_type not in RESOURCE_REGISTRY:
        raise ValueError(f"Unknown resource: {resource_type}. Available: {RESOURCES}")

    cfg = RESOURCE_REGISTRY[resource_type].copy()
    cfg["resource_type"]    = resource_type
    cfg["api_url"]          = f"{FHIR_BASE_URL}{cfg['api_path']}"
    cfg["bronze_table_fqn"] = f"{CATALOG}.{BRONZE_SCHEMA}.{cfg['bronze_table']}"
    cfg["silver_table_fqn"] = f"{CATALOG}.{SILVER_SCHEMA}.{cfg['silver_table']}"
    cfg["volume_path"]      = get_volume_path(resource_type)
    return cfg


def get_volume_path(resource_type, date_str=None):
    """Build the volume path for raw JSON storage."""
    if date_str is None:
        date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    return f"/Volumes/{CATALOG}/{RAW_SCHEMA}/raw_files/{resource_type}/{date_str}"


def get_bronze_table(resource_type):
    return get_resource_config(resource_type)["bronze_table_fqn"]


def get_silver_table(resource_type):
    return get_resource_config(resource_type)["silver_table_fqn"]


def get_ingestion_log_table():
    return f"{CATALOG}.{BRONZE_SCHEMA}.ingestion_log"


def get_ingestion_log_schema():
    from pyspark.sql.types import (
        StructType, StructField, StringType, IntegerType, TimestampType, DateType
    )
    return StructType([
        StructField("log_id", StringType(), True),
        StructField("resource_type", StringType(), True),
        StructField("api_url", StringType(), True),
        StructField("page_number", IntegerType(), True),
        StructField("records_fetched", IntegerType(), True),
        StructField("extraction_timestamp", TimestampType(), True),
        StructField("save_timestamp", TimestampType(), True),
        StructField("status", StringType(), True),
        StructField("error_message", StringType(), True),
        StructField("ingestion_date", DateType(), True),
    ])


def get_incremental_date_range():
    """Returns (start_date, end_date) strings for the incremental window."""
    now = datetime.now(timezone.utc)
    start = now - timedelta(days=INCREMENTAL_DAYS)
    return start.strftime("%Y-%m-%d"), now.strftime("%Y-%m-%d")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Initialize catalog, schemas, volume, and log table

# COMMAND ----------

def initialize_lakehouse():
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
    spark.sql(f"USE CATALOG {CATALOG}")

    for s in [RAW_SCHEMA, BRONZE_SCHEMA, SILVER_SCHEMA, GOLD_SCHEMA]:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{s}")

    spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{RAW_SCHEMA}.raw_files")

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {get_ingestion_log_table()} (
            log_id STRING,
            resource_type STRING,
            api_url STRING,
            page_number INT,
            records_fetched INT,
            extraction_timestamp TIMESTAMP,
            save_timestamp TIMESTAMP,
            status STRING,
            error_message STRING,
            ingestion_date DATE
        ) USING DELTA
    """)

    print(f"Lakehouse ready: {CATALOG} [{RAW_SCHEMA}, {BRONZE_SCHEMA}, {SILVER_SCHEMA}, {GOLD_SCHEMA}]")
    print(f"Resources: {RESOURCES}")

initialize_lakehouse()
