# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer
# MAGIC Reads Bronze tables, cleans and deduplicates, then applies SCD Type 2
# MAGIC to track historical changes. Each record gets:
# MAGIC - `is_current` - whether this is the latest version
# MAGIC - `valid_from` / `valid_to` - when this version was active
# MAGIC - `record_hash` - MD5 hash of business columns for change detection

# COMMAND ----------

# MAGIC %run ./01_config

# COMMAND ----------

dbutils.widgets.text("resource_type", "Patient", "FHIR Resource Type")
RESOURCE_TYPE = dbutils.widgets.get("resource_type")
print(f"Processing Silver layer for: {RESOURCE_TYPE}")

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime, timezone
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load config and read bronze data

# COMMAND ----------

# get the SCD2 hash columns from the YAML config
resource_cfg = get_resource_config(RESOURCE_TYPE)
hash_cols = resource_cfg["scd2_hash_columns"]

bronze_table = get_bronze_table(RESOURCE_TYPE)
silver_table = get_silver_table(RESOURCE_TYPE)

print(f"Bronze: {bronze_table}")
print(f"Silver: {silver_table}")
print(f"Hash columns: {len(hash_cols)}")

bronze_df = spark.table(bronze_table)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clean and deduplicate
# MAGIC Remove nulls, dedup by (resource_id, meta_version_id), and trim strings.

# COMMAND ----------

# keep only the earliest extraction per (resource_id, version)
w = Window.partitionBy("resource_id", "meta_version_id").orderBy("extraction_timestamp")

cleaned_df = (
    bronze_df
    .filter(F.col("resource_id").isNotNull())
    .withColumn("_rn", F.row_number().over(w))
    .filter(F.col("_rn") == 1)
    .drop("_rn")
)

# trim all string columns
for col_name, col_type in cleaned_df.dtypes:
    if col_type == "string":
        cleaned_df = cleaned_df.withColumn(col_name, F.trim(F.col(col_name)))

# compute hash for change detection
cleaned_df = cleaned_df.withColumn(
    "record_hash",
    F.md5(F.concat_ws("||", *[
        F.coalesce(F.col(c).cast("string"), F.lit("__NULL__")) for c in hash_cols
    ]))
)

print(f"Cleaned rows: {cleaned_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## SCD Type 2 merge
# MAGIC - First run: just write everything as current
# MAGIC - Subsequent runs: compare hashes, expire old records, insert new versions

# COMMAND ----------

now_ts = datetime.now(timezone.utc)

# prepare incoming data with SCD2 columns
staged_df = (
    cleaned_df
    .withColumn("is_current", F.lit(True))
    .withColumn("valid_from", F.lit(now_ts).cast("timestamp"))
    .withColumn("valid_to", F.lit(None).cast("timestamp"))
    .withColumn("scd_updated_at", F.lit(now_ts).cast("timestamp"))
    .drop("raw_resource_json")  # don't carry raw JSON to silver
)

# COMMAND ----------

silver_exists = spark.catalog.tableExists(silver_table)

if not silver_exists:
    # first load - just create the table
    print("Initial load - creating Silver table")
    staged_df.write.format("delta").mode("overwrite").saveAsTable(silver_table)
    print(f"Created {silver_table} with {staged_df.count()} rows")

else:
    # incremental load with SCD2
    print("Running SCD Type 2 merge...")

    # get current records from silver
    existing = (
        spark.table(silver_table)
        .filter("is_current = true")
        .select("resource_id", F.col("record_hash").alias("old_hash"))
    )

    # find new + changed records
    incoming = staged_df.select("resource_id", "record_hash").distinct()
    changed_ids = (
        incoming.alias("new")
        .join(existing.alias("old"), "resource_id", "left")
        .filter(
            F.col("old.old_hash").isNull() |
            (F.col("new.record_hash") != F.col("old.old_hash"))
        )
        .select("new.resource_id")
    )

    to_upsert = staged_df.join(changed_ids, "resource_id", "inner")
    change_count = to_upsert.count()
    print(f"Changed/new records: {change_count}")

    if change_count > 0:
        # expire old current records
        changed_ids.createOrReplaceTempView("_changed_ids")
        spark.sql(f"""
            MERGE INTO {silver_table} AS t
            USING _changed_ids AS s
            ON t.resource_id = s.resource_id AND t.is_current = true
            WHEN MATCHED THEN UPDATE SET
                t.is_current = false,
                t.valid_to = '{now_ts.isoformat()}',
                t.scd_updated_at = '{now_ts.isoformat()}'
        """)
        print("Expired old records")

        # insert new versions
        to_upsert.write.format("delta").mode("append").saveAsTable(silver_table)
        print(f"Inserted {change_count} new version(s)")
    else:
        print("No changes detected")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

total = spark.table(silver_table).count()
current = spark.table(silver_table).filter("is_current = true").count()
print(f"--- Silver Summary: {RESOURCE_TYPE} ---")
print(f"Total rows: {total}, Current: {current}, Historical: {total - current}")
