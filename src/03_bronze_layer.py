# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer
# MAGIC Reads raw JSON files from the Volume, parses FHIR Bundle entries,
# MAGIC flattens them into typed columns, and writes to Bronze Delta tables.

# COMMAND ----------

# MAGIC %run ./01_config

# COMMAND ----------

dbutils.widgets.text("resource_type", "Patient", "FHIR Resource Type")
RESOURCE_TYPE = dbutils.widgets.get("resource_type")
print(f"Processing Bronze layer for: {RESOURCE_TYPE}")

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import *
from datetime import datetime, timezone

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read raw JSON from Volume

# COMMAND ----------

today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
volume_path = get_volume_path(RESOURCE_TYPE, today_str)
print(f"Source volume path: {volume_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parse Bundle and explode entries
# MAGIC Each JSON file is a FHIR Bundle. We extract individual resources from `entry[]`.

# COMMAND ----------

# -- common meta fields used by all resources
def _meta_fields():
    return StructType([
        StructField("versionId", StringType()),
        StructField("lastUpdated", StringType()),
        StructField("source", StringType()),
    ])

def _coding_array():
    return ArrayType(StructType([
        StructField("system", StringType()),
        StructField("code", StringType()),
        StructField("display", StringType()),
    ]))


def get_patient_schema():
    return StructType([
        StructField("id", StringType()),
        StructField("meta", _meta_fields()),
        StructField("active", BooleanType()),
        StructField("gender", StringType()),
        StructField("birthDate", StringType()),
        StructField("name", ArrayType(StructType([
            StructField("text", StringType()),
            StructField("family", StringType()),
            StructField("given", ArrayType(StringType())),
        ]))),
        StructField("identifier", ArrayType(StructType([
            StructField("system", StringType()),
            StructField("value", StringType()),
        ]))),
        StructField("address", ArrayType(StructType([
            StructField("city", StringType()),
            StructField("state", StringType()),
            StructField("country", StringType()),
            StructField("postalCode", StringType()),
        ]))),
        StructField("maritalStatus", StructType([
            StructField("coding", _coding_array()),
        ])),
    ])


def get_encounter_schema():
    return StructType([
        StructField("id", StringType()),
        StructField("meta", _meta_fields()),
        StructField("status", StringType()),
        StructField("class", StructType([
            StructField("system", StringType()),
            StructField("code", StringType()),
            StructField("display", StringType()),
        ])),
        StructField("type", ArrayType(StructType([
            StructField("coding", _coding_array()),
        ]))),
        StructField("subject", StructType([StructField("reference", StringType())])),
        StructField("period", StructType([
            StructField("start", StringType()),
            StructField("end", StringType()),
        ])),
        StructField("reasonCode", ArrayType(StructType([
            StructField("coding", _coding_array()),
        ]))),
    ])


def get_observation_schema():
    return StructType([
        StructField("id", StringType()),
        StructField("meta", _meta_fields()),
        StructField("status", StringType()),
        StructField("category", ArrayType(StructType([StructField("coding", _coding_array())]))),
        StructField("code", StructType([
            StructField("coding", _coding_array()),
            StructField("text", StringType()),
        ])),
        StructField("subject", StructType([StructField("reference", StringType())])),
        StructField("encounter", StructType([StructField("reference", StringType())])),
        StructField("effectiveDateTime", StringType()),
        StructField("valueQuantity", StructType([
            StructField("value", DoubleType()),
            StructField("unit", StringType()),
        ])),
        StructField("valueString", StringType()),
        StructField("valueCodeableConcept", StructType([
            StructField("coding", _coding_array()),
            StructField("text", StringType()),
        ])),
    ])


def get_condition_schema():
    return StructType([
        StructField("id", StringType()),
        StructField("meta", _meta_fields()),
        StructField("clinicalStatus", StructType([StructField("coding", _coding_array())])),
        StructField("verificationStatus", StructType([StructField("coding", _coding_array())])),
        StructField("category", ArrayType(StructType([StructField("coding", _coding_array())]))),
        StructField("code", StructType([
            StructField("coding", _coding_array()),
            StructField("text", StringType()),
        ])),
        StructField("subject", StructType([StructField("reference", StringType())])),
        StructField("encounter", StructType([StructField("reference", StringType())])),
        StructField("onsetDateTime", StringType()),
        StructField("abatementDateTime", StringType()),
        StructField("recordedDate", StringType()),
    ])


def get_bundle_schema(res_schema):
    return StructType([
        StructField("resourceType", StringType()),
        StructField("id", StringType()),
        StructField("entry", ArrayType(StructType([
            StructField("fullUrl", StringType()),
            StructField("resource", res_schema),
        ]))),
    ])


def flatten_patient(df):
    return df.select(
        "full_url",
        F.col("r.id").alias("resource_id"),
        F.col("r.meta.versionId").alias("meta_version_id"),
        F.col("r.meta.lastUpdated").alias("meta_last_updated"),
        F.col("r.meta.source").alias("meta_source"),
        F.col("r.active").alias("active"),
        F.col("r.gender").alias("gender"),
        F.col("r.birthDate").alias("birth_date"),
        F.col("r.name")[0]["text"].alias("name_text"),
        F.col("r.name")[0]["family"].alias("name_family"),
        F.col("r.name")[0]["given"][0].alias("name_given"),
        F.col("r.identifier")[0]["system"].alias("identifier_system"),
        F.col("r.identifier")[0]["value"].alias("identifier_value"),
        F.col("r.address")[0]["city"].alias("address_city"),
        F.col("r.address")[0]["state"].alias("address_state"),
        F.col("r.address")[0]["country"].alias("address_country"),
        F.col("r.address")[0]["postalCode"].alias("address_postal_code"),
        F.col("r.maritalStatus.coding")[0]["code"].alias("marital_status_code"),
        "raw_resource_json",
    )


def flatten_encounter(df):
    return df.select(
        "full_url",
        F.col("r.id").alias("resource_id"),
        F.col("r.meta.versionId").alias("meta_version_id"),
        F.col("r.meta.lastUpdated").alias("meta_last_updated"),
        F.col("r.meta.source").alias("meta_source"),
        F.col("r.status").alias("status"),
        F.col("r.class.code").alias("class_code"),
        F.col("r.class.display").alias("class_display"),
        F.col("r.subject.reference").alias("patient_reference"),
        F.col("r.period.start").alias("period_start"),
        F.col("r.period.end").alias("period_end"),
        F.col("r.type")[0]["coding"][0]["code"].alias("type_code"),
        F.col("r.type")[0]["coding"][0]["display"].alias("type_display"),
        F.col("r.reasonCode")[0]["coding"][0]["display"].alias("reason_display"),
        "raw_resource_json",
    )


def flatten_observation(df):
    return df.select(
        "full_url",
        F.col("r.id").alias("resource_id"),
        F.col("r.meta.versionId").alias("meta_version_id"),
        F.col("r.meta.lastUpdated").alias("meta_last_updated"),
        F.col("r.meta.source").alias("meta_source"),
        F.col("r.status").alias("status"),
        F.col("r.category")[0]["coding"][0]["code"].alias("category_code"),
        F.col("r.category")[0]["coding"][0]["display"].alias("category_display"),
        F.col("r.code.coding")[0]["system"].alias("code_system"),
        F.col("r.code.coding")[0]["code"].alias("observation_code"),
        F.col("r.code.coding")[0]["display"].alias("observation_display"),
        F.col("r.code.text").alias("observation_text"),
        F.col("r.subject.reference").alias("patient_reference"),
        F.col("r.encounter.reference").alias("encounter_reference"),
        F.col("r.effectiveDateTime").alias("effective_datetime"),
        F.col("r.valueQuantity.value").alias("value_quantity"),
        F.col("r.valueQuantity.unit").alias("value_unit"),
        F.col("r.valueString").alias("value_string"),
        F.col("r.valueCodeableConcept.text").alias("value_codeable_text"),
        "raw_resource_json",
    )


def flatten_condition(df):
    return df.select(
        "full_url",
        F.col("r.id").alias("resource_id"),
        F.col("r.meta.versionId").alias("meta_version_id"),
        F.col("r.meta.lastUpdated").alias("meta_last_updated"),
        F.col("r.meta.source").alias("meta_source"),
        F.col("r.clinicalStatus.coding")[0]["code"].alias("clinical_status"),
        F.col("r.verificationStatus.coding")[0]["code"].alias("verification_status"),
        F.col("r.verificationStatus.coding")[0]["display"].alias("verification_display"),
        F.col("r.category")[0]["coding"][0]["code"].alias("category_code"),
        F.col("r.category")[0]["coding"][0]["display"].alias("category_display"),
        F.col("r.code.coding")[0]["system"].alias("code_system"),
        F.col("r.code.coding")[0]["code"].alias("condition_code"),
        F.col("r.code.coding")[0]["display"].alias("condition_display"),
        F.col("r.code.text").alias("condition_text"),
        F.col("r.subject.reference").alias("patient_reference"),
        F.col("r.encounter.reference").alias("encounter_reference"),
        F.col("r.onsetDateTime").alias("onset_datetime"),
        F.col("r.abatementDateTime").alias("abatement_datetime"),
        F.col("r.recordedDate").alias("recorded_date"),
        "raw_resource_json",
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Apply flattening and add metadata

# COMMAND ----------

resource_schemas = {
    "Patient": get_patient_schema(),
    "Encounter": get_encounter_schema(),
    "Observation": get_observation_schema(),
    "Condition": get_condition_schema(),
}

flatten_map = {
    "Patient": flatten_patient,
    "Encounter": flatten_encounter,
    "Observation": flatten_observation,
    "Condition": flatten_condition,
}

res_schema = resource_schemas.get(RESOURCE_TYPE)
flatten_fn = flatten_map.get(RESOURCE_TYPE)
if not res_schema or not flatten_fn:
    dbutils.notebook.exit(f"Unknown resource type: {RESOURCE_TYPE}")

bundle_schema = get_bundle_schema(res_schema)

try:
    bundle_df = (
        spark.read
        .option("multiline", "true")
        .schema(bundle_schema)
        .json(f"{volume_path}/*.json")
    )
    pages_count = bundle_df.count()
    print(f"Read {pages_count} bundle page(s) from {volume_path}")
    if pages_count == 0:
        dbutils.notebook.exit(f"No raw files for {RESOURCE_TYPE} on {today_str}")
except Exception as e:
    print(f"Error reading JSON from {volume_path}: {e}")
    dbutils.notebook.exit(f"No raw files for {RESOURCE_TYPE} on {today_str}")

parsed_df = (
    bundle_df
    .filter(F.col("entry").isNotNull())
    .select(F.explode("entry").alias("entry"))
    .select(
        F.col("entry.fullUrl").alias("full_url"),
        F.col("entry.resource").alias("r"),
        F.to_json(F.col("entry.resource")).alias("raw_resource_json"),
    )
)

print(f"Total entries: {parsed_df.count()}")

flattened_df = flatten_fn(parsed_df)

# add metadata columns
extraction_ts = datetime.now(timezone.utc)
bronze_df = (
    flattened_df
    .withColumn("extraction_timestamp", F.lit(extraction_ts).cast("timestamp"))
    .withColumn("api_url_or_params", F.lit(f"{FHIR_BASE_URL}/{RESOURCE_TYPE}?_count={PAGE_SIZE}"))
    .withColumn("ingestion_date", F.lit(today_str).cast("date"))
    .withColumn("resource_type", F.lit(RESOURCE_TYPE))
)

print(f"Bronze rows: {bronze_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Bronze Delta table

# COMMAND ----------

bronze_table = get_bronze_table(RESOURCE_TYPE)
bronze_df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(bronze_table)

total = spark.table(bronze_table).count()
print(f"Written to {bronze_table} (total rows now: {total})")
