# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer
# MAGIC Creates analytical views on top of Silver tables for reporting:
# MAGIC - dim_patient, dim_condition (dimensions)
# MAGIC - fact_encounter, fact_observation (facts)
# MAGIC - patient_summary (aggregated KPIs)

# COMMAND ----------

# MAGIC %run ./01_config

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{GOLD_SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## dim_patient - Patient demographics (current only)

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {CATALOG}.{GOLD_SCHEMA}.dim_patient AS
SELECT
    resource_id AS patient_id,
    name_given AS first_name,
    name_family AS last_name,
    name_text AS full_name,
    gender,
    birth_date,
    FLOOR(DATEDIFF(CURRENT_DATE(), TO_DATE(birth_date)) / 365.25) AS age,
    active AS is_active,
    identifier_system,
    identifier_value,
    address_city,
    address_state,
    address_country,
    address_postal_code,
    marital_status_code,
    meta_version_id,
    meta_last_updated,
    valid_from,
    valid_to,
    is_current
FROM {CATALOG}.{SILVER_SCHEMA}.silver_patient
WHERE is_current = true
""")
print("Created: dim_patient")

# COMMAND ----------

# MAGIC %md
# MAGIC ## dim_condition - Distinct condition codes

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {CATALOG}.{GOLD_SCHEMA}.dim_condition AS
SELECT DISTINCT
    condition_code,
    code_system,
    condition_display,
    condition_text,
    category_code AS condition_category,
    category_display AS condition_category_display
FROM {CATALOG}.{SILVER_SCHEMA}.silver_condition
WHERE is_current = true AND condition_code IS NOT NULL
""")
print("Created: dim_condition")

# COMMAND ----------

# MAGIC %md
# MAGIC ## fact_encounter - Encounters joined with patient info

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {CATALOG}.{GOLD_SCHEMA}.fact_encounter AS
SELECT
    e.resource_id AS encounter_id,
    REGEXP_EXTRACT(e.patient_reference, 'Patient/(.+)', 1) AS patient_id,
    e.status AS encounter_status,
    e.class_code AS encounter_class,
    e.class_display AS encounter_class_display,
    e.type_code AS encounter_type_code,
    e.type_display AS encounter_type_display,
    e.reason_display AS encounter_reason,
    e.period_start,
    e.period_end,
    CASE
        WHEN e.period_start IS NOT NULL AND e.period_end IS NOT NULL
        THEN ROUND((UNIX_TIMESTAMP(TO_TIMESTAMP(e.period_end)) -
                     UNIX_TIMESTAMP(TO_TIMESTAMP(e.period_start))) / 3600.0, 2)
    END AS duration_hours,
    e.meta_last_updated,
    e.ingestion_date,
    p.full_name AS patient_name,
    p.gender AS patient_gender,
    p.age AS patient_age
FROM {CATALOG}.{SILVER_SCHEMA}.silver_encounter e
LEFT JOIN {CATALOG}.{GOLD_SCHEMA}.dim_patient p
    ON REGEXP_EXTRACT(e.patient_reference, 'Patient/(.+)', 1) = p.patient_id
WHERE e.is_current = true
""")
print("Created: fact_encounter")

# COMMAND ----------

# MAGIC %md
# MAGIC ## fact_observation - Observations with patient context

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {CATALOG}.{GOLD_SCHEMA}.fact_observation AS
SELECT
    o.resource_id AS observation_id,
    REGEXP_EXTRACT(o.patient_reference, 'Patient/(.+)', 1) AS patient_id,
    REGEXP_EXTRACT(o.encounter_reference, 'Encounter/(.+)', 1) AS encounter_id,
    o.status AS observation_status,
    o.category_code,
    o.category_display,
    o.code_system,
    o.observation_code,
    o.observation_display,
    o.observation_text,
    o.effective_datetime,
    o.value_quantity,
    o.value_unit,
    o.value_string,
    o.value_codeable_text,
    COALESCE(CAST(o.value_quantity AS STRING), o.value_string, o.value_codeable_text) AS display_value,
    o.meta_last_updated,
    o.ingestion_date,
    p.full_name AS patient_name,
    p.gender AS patient_gender,
    p.age AS patient_age
FROM {CATALOG}.{SILVER_SCHEMA}.silver_observation o
LEFT JOIN {CATALOG}.{GOLD_SCHEMA}.dim_patient p
    ON REGEXP_EXTRACT(o.patient_reference, 'Patient/(.+)', 1) = p.patient_id
WHERE o.is_current = true
""")
print("Created: fact_observation")

# COMMAND ----------

# MAGIC %md
# MAGIC ## patient_summary - Aggregated patient-level metrics

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {CATALOG}.{GOLD_SCHEMA}.patient_summary AS
WITH enc AS (
    SELECT
        patient_id,
        COUNT(DISTINCT encounter_id) AS total_encounters,
        MIN(period_start) AS first_encounter,
        MAX(period_start) AS last_encounter,
        AVG(duration_hours) AS avg_duration_hours
    FROM {CATALOG}.{GOLD_SCHEMA}.fact_encounter
    GROUP BY patient_id
),
cond AS (
    SELECT
        REGEXP_EXTRACT(patient_reference, 'Patient/(.+)', 1) AS patient_id,
        COUNT(DISTINCT condition_code) AS distinct_conditions,
        COLLECT_SET(condition_display) AS condition_list
    FROM {CATALOG}.{SILVER_SCHEMA}.silver_condition
    WHERE is_current = true
    GROUP BY REGEXP_EXTRACT(patient_reference, 'Patient/(.+)', 1)
),
obs AS (
    SELECT
        patient_id,
        COUNT(DISTINCT observation_id) AS total_observations,
        COUNT(DISTINCT observation_code) AS distinct_obs_types,
        MAX(effective_datetime) AS latest_observation
    FROM {CATALOG}.{GOLD_SCHEMA}.fact_observation
    GROUP BY patient_id
)
SELECT
    p.patient_id,
    p.full_name,
    p.gender,
    p.age,
    p.birth_date,
    p.is_active,
    p.address_city,
    p.address_state,
    p.address_country,
    COALESCE(e.total_encounters, 0) AS total_encounters,
    e.first_encounter,
    e.last_encounter,
    ROUND(e.avg_duration_hours, 2) AS avg_duration_hours,
    COALESCE(c.distinct_conditions, 0) AS distinct_conditions,
    c.condition_list,
    COALESCE(o.total_observations, 0) AS total_observations,
    COALESCE(o.distinct_obs_types, 0) AS distinct_obs_types,
    o.latest_observation
FROM {CATALOG}.{GOLD_SCHEMA}.dim_patient p
LEFT JOIN enc e ON p.patient_id = e.patient_id
LEFT JOIN cond c ON p.patient_id = c.patient_id
LEFT JOIN obs o ON p.patient_id = o.patient_id
""")
print("Created: patient_summary")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify gold objects

# COMMAND ----------

for view_name in ["dim_patient", "dim_condition", "fact_encounter", "fact_observation", "patient_summary"]:
    try:
        cnt = spark.table(f"{CATALOG}.{GOLD_SCHEMA}.{view_name}").count()
        print(f"  {view_name}: {cnt} rows")
    except Exception as e:
        print(f"  {view_name}: ERROR - {e}")
