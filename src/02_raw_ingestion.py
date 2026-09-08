# Databricks notebook source
# MAGIC %md
# MAGIC # Raw Ingestion
# MAGIC Fetches data from HAPI FHIR R4 API incrementally (last N days) with pagination.
# MAGIC Saves each page as raw JSON into Unity Catalog Volumes and logs every call.

# COMMAND ----------

# MAGIC %run ./01_config

# COMMAND ----------

dbutils.widgets.text("resource_type", "Patient", "FHIR Resource Type")
RESOURCE_TYPE = dbutils.widgets.get("resource_type")
print(f"Ingesting: {RESOURCE_TYPE}")

# COMMAND ----------

import requests
import json
import uuid
from datetime import datetime, timezone
from pyspark.sql import Row

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fetch pages from FHIR API

# COMMAND ----------

def fetch_fhir_pages(resource_type):
    """
    Generator that fetches FHIR resources using _lastUpdated date filter
    and follows the 'next' pagination link.
    Yields (page_num, url, raw_json, count, timestamp, error) per page.
    """
    start_date, end_date = get_incremental_date_range()

    url = (
        f"{FHIR_BASE_URL}/{resource_type}"
        f"?_lastUpdated=ge{start_date}"
        f"&_lastUpdated=le{end_date}"
        f"&_count={PAGE_SIZE}"
        f"&_format=json"
    )

    page = 0
    while url and page < MAX_PAGES:
        ts = datetime.now(timezone.utc)
        page += 1
        print(f"  Page {page}: {url[:100]}...")

        try:
            resp = requests.get(url, timeout=60)
            resp.raise_for_status()
            raw = resp.text
            bundle = resp.json()
        except Exception as e:
            print(f"  Error on page {page}: {e}")
            yield (page, url, None, 0, ts, str(e))
            break

        entries = bundle.get("entry", [])
        if not entries:
            print(f"  No records on page {page}, stopping.")
            break

        yield (page, url, raw, len(entries), ts, None)

        # follow the next link
        url = None
        for link in bundle.get("link", []):
            if link.get("relation") == "next":
                url = link["url"]
                break

    print(f"Done fetching {resource_type}: {page} page(s)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save raw JSON to volumes and log metadata

# COMMAND ----------

today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
volume_path = get_volume_path(RESOURCE_TYPE, today_str)
dbutils.fs.mkdirs(volume_path)

total_records = 0
log_rows = []

for page_num, api_url, raw_json, count, ts, error in fetch_fhir_pages(RESOURCE_TYPE):
    save_ts = datetime.now(timezone.utc)

    if raw_json:
        file_path = f"{volume_path}/page_{page_num:04d}.json"
        dbutils.fs.put(file_path, raw_json, overwrite=True)
        total_records += count
        status = "SUCCESS"
        print(f"  Saved {file_path} ({count} records)")
    else:
        status = "ERROR"

    log_rows.append(Row(
        log_id=str(uuid.uuid4()),
        resource_type=RESOURCE_TYPE,
        api_url=api_url,
        page_number=page_num,
        records_fetched=count,
        extraction_timestamp=ts,
        save_timestamp=save_ts,
        status=status,
        error_message=error,
        ingestion_date=datetime.now(timezone.utc).date()
    ))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write ingestion log

# COMMAND ----------

if log_rows:
    log_df = spark.createDataFrame(log_rows, schema=get_ingestion_log_schema())
    log_df.write.format("delta").mode("append").saveAsTable(get_ingestion_log_table())
    print(f"Logged {len(log_rows)} entries to {get_ingestion_log_table()}")

# COMMAND ----------

print(f"--- Raw Ingestion Summary: {RESOURCE_TYPE} ---")
print(f"Date: {today_str}")
print(f"Pages: {len(log_rows)}, Records: {total_records}")
print(f"Volume: {volume_path}")
