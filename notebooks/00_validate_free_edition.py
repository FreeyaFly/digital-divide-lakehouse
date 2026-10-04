# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Validate Databricks Free Edition
# MAGIC
# MAGIC Answers the Week 1 go/no-go questions before any pipeline code is written:
# MAGIC
# MAGIC 1. Can we create our own schema and volume in Unity Catalog?
# MAGIC 2. Can serverless compute reach the source systems (World Bank API, Ookla S3, CBS)?
# MAGIC 3. Can it reach our ADLS Gen2 account, over HTTPS and through Spark?
# MAGIC
# MAGIC Record the results in `docs/week1-checklist.md`.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "digital_divide_dev")
dbutils.widgets.text("storage_account", "")  # Terraform output: storage_account_name

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
STORAGE_ACCOUNT = dbutils.widgets.get("storage_account").strip()

results = {}

# COMMAND ----------

# MAGIC %md ## 1 · Unity Catalog: schema and volume

# COMMAND ----------

try:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.landing")
    probe = f"/Volumes/{CATALOG}/{SCHEMA}/landing/_probe.txt"
    dbutils.fs.put(probe, "ok", overwrite=True)
    dbutils.fs.rm(probe)
    results["uc_schema_and_volume"] = "PASS"
except Exception as e:
    results["uc_schema_and_volume"] = f"FAIL: {type(e).__name__}: {e}"

# COMMAND ----------

# MAGIC %md ## 2 · Outbound access to sources

# COMMAND ----------

import requests

SOURCES = {
    "world_bank_api": "https://api.worldbank.org/v2/country/NLD/indicator/IT.NET.USER.ZS?format=json&per_page=1",
    "ookla_s3": "https://ookla-open-data.s3.amazonaws.com/parquet/performance/type=fixed/year=2024/quarter=1/2024-01-01_performance_fixed_tiles.parquet",
    "cbs_statline": "https://opendata.cbs.nl/ODataApi/odata/",
}

for name, url in SOURCES.items():
    try:
        # HEAD for the large Parquet file so nothing is downloaded.
        r = requests.head(url, timeout=20, allow_redirects=True) if name == "ookla_s3" \
            else requests.get(url, timeout=20)
        size = r.headers.get("Content-Length")
        results[f"egress_{name}"] = f"HTTP {r.status_code}" + (f", {int(size)/1e6:.0f} MB" if size else "")
    except Exception as e:
        results[f"egress_{name}"] = f"FAIL: {type(e).__name__}: {e}"

# COMMAND ----------

# MAGIC %md ## 3 · ADLS Gen2 reachability
# MAGIC
# MAGIC Two separate questions:
# MAGIC * **HTTPS**: can Python code reach `*.dfs.core.windows.net`? If so, the ingest jobs can upload to ADLS with an SDK.
# MAGIC * **Spark**: can Spark read `abfss://` paths? Serverless compute usually needs a Unity Catalog external location for that, which may not be possible on Free Edition.

# COMMAND ----------

if not STORAGE_ACCOUNT:
    results["adls"] = "SKIPPED: set the storage_account widget"
else:
    dfs = f"https://{STORAGE_ACCOUNT}.dfs.core.windows.net/landing?resource=filesystem"
    try:
        r = requests.get(dfs, timeout=20)
        # 401/403 still proves the endpoint is reachable; auth comes later.
        results["adls_https"] = f"HTTP {r.status_code} (reachable)"
    except Exception as e:
        results["adls_https"] = f"FAIL: {type(e).__name__}: {e}"

    try:
        spark.read.format("text").load(f"abfss://landing@{STORAGE_ACCOUNT}.dfs.core.windows.net/").limit(1).collect()
        results["adls_spark"] = "PASS"
    except Exception as e:
        results["adls_spark"] = f"FAIL: {type(e).__name__}: {str(e)[:200]}"

# COMMAND ----------

for k, v in results.items():
    print(f"{k:<28} {v}")
