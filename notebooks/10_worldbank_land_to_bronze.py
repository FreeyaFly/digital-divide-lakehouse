# Databricks notebook source
# MAGIC %md
# MAGIC # 10 · World Bank: ADLS landing → UC Volume → Bronze
# MAGIC
# MAGIC Free Edition cannot read ADLS with Spark (see ADR 0001), so this notebook bridges it:
# MAGIC
# MAGIC 1. List `landing/worldbank/` in ADLS over HTTPS, using a read/list user-delegation SAS
# MAGIC    kept in a Databricks secret scope.
# MAGIC 2. Copy new data files into the UC Volume, keeping the same relative paths.
# MAGIC 3. `COPY INTO` two Bronze tables. COPY INTO remembers which files it has loaded,
# MAGIC    so re-running loads nothing twice.
# MAGIC 4. Reconcile Bronze row counts per file against the ingestion manifests.
# MAGIC
# MAGIC Bronze keeps every field as a string (`primitivesAsString`); typing happens in Silver.

# COMMAND ----------

dbutils.widgets.text("storage_account", "")
dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "digital_divide_dev")
dbutils.widgets.text("secret_scope", "ddlake")
dbutils.widgets.text("secret_key", "adls-landing-sas")

STORAGE_ACCOUNT = dbutils.widgets.get("storage_account").strip()
CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
assert STORAGE_ACCOUNT, "Set the storage_account widget (Terraform output storage_account_name)"

SAS = dbutils.secrets.get(dbutils.widgets.get("secret_scope"), dbutils.widgets.get("secret_key"))

PREFIX = "worldbank"
VOLUME_ROOT = f"/Volumes/{CATALOG}/{SCHEMA}/landing"
BRONZE_OBSERVATIONS = f"{CATALOG}.{SCHEMA}.bronze_worldbank_observations"
BRONZE_COUNTRIES = f"{CATALOG}.{SCHEMA}.bronze_worldbank_countries"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.landing")

# COMMAND ----------

# MAGIC %md ## 1 · List the landing files in ADLS

# COMMAND ----------

import json
import os
from urllib.parse import quote

import requests

DFS = f"https://{STORAGE_ACCOUNT}.dfs.core.windows.net/landing"
BLOB = f"https://{STORAGE_ACCOUNT}.blob.core.windows.net/landing"


def list_adls_files(directory: str) -> list[dict]:
    """ADLS Gen2 'List Paths' REST call, following continuation tokens."""
    files, continuation = [], None
    while True:
        url = f"{DFS}?resource=filesystem&recursive=true&directory={quote(directory)}&{SAS}"
        if continuation:
            url += f"&continuation={quote(continuation)}"
        response = requests.get(url, timeout=60)
        if response.status_code == 403:
            raise PermissionError("ADLS returned 403: the SAS has probably expired; regenerate it")
        response.raise_for_status()
        for p in response.json().get("paths", []):
            if p.get("isDirectory") != "true":
                files.append({"path": p["name"], "size": int(p.get("contentLength", 0))})
        continuation = response.headers.get("x-ms-continuation")
        if not continuation:
            return files


def download(path: str) -> bytes:
    response = requests.get(f"{BLOB}/{quote(path, safe='/')}?{SAS}", timeout=120)
    response.raise_for_status()
    return response.content


adls_files = list_adls_files(PREFIX)
data_files = [f for f in adls_files if f["path"].endswith(".jsonl.gz")]
manifest_files = [f for f in adls_files if f["path"].startswith(f"{PREFIX}/_manifests/")]
print(f"ADLS: {len(data_files)} data files, {len(manifest_files)} manifests")

# COMMAND ----------

# MAGIC %md ## 2 · Copy new data files into the Volume

# COMMAND ----------

copied = skipped = 0
for f in data_files:
    target = f"{VOLUME_ROOT}/{f['path']}"
    # Same relative path and same size means this exact file is already there.
    if os.path.exists(target) and os.path.getsize(target) == f["size"]:
        skipped += 1
        continue
    os.makedirs(os.path.dirname(target), exist_ok=True)
    with open(target, "wb") as out:
        out.write(download(f["path"]))
    copied += 1

print(f"Volume: copied {copied}, already present {skipped}")

# COMMAND ----------

# MAGIC %md ## 3 · COPY INTO Bronze

# COMMAND ----------


def copy_into(table: str, pattern: str) -> dict:
    # An empty Delta table is enough; COPY INTO infers the columns on first load.
    spark.sql(f"CREATE TABLE IF NOT EXISTS {table}")
    metrics = spark.sql(f"""
        COPY INTO {table}
        FROM (
            SELECT
                *,
                _metadata.file_path AS _source_file,
                regexp_extract(_metadata.file_path, 'source_updated=([0-9-]+)', 1) AS _source_updated,
                current_timestamp() AS _loaded_at
            FROM '{VOLUME_ROOT}/{PREFIX}'
        )
        FILEFORMAT = JSON
        PATTERN = '{pattern}'
        FORMAT_OPTIONS ('primitivesAsString' = 'true')
        COPY_OPTIONS ('mergeSchema' = 'true')
    """).first().asDict()
    print(f"{table}: {metrics}")
    return metrics


_ = copy_into(BRONZE_OBSERVATIONS, "source_updated=*/indicators/indicator=*/*.jsonl.gz")
_ = copy_into(BRONZE_COUNTRIES, "source_updated=*/countries/*.jsonl.gz")

# COMMAND ----------

# MAGIC %md ## 4 · Reconcile Bronze against the ingestion manifests

# COMMAND ----------

# Expected rows per file: the latest landed manifest that mentions the file wins.
expected = {}
for m in sorted(manifest_files, key=lambda f: f["path"]):
    manifest = json.loads(download(m["path"]))
    if manifest.get("status") == "landed":
        for f in manifest["files"]:
            expected[f["path"]] = f["rows"]

actual = {}
for table in (BRONZE_OBSERVATIONS, BRONZE_COUNTRIES):
    rows = spark.sql(f"""
        SELECT regexp_extract(_source_file, '({PREFIX}/.*)$', 1) AS path, count(*) AS n
        FROM {table} GROUP BY 1
    """).collect()
    actual.update({r["path"]: r["n"] for r in rows})

mismatches = {p: (expected.get(p), actual.get(p))
              for p in set(expected) | set(actual) if expected.get(p) != actual.get(p)}
print(f"Files reconciled: {len(expected)} expected, {len(actual)} in Bronze")
print(f"Rows: expected {sum(expected.values()):,}, in Bronze {sum(actual.values()):,}")
assert not mismatches, f"Row count mismatches (expected, actual): {mismatches}"
print("Reconciliation OK")
