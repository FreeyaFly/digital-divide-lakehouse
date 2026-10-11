# Databricks notebook source
# MAGIC %md
# MAGIC # 30 · Ookla discovery (one quarter, fixed broadband)
# MAGIC
# MAGIC Measures before building the Ookla pipeline, so scope is decided on facts:
# MAGIC
# MAGIC 1. Download one global quarter into the UC Volume and time it.
# MAGIC 2. Schema, global row count and the time Spark needs to read and aggregate it.
# MAGIC 3. How much of it falls in a Netherlands bounding box.
# MAGIC 4. Which spatial tools work on this compute: H3 functions, spatial SQL (`ST_`).
# MAGIC 5. Whether the Dutch municipality boundaries (PDOK) and CBS are reachable.
# MAGIC
# MAGIC No ADLS and no secrets: Ookla is public data on S3 (CC BY-NC-SA 4.0, © Ookla).

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "digital_divide_dev")
dbutils.widgets.text("year", "2026")
dbutils.widgets.text("quarter", "3")
dbutils.widgets.text("type", "fixed")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
YEAR = int(dbutils.widgets.get("year"))
QUARTER = int(dbutils.widgets.get("quarter"))
TYPE = dbutils.widgets.get("type")

MONTH = {1: "01", 2: "04", 3: "07", 4: "10"}[QUARTER]
FILE = f"{YEAR}-{MONTH}-01_performance_{TYPE}_tiles.parquet"
KEY = f"parquet/performance/type={TYPE}/year={YEAR}/quarter={QUARTER}/{FILE}"
SOURCE_URL = f"https://ookla-open-data.s3.amazonaws.com/{KEY}"
TARGET = f"/Volumes/{CATALOG}/{SCHEMA}/landing/ookla/type={TYPE}/year={YEAR}/quarter={QUARTER}/{FILE}"

# Netherlands bounding box (WGS84); it also catches slivers of Belgium and Germany,
# which the municipality join will remove later.
NL_BBOX = {"lon_min": 3.2, "lon_max": 7.3, "lat_min": 50.7, "lat_max": 53.6}

summary = {}

# COMMAND ----------

# MAGIC %md ## 1 · Download one quarter into the Volume

# COMMAND ----------

import os
import time

import requests

head = requests.head(SOURCE_URL, timeout=30)
head.raise_for_status()
size = int(head.headers["Content-Length"])

if os.path.exists(TARGET) and os.path.getsize(TARGET) == size:
    summary["download"] = f"already present ({size / 1e6:.0f} MB)"
else:
    os.makedirs(os.path.dirname(TARGET), exist_ok=True)
    start = time.time()
    with requests.get(SOURCE_URL, stream=True, timeout=120) as r, open(TARGET, "wb") as out:
        r.raise_for_status()
        for chunk in r.iter_content(chunk_size=8 * 1024 * 1024):
            out.write(chunk)
    seconds = time.time() - start
    summary["download"] = f"{size / 1e6:.0f} MB in {seconds:.0f} s ({size / 1e6 / seconds:.0f} MB/s)"

# Size of the other connection type, without downloading it.
other = "mobile" if TYPE == "fixed" else "fixed"
other_url = SOURCE_URL.replace(f"type={TYPE}", f"type={other}").replace(f"_{TYPE}_", f"_{other}_")
other_head = requests.head(other_url, timeout=30)
summary[f"{other}_same_quarter_mb"] = (
    round(int(other_head.headers["Content-Length"]) / 1e6) if other_head.ok else "not available")
print(summary)

# COMMAND ----------

# MAGIC %md ## 2 · Schema, size and Spark timing (global)

# COMMAND ----------

from pyspark.sql import functions as F

df = spark.read.parquet(TARGET)
df.printSchema()

start = time.time()
rows = df.count()
summary["global_rows"] = rows
summary["count_seconds"] = round(time.time() - start, 1)

start = time.time()
g = df.agg(
    F.sum("tests").alias("tests"),
    F.sum("devices").alias("devices"),
    (F.sum(F.col("avg_d_kbps") * F.col("tests")) / F.sum("tests") / 1000).alias("down_mbps"),
    (F.sum(F.col("avg_u_kbps") * F.col("tests")) / F.sum("tests") / 1000).alias("up_mbps"),
).first()
summary["aggregate_seconds"] = round(time.time() - start, 1)
summary["global_tests"] = int(g["tests"])
summary["global_down_mbps_test_weighted"] = round(g["down_mbps"], 1)
summary["global_up_mbps_test_weighted"] = round(g["up_mbps"], 1)
summary["columns"] = df.columns
print({k: summary[k] for k in ("global_rows", "count_seconds", "aggregate_seconds",
                               "global_tests", "global_down_mbps_test_weighted")})

# COMMAND ----------

# MAGIC %md ## 3 · Netherlands bounding box

# COMMAND ----------

if {"tile_x", "tile_y"} <= set(df.columns):
    # Newer files carry the tile centroid directly.
    lon, lat = F.col("tile_x").cast("double"), F.col("tile_y").cast("double")
    summary["centroid_source"] = "tile_x / tile_y columns"
else:
    # Fall back to the first corner of the tile polygon (WKT), good enough for a bbox.
    lon =F.regexp_extract("tile", r"POLYGON\s*\(\(\s*([-0-9.]+)", 1).cast("double")
    lat = F.regexp_extract("tile", r"POLYGON\s*\(\(\s*[-0-9.]+\s+([-0-9.]+)", 1).cast("double")
    summary["centroid_source"] = "parsed from the WKT tile polygon"

start = time.time()
nl = df.where(lon.between(NL_BBOX["lon_min"], NL_BBOX["lon_max"])
              & lat.between(NL_BBOX["lat_min"], NL_BBOX["lat_max"]))
n = nl.agg(
    F.count("*").alias("tiles"),
    F.sum("tests").alias("tests"),
    (F.sum(F.col("avg_d_kbps") * F.col("tests")) / F.sum("tests") / 1000).alias("down_mbps"),
).first()
summary["nl_bbox_seconds"] = round(time.time() - start, 1)
summary["nl_bbox_tiles"] = int(n["tiles"])
summary["nl_bbox_share_of_rows_pct"] = round(100 * n["tiles"] / rows, 2)
summary["nl_bbox_tests"] = int(n["tests"])
summary["nl_bbox_down_mbps_test_weighted"] = round(n["down_mbps"], 1)
print({k: v for k, v in summary.items() if k.startswith(("nl_", "centroid"))})

# COMMAND ----------

# MAGIC %md ## 4 · Spatial capabilities on this compute

# COMMAND ----------

PROBES = {
    "h3_longlatash3": "SELECT h3_longlatash3(4.9041, 52.3676, 9)",
    "h3_polyfillash3": ("SELECT size(h3_polyfillash3("
                        "'POLYGON((4.88 52.36, 4.92 52.36, 4.92 52.38, 4.88 52.38, 4.88 52.36))', 9))"),
    "h3_tessellateaswkb": ("SELECT size(h3_tessellateaswkb("
                           "'POLYGON((4.88 52.36, 4.92 52.36, 4.92 52.38, 4.88 52.38, 4.88 52.36))', 9))"),
    "st_point_contains": ("SELECT st_contains(st_geomfromtext("
                          "'POLYGON((4.88 52.36, 4.92 52.36, 4.92 52.38, 4.88 52.38, 4.88 52.36))'),"
                          " st_point(4.90, 52.37))"),
}
for name, sql in PROBES.items():
    try:
        summary[f"probe_{name}"] = f"PASS ({spark.sql(sql).first()[0]})"
    except Exception as e:
        summary[f"probe_{name}"] = f"FAIL: {type(e).__name__}: {str(e).splitlines()[0][:160]}"
    print(f"{name:<22} {summary[f'probe_{name}']}")

# COMMAND ----------

# MAGIC %md ## 5 · Can we reach the Dutch boundary and statistics sources?

# COMMAND ----------

SOURCES = {
    "pdok_gemeentegebied": ("https://api.pdok.nl/kadaster/bestuurlijkegebieden/ogc/v1/"
                            "collections/gemeentegebied/items?f=json&limit=1"),
    "cbs_odata_catalog": "https://opendata.cbs.nl/ODataApi/odata/",
    "cbs_odata_tables": "https://opendata.cbs.nl/ODataCatalog/Tables?$top=1&$format=json",
}
for name, url in SOURCES.items():
    try:
        r = requests.get(url, timeout=30)
        detail = ""
        if r.ok and "json" in r.headers.get("Content-Type", ""):
            body = r.json()
            if isinstance(body, dict) and body.get("features"):
                detail = f", feature properties: {sorted(body['features'][0]['properties'])}"
        summary[f"source_{name}"] = f"HTTP {r.status_code}{detail}"
    except Exception as e:
        summary[f"source_{name}"] = f"FAIL: {type(e).__name__}: {e}"
    print(f"{name:<22} {summary[f'source_{name}']}")

# COMMAND ----------

# MAGIC %md ## Summary

# COMMAND ----------

for k, v in summary.items():
    if k != "columns":
        print(f"{k:<36} {v}")
print(f"{'columns':<36} {summary['columns']}")
