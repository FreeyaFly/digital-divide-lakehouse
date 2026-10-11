# Databricks notebook source
# MAGIC %md
# MAGIC # 31 · Ookla: public S3 → Bronze (global tiles)
# MAGIC
# MAGIC For each quarter and connection type (one Parquet file each):
# MAGIC
# MAGIC 1. Skip it if `ingestion_versions` already records it as complete (so a run that stops
# MAGIC    half-way, e.g. on a quota, resumes where it left off).
# MAGIC 2. Download it into the UC Volume.
# MAGIC 3. Read it, check the columns, and derive tile centroids if an older file lacks them.
# MAGIC 4. Overwrite exactly its partition of `bronze_ookla_tiles` (`replaceWhere`), so a re-run
# MAGIC    replaces rather than duplicates.
# MAGIC 5. Reconcile: rows in the partition must equal rows in the file.
# MAGIC 6. Record it as complete, then delete the raw file. The public, immutable S3 bucket is the
# MAGIC    archive; keeping a second copy would double storage (ADR 0003).
# MAGIC
# MAGIC Speed test data © Ookla, 2019–present, CC BY-NC-SA 4.0.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "digital_divide_dev")
dbutils.widgets.text("latest_quarters", "8")
dbutils.widgets.text("types", "fixed,mobile")
dbutils.widgets.text("delete_raw", "true")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
LATEST_QUARTERS = int(dbutils.widgets.get("latest_quarters"))
TYPES = [t.strip() for t in dbutils.widgets.get("types").split(",") if t.strip()]
DELETE_RAW = dbutils.widgets.get("delete_raw").strip().lower() == "true"

T = f"{CATALOG}.{SCHEMA}"
BRONZE = f"{T}.bronze_ookla_tiles"
INGESTION_VERSIONS = f"{T}.ingestion_versions"
VOLUME_ROOT = f"/Volumes/{CATALOG}/{SCHEMA}/landing/ookla"
S3 = "https://ookla-open-data.s3.amazonaws.com/parquet/performance"
MONTH = {1: "01", 2: "04", 3: "07", 4: "10"}

# COMMAND ----------

# MAGIC %md ## 1 · Which quarters?

# COMMAND ----------

import os
import time
from datetime import date

import requests
from pyspark.sql import functions as F


def file_name(conn_type: str, year: int, quarter: int) -> str:
    return f"{year}-{MONTH[quarter]}-01_performance_{conn_type}_tiles.parquet"


def url(conn_type: str, year: int, quarter: int) -> str:
    return (f"{S3}/type={conn_type}/year={year}/quarter={quarter}/"
            f"{file_name(conn_type, year, quarter)}")


def previous(year: int, quarter: int) -> tuple[int, int]:
    return (year, quarter - 1) if quarter > 1 else (year - 1, 4)


# Walk back from the current quarter to the newest one Ookla has published (fixed is the reference).
today = date.today()
y, q = today.year, (today.month - 1) // 3 + 1
for _ in range(8):
    if requests.head(url("fixed", y, q), timeout=30).ok:
        break
    y, q = previous(y, q)
else:
    raise RuntimeError("No published Ookla quarter found in the last two years")

QUARTERS = []
for _ in range(LATEST_QUARTERS):
    QUARTERS.append((y, q))
    y, q = previous(y, q)
QUARTERS.reverse()
print(f"Quarters: {QUARTERS[0][0]} Q{QUARTERS[0][1]} … {QUARTERS[-1][0]} Q{QUARTERS[-1][1]} "
      f"({len(QUARTERS)}) × types {TYPES} = {len(QUARTERS) * len(TYPES)} files")

# COMMAND ----------

# MAGIC %md ## 2 · Tables

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {BRONZE} (
        quadkey STRING, tile STRING, tile_x DOUBLE, tile_y DOUBLE,
        avg_d_kbps BIGINT, avg_u_kbps BIGINT, avg_lat_ms BIGINT,
        avg_lat_down_ms DOUBLE, avg_lat_up_ms DOUBLE, tests BIGINT, devices BIGINT,
        conn_type STRING, year INT, quarter INT, period_start DATE,
        centroid_derived BOOLEAN, _source_url STRING, _loaded_at TIMESTAMP
    )
    PARTITIONED BY (conn_type, year, quarter)
    COMMENT 'Ookla global speed-test tiles (zoom 16), one partition per connection type and quarter'
""")
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {INGESTION_VERSIONS} (
        source STRING NOT NULL, source_updated STRING NOT NULL, indicators ARRAY<STRING>,
        countries BOOLEAN, files INT, `rows` BIGINT, completed_at TIMESTAMP
    ) COMMENT 'Releases fully landed in Bronze and reconciled'
""")

SOURCE_COLUMNS = {
    "quadkey": "string", "tile": "string", "tile_x": "double", "tile_y": "double",
    "avg_d_kbps": "bigint", "avg_u_kbps": "bigint", "avg_lat_ms": "bigint",
    "avg_lat_down_ms": "double", "avg_lat_up_ms": "double", "tests": "bigint", "devices": "bigint",
}
REQUIRED = {"quadkey", "tile", "avg_d_kbps", "avg_u_kbps", "avg_lat_ms", "tests", "devices"}

# COMMAND ----------

# MAGIC %md ## 3 · Load each file

# COMMAND ----------


def completed_rows(source: str, period: str):
    r = (spark.table(INGESTION_VERSIONS)
         .where((F.col("source") == source) & (F.col("source_updated") == period))
         .select("rows").first())
    return r["rows"] if r else None


def record_complete(source: str, period: str, rows: int) -> None:
    spark.createDataFrame(
        [(source, period, [], False, 1, rows)],
        "source STRING, source_updated STRING, indicators ARRAY<STRING>, countries BOOLEAN, "
        "files INT, `rows` BIGINT",
    ).createOrReplaceTempView("completed_file")
    spark.sql(f"""
        MERGE INTO {INGESTION_VERSIONS} t USING completed_file s
        ON t.source = s.source AND t.source_updated = s.source_updated
        WHEN MATCHED THEN UPDATE SET t.`rows` = s.`rows`, t.files = s.files,
                                     t.completed_at = current_timestamp()
        WHEN NOT MATCHED THEN INSERT (source, source_updated, indicators, countries, files, `rows`,
                                      completed_at)
                              VALUES (s.source, s.source_updated, s.indicators, s.countries,
                                      s.files, s.`rows`, current_timestamp())
    """)


results = []
for conn_type in TYPES:
    for year, quarter in QUARTERS:
        source, period = f"ookla_{conn_type}", f"{year}-{MONTH[quarter]}-01"
        partition = f"conn_type = '{conn_type}' AND year = {year} AND quarter = {quarter}"
        label = f"{conn_type} {year} Q{quarter}"

        recorded = completed_rows(source, period)
        if recorded is not None and spark.table(BRONZE).where(partition).count() == recorded:
            results.append((label, "skipped (complete)", recorded, 0.0))
            continue

        start = time.time()
        src_url = url(conn_type, year, quarter)
        target = f"{VOLUME_ROOT}/type={conn_type}/year={year}/quarter={quarter}/" \
                 f"{file_name(conn_type, year, quarter)}"
        size = int(requests.head(src_url, timeout=30).headers["Content-Length"])
        if not (os.path.exists(target) and os.path.getsize(target) == size):
            os.makedirs(os.path.dirname(target), exist_ok=True)
            with requests.get(src_url, stream=True, timeout=120) as r, open(target, "wb") as out:
                r.raise_for_status()
                for chunk in r.iter_content(chunk_size=8 * 1024 * 1024):
                    out.write(chunk)

        raw = spark.read.parquet(target)
        missing = REQUIRED - set(raw.columns)
        assert not missing, f"{label}: required columns missing: {sorted(missing)}"

        # Older files may lack the centroid or the split latency columns: derive or fill.
        derived = not {"tile_x", "tile_y"} <= set(raw.columns)
        if derived:
            raw = raw.withColumn("tile_x", F.expr("st_x(st_centroid(st_geomfromtext(tile)))")) \
                     .withColumn("tile_y", F.expr("st_y(st_centroid(st_geomfromtext(tile)))"))
        for col, dtype in SOURCE_COLUMNS.items():
            if col not in raw.columns:
                raw = raw.withColumn(col, F.lit(None).cast(dtype))

        source_rows = raw.count()
        bronze_df = raw.select(
            *[F.col(c).cast(t).alias(c) for c, t in SOURCE_COLUMNS.items()],
            F.lit(conn_type).alias("conn_type"),
            F.lit(year).cast("int").alias("year"),
            F.lit(quarter).cast("int").alias("quarter"),
            F.to_date(F.lit(period)).alias("period_start"),
            F.lit(derived).alias("centroid_derived"),
            F.lit(src_url).alias("_source_url"),
            F.current_timestamp().alias("_loaded_at"),
        )
        (bronze_df.write.format("delta").mode("overwrite")
            .option("replaceWhere", partition).saveAsTable(BRONZE))

        written = spark.table(BRONZE).where(partition).count()
        assert written == source_rows, f"{label}: {written} rows written, {source_rows} in the file"
        record_complete(source, period, written)
        if DELETE_RAW:
            os.remove(target)
        results.append((label, "loaded" + (", centroid derived" if derived else ""),
                        written, round(time.time() - start, 1)))
        print(f"{label}: {written:,} rows in {results[-1][3]} s")

# COMMAND ----------

# MAGIC %md ## 4 · Summary and storage

# COMMAND ----------

display(spark.createDataFrame(results, "file STRING, status STRING, rows BIGINT, seconds DOUBLE"))

detail = spark.sql(f"DESCRIBE DETAIL {BRONZE}").first()
total_rows = spark.table(BRONZE).count()
loaded = sum(r[2] for r in results if r[1].startswith("loaded"))
print(f"Loaded this run: {loaded:,} rows | Bronze total: {total_rows:,} rows | "
      f"Delta size: {detail['sizeInBytes'] / 1e9:.2f} GB in {detail['numFiles']} files")
display(spark.sql(f"""
    SELECT conn_type, year, quarter, count(*) AS tiles, sum(tests) AS tests
    FROM {BRONZE} GROUP BY ALL ORDER BY conn_type, year, quarter
"""))
