# Databricks notebook source
# MAGIC %md
# MAGIC # 33 · Ookla Netherlands: Bronze tiles → municipalities (Silver)
# MAGIC
# MAGIC 1. Take Bronze tiles of every quarter recorded as complete in `ingestion_versions`.
# MAGIC 2. Keep the Netherlands bounding box, convert each tile centroid to an H3 cell (resolution 9)
# MAGIC    and join it to `silver_nl_municipality_h3`, an equi-join instead of a point-in-polygon join.
# MAGIC 3. MERGE into `silver_ookla_nl_tiles`, keyed on (conn_type, year, quarter, quadkey).
# MAGIC 4. Checks: tiles in the box = matched + unmatched; one row per key; municipality coverage.
# MAGIC 5. Accuracy: for the latest quarter, compare the H3 assignment with an exact
# MAGIC    `st_contains` assignment.
# MAGIC
# MAGIC Municipality-level aggregates belong in the dbt Gold layer (week 5); the last cell only
# MAGIC previews them.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "digital_divide_dev")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
T = f"{CATALOG}.{SCHEMA}"
BRONZE = f"{T}.bronze_ookla_tiles"
INGESTION_VERSIONS = f"{T}.ingestion_versions"
SILVER_MUNICIPALITIES = f"{T}.silver_nl_municipalities"
SILVER_MUNICIPALITY_H3 = f"{T}.silver_nl_municipality_h3"
SILVER_TILES = f"{T}.silver_ookla_nl_tiles"
H3_RESOLUTION = 9
NL_BBOX = (3.2, 7.3, 50.7, 53.6)  # lon_min, lon_max, lat_min, lat_max

# COMMAND ----------

# MAGIC %md ## 1 · Completed quarters → Netherlands tiles → municipalities

# COMMAND ----------

from pyspark.sql import functions as F

completed = (
    spark.table(INGESTION_VERSIONS)
    .where(F.col("source").isin("ookla_fixed", "ookla_mobile"))
    .select(F.regexp_replace("source", "^ookla_", "").alias("conn_type"),
            F.to_date("source_updated").alias("period_start"))
)
periods = completed.count()
assert periods > 0, "No completed Ookla quarters; run Notebook 31 first"

lon_min, lon_max, lat_min, lat_max = NL_BBOX
in_box = (
    spark.table(BRONZE)
    .join(completed, ["conn_type", "period_start"])
    .where(F.col("tile_x").between(lon_min, lon_max) & F.col("tile_y").between(lat_min, lat_max))
    .withColumn("h3_r9", F.expr(f"h3_longlatash3(tile_x, tile_y, {H3_RESOLUTION})"))
)
municipalities = spark.table(SILVER_MUNICIPALITIES).select(
    "gemeente_code", "gemeente_naam", "provincie_naam")
assigned = (
    in_box.join(spark.table(SILVER_MUNICIPALITY_H3), "h3_r9", "left")
    .join(municipalities, "gemeente_code", "left")
)

box_tiles = in_box.count()
matched = assigned.where("gemeente_code IS NOT NULL").count()
unmatched = assigned.where("gemeente_code IS NULL").count()
print(f"Completed periods: {periods} | tiles in NL box: {box_tiles:,} | "
      f"matched to a municipality: {matched:,} | outside NL (BE/DE/sea): {unmatched:,}")
assert box_tiles == matched + unmatched, "Row conservation failed: the join changed the row count"

# COMMAND ----------

# MAGIC %md ## 2 · MERGE into Silver

# COMMAND ----------

KEY = ["conn_type", "year", "quarter", "quadkey"]
VALUE_COLS = ["period_start", "tile_x", "tile_y", "h3_r9", "gemeente_code", "gemeente_naam",
              "provincie_naam", "avg_d_kbps", "avg_u_kbps", "avg_lat_ms", "tests", "devices"]

silver_src = assigned.where("gemeente_code IS NOT NULL").select(*KEY, *VALUE_COLS)
dupes = silver_src.groupBy(*KEY).count().where("count > 1").count()
assert dupes == 0, f"{dupes} duplicate (conn_type, year, quarter, quadkey) keys"

silver_src = silver_src.withColumn(
    "row_hash",
    F.sha2(F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("∅")) for c in VALUE_COLS]),
           256))
silver_src.createOrReplaceTempView("src_ookla_nl_tiles")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {SILVER_TILES} (
        conn_type STRING NOT NULL, year INT NOT NULL, quarter INT NOT NULL, quadkey STRING NOT NULL,
        period_start DATE, tile_x DOUBLE, tile_y DOUBLE, h3_r9 BIGINT,
        gemeente_code STRING, gemeente_naam STRING, provincie_naam STRING,
        avg_d_kbps BIGINT, avg_u_kbps BIGINT, avg_lat_ms BIGINT, tests BIGINT, devices BIGINT,
        row_hash STRING, updated_at TIMESTAMP
    ) COMMENT 'Ookla tiles inside Dutch municipalities (current boundaries), one row per tile and quarter'
""")
cols = KEY + VALUE_COLS + ["row_hash"]
metrics = spark.sql(f"""
    MERGE INTO {SILVER_TILES} t
    USING src_ookla_nl_tiles s
    ON {" AND ".join(f"t.{k} = s.{k}" for k in KEY)}
    WHEN MATCHED AND t.row_hash <> s.row_hash THEN
        UPDATE SET {", ".join(f"t.{c} = s.{c}" for c in cols)}, t.updated_at = current_timestamp()
    WHEN NOT MATCHED THEN
        INSERT ({", ".join(cols)}, updated_at) VALUES ({", ".join(f"s.{c}" for c in cols)},
                                                       current_timestamp())
    WHEN NOT MATCHED BY SOURCE THEN DELETE
""").first().asDict()
print(f"{SILVER_TILES}: {metrics}")

# COMMAND ----------

# MAGIC %md ## 3 · Checks and coverage

# COMMAND ----------

total_municipalities = spark.table(SILVER_MUNICIPALITIES).count()
coverage = spark.sql(f"""
    SELECT conn_type, year, quarter, count(*) AS tiles, sum(tests) AS tests,
           count(DISTINCT gemeente_code) AS municipalities
    FROM {SILVER_TILES} GROUP BY ALL ORDER BY conn_type, year, quarter
""")
display(coverage)
silver_rows = spark.table(SILVER_TILES).count()
assert silver_rows == matched, f"Silver has {silver_rows:,} rows, expected {matched:,} matched tiles"

latest = spark.table(SILVER_TILES).agg(F.max("period_start")).first()[0]
for conn_type in ("fixed", "mobile"):
    present = {r[0] for r in spark.table(SILVER_TILES)
               .where((F.col("period_start") == latest) & (F.col("conn_type") == conn_type))
               .select("gemeente_code").distinct().collect()}
    missing = (spark.table(SILVER_MUNICIPALITIES)
               .where(~F.col("gemeente_code").isin(list(present)) if present else F.lit(True))
               .select("gemeente_naam").orderBy("gemeente_naam").collect())
    print(f"{latest} {conn_type}: {len(present)}/{total_municipalities} municipalities with data"
          + (f"; missing: {[r[0] for r in missing]}" if missing else ""))

# COMMAND ----------

# MAGIC %md ## 4 · Accuracy of the H3 approximation (latest quarter, fixed)

# COMMAND ----------

# Exact assignment: point-in-polygon against every municipality boundary, for one quarter only.
sample = in_box.where((F.col("conn_type") == "fixed") & (F.col("period_start") == latest)) \
               .select("quadkey", "tile_x", "tile_y")
h3_assignment = assigned.where((F.col("conn_type") == "fixed") & (F.col("period_start") == latest)) \
                        .select("quadkey", F.col("gemeente_code").alias("h3_gemeente"))
# Bounding box per municipality (from the WKT coordinates), so each tile is tested only against
# the one or two municipalities whose box contains it, not all of them.
coord = r"(-?[0-9.]+) (-?[0-9.]+)"
boxes = spark.table(SILVER_MUNICIPALITIES).select(
    "gemeente_code", "boundary_wkt",
    F.expr(f"array_min(transform(regexp_extract_all(boundary_wkt, '{coord}', 1), x -> double(x)))")
     .alias("xmin"),
    F.expr(f"array_max(transform(regexp_extract_all(boundary_wkt, '{coord}', 1), x -> double(x)))")
     .alias("xmax"),
    F.expr(f"array_min(transform(regexp_extract_all(boundary_wkt, '{coord}', 2), x -> double(x)))")
     .alias("ymin"),
    F.expr(f"array_max(transform(regexp_extract_all(boundary_wkt, '{coord}', 2), x -> double(x)))")
     .alias("ymax"),
)
exact = (
    sample.join(F.broadcast(boxes),
                F.col("tile_x").between(F.col("xmin"), F.col("xmax"))
                & F.col("tile_y").between(F.col("ymin"), F.col("ymax")))
    .where(F.expr("st_contains(st_geomfromtext(boundary_wkt), st_point(tile_x, tile_y))"))
    .select("quadkey", F.col("gemeente_code").alias("exact_gemeente"))
)
comparison = (
    h3_assignment.join(exact, "quadkey", "full")
    .withColumn("outcome", F.expr("""
        CASE WHEN h3_gemeente IS NULL AND exact_gemeente IS NULL THEN 'both outside NL'
             WHEN h3_gemeente = exact_gemeente THEN 'same municipality'
             WHEN h3_gemeente IS NULL THEN 'only exact assigns'
             WHEN exact_gemeente IS NULL THEN 'only H3 assigns'
             ELSE 'different municipality' END"""))
    .groupBy("outcome").count()
)
total = comparison.agg(F.sum("count")).first()[0]
display(comparison.withColumn("pct", F.round(100 * F.col("count") / F.lit(total), 3))
                  .orderBy(F.desc("count")))

# COMMAND ----------

# MAGIC %md ## 5 · Preview: latest quarter by municipality (Gold comes in week 5)

# COMMAND ----------

display(spark.sql(f"""
    SELECT conn_type, gemeente_naam, provincie_naam, count(*) AS tiles, sum(tests) AS tests,
           round(sum(avg_d_kbps * tests) / sum(tests) / 1000, 1) AS down_mbps,
           round(sum(avg_u_kbps * tests) / sum(tests) / 1000, 1) AS up_mbps
    FROM {SILVER_TILES}
    WHERE period_start = DATE'{latest}'
    GROUP BY ALL
    HAVING sum(tests) >= 100
    ORDER BY conn_type, down_mbps DESC
"""))
