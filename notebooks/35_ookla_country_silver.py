# Databricks notebook source
# MAGIC %md
# MAGIC # 35 · Ookla global: Bronze tiles → countries (Silver)
# MAGIC
# MAGIC 1. **Mapping** `silver_ookla_tile_country`: one row per distinct tile (quadkey) with its
# MAGIC    country. A tile never changes country, so each tile is assigned once; later runs only
# MAGIC    assign tiles not seen before. Assignment uses the H3 chips from Notebook 34:
# MAGIC    - pass 1: the tile's own resolution-5 cell. A core cell decides by cell id alone; in a
# MAGIC      border cell the tile is tested against the cell's chip (`st_contains`);
# MAGIC    - pass 2: a tile not placed by pass 1 is tested against the chips of the six
# MAGIC      neighbouring cells, which catches points within metres of a cell edge;
# MAGIC    - still unplaced: outside every country (sea, or a coast drawn inland at 1:10m).
# MAGIC 2. **Aggregate** `silver_ookla_country_quarter`: one row per connection type, quarter and
# MAGIC    country with *additive* measures (Σ tests, Σ devices, Σ speed × tests), so Gold can
# MAGIC    re-weight correctly at any level (region, income group). MERGE with a row hash.
# MAGIC 3. **Checks** against external expected values: tile counts per quarter from
# MAGIC    `ingestion_versions`; an independent shapely assignment of fixed 2026 Q3; the week-3
# MAGIC    Dutch Silver table, built with a different method and different boundaries.
# MAGIC 4. **Performance**: timings per step, and a comparison of the chip method with a plain
# MAGIC    bounding-box + `st_contains` join on a 0.1% sample.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "digital_divide_dev")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
T = f"{CATALOG}.{SCHEMA}"
BRONZE = f"{T}.bronze_ookla_tiles"
INGESTION_VERSIONS = f"{T}.ingestion_versions"
SILVER_BOUNDARIES = f"{T}.silver_country_boundaries"
SILVER_CHIPS = f"{T}.silver_country_h3_chips"
SILVER_TILE_COUNTRY = f"{T}.silver_ookla_tile_country"
SILVER_COUNTRY_QUARTER = f"{T}.silver_ookla_country_quarter"
SILVER_NL_TILES = f"{T}.silver_ookla_nl_tiles"
SILVER_WB_COUNTRIES = f"{T}.silver_worldbank_countries"
# Serverless compute has no DataFrame cache, so intermediate results that are read several
# times are written to staging tables, dropped at the end.
STAGE_NEW_TILES = f"{T}.stage_ookla_new_tiles"
STAGE_PASS1 = f"{T}.stage_ookla_pass1"
STAGE_PER_COUNTRY = f"{T}.stage_ookla_per_country"
NL_BBOX = (3.2, 7.3, 50.7, 53.6)  # lon_min, lon_max, lat_min, lat_max (as in Notebook 33)

# Independent reference: 2026 Q3 assigned locally with shapely (GEOS point-in-polygon on the
# same Natural Earth file, same code mapping), 2026-10-11. Tests per country for the largest
# countries plus the units with special codes (XKX, CHI, FIN via Åland, FJI across 180°,
# de facto CYN and SOL), and the tests outside every country.
REFERENCE_PERIOD = "2026-07-01"
REFERENCE = {
    "fixed": {
        "tiles": 6_640_362, "unassigned_tests": 2_087_172,
        "tests": {"USA": 15419622, "BRA": 7494125, "IDN": 6431384, "GBR": 5704738,
                  "IND": 5284116, "MEX": 4692194, "PHL": 3875058, "ITA": 3362890,
                  "PAK": 3362771, "DEU": 2435893, "NLD": 1069901, "TWN": 1485530,
                  "XKX": 61540, "CHI": 14153, "FIN": 211620, "FJI": 8114, "CYN": 23097,
                  "SOL": 3207},
    },
    "mobile": {
        "tiles": 3_477_926, "unassigned_tests": 524_068,
        "tests": {"USA": 1493620, "BRA": 524859, "IDN": 2296152, "GBR": 409750,
                  "IND": 3046716, "MEX": 248747, "PHL": 599129, "ITA": 949736,
                  "PAK": 1367384, "DEU": 392188, "NLD": 56227, "TWN": 355106,
                  "XKX": 3303, "CHI": 1341, "FIN": 129273, "FJI": 4102, "CYN": 4516,
                  "SOL": 2809},
    },
}
# Both methods test the same planar polygons; they can only differ for tiles within metres of
# an H3 cell edge (0.02% of points in a probe), so allow 0.1% or 100 tests, whichever is larger.
TOLERANCE_SHARE, TOLERANCE_TESTS = 0.001, 100
# Tests outside every country: 1.99% (fixed) and 2.50% (mobile) of 2026 Q3 locally: coastal tiles
# whose centre lies seaward of the 1:10m coastline, and offshore tests. Fail above 4% in any file.
UNASSIGNED_MAX_SHARE = 0.04
# Global NLD vs week-3 municipalities (different boundaries). Mobile reached -1.986% in week 4,
# too close to a 2% limit, so mobile gets its own margin.
NL_TOLERANCE = {"fixed": 0.02, "mobile": 0.03}

timings = {}

# COMMAND ----------

# MAGIC %md ## 1 · Completed quarters, boundary version

# COMMAND ----------

import time

from pyspark.sql import functions as F

completed = (
    spark.table(INGESTION_VERSIONS)
    .where(F.col("source").isin("ookla_fixed", "ookla_mobile"))
    .select(F.regexp_replace("source", "^ookla_", "").alias("conn_type"),
            F.to_date("source_updated").alias("period_start"),
            F.col("rows").alias("expected_tiles"))
)
assert completed.count() > 0, "No completed Ookla quarters; run Notebook 31 first"
bronze = spark.table(BRONZE).join(completed.drop("expected_tiles"), ["conn_type", "period_start"])

chips = spark.table(SILVER_CHIPS)
versions = chips.select("h3_resolution", "boundaries_version").distinct().collect()
assert len(versions) == 1, f"Chips table mixes versions: {versions}"
H3_RESOLUTION, BOUNDARIES_VERSION = versions[0]
print(f"Completed files: {completed.count()} | chips: {BOUNDARIES_VERSION}, "
      f"H3 resolution {H3_RESOLUTION}")

# COMMAND ----------

# MAGIC %md ## 2 · Tile → country mapping (incremental)

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {SILVER_TILE_COUNTRY} (
        quadkey STRING NOT NULL, tile_x DOUBLE, tile_y DOUBLE, h3_cell BIGINT,
        country_code STRING, ne_adm0_a3 STRING, method STRING,
        boundaries_version STRING, h3_resolution INT, mapped_at TIMESTAMP
    ) COMMENT 'Country of each distinct Ookla tile (NULL = outside every country)'
""")
# New boundaries or resolution invalidate every assignment: start over.
stale = spark.table(SILVER_TILE_COUNTRY).where(
    (F.col("boundaries_version") != BOUNDARIES_VERSION)
    | (F.col("h3_resolution") != H3_RESOLUTION)).count()
if stale:
    print(f"{stale:,} mappings from other boundaries/resolution: re-mapping every tile")
    spark.sql(f"DELETE FROM {SILVER_TILE_COUNTRY}")

# Distinct tiles not mapped yet, with their H3 cell, staged once.
start = time.time()
(bronze.groupBy("quadkey")
 .agg(F.min("tile_x").alias("tile_x"), F.min("tile_y").alias("tile_y"),
      (F.max("tile_x") - F.min("tile_x") + F.max("tile_y") - F.min("tile_y")).alias("spread"))
 .join(spark.table(SILVER_TILE_COUNTRY).select("quadkey"), "quadkey", "left_anti")
 .withColumn("h3_cell", F.expr(f"h3_longlatash3(tile_x, tile_y, {H3_RESOLUTION})"))
 .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(STAGE_NEW_TILES))
timings["distinct new tiles"] = time.time() - start
new_tiles = spark.table(STAGE_NEW_TILES)
n_new, spread = new_tiles.agg(F.count("*"), F.max("spread")).first()
# The same tile must have the same centroid in every file.
assert n_new == 0 or spread < 1e-9, f"A quadkey has different centroids across files ({spread})"
print(f"Tiles not yet mapped: {n_new:,}")

# COMMAND ----------

chip_cols = chips.select(F.col("h3_cell").alias("chip_cell"), "core", "chip", "country_code",
                         "ne_adm0_a3")
point = "st_point(tile_x, tile_y)"
# CASE evaluates the geometry test only for border chips; a core cell decides by id.
in_chip = F.when(F.col("core"), F.lit(True)) \
           .otherwise(F.expr(f"st_contains(st_geomfromwkb(chip), {point})"))

start = time.time()
# Pass 1: the tile's own cell.
(new_tiles.join(chip_cols, F.col("h3_cell") == F.col("chip_cell"))
 .where(in_chip)
 .select("quadkey", "country_code", "ne_adm0_a3",
         F.when(F.col("core"), "core").otherwise("chip").alias("method"))
 .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(STAGE_PASS1))
timings["pass 1 (own cell)"] = time.time() - start

start = time.time()
pass1 = spark.table(STAGE_PASS1)
# Pass 2: the six neighbouring cells, only for tiles pass 1 did not place.
pass2 = (
    new_tiles.join(pass1.select("quadkey"), "quadkey", "left_anti")
    .select("quadkey", "tile_x", "tile_y", "h3_cell",
            F.explode(F.expr("h3_kring(h3_cell, 1)")).alias("ring_cell"))
    .where("ring_cell <> h3_cell")
    .join(chip_cols, F.col("ring_cell") == F.col("chip_cell"))
    .where(F.expr(f"st_contains(st_geomfromwkb(chip), {point})"))
    .select("quadkey", "country_code", "ne_adm0_a3", F.lit("neighbour_chip").alias("method"))
)
assigned = pass1.unionByName(pass2)
twice = assigned.groupBy("quadkey").count().where("count > 1").count()
assert twice == 0, f"{twice:,} tiles fall inside two countries"
mapping = (
    new_tiles.join(assigned, "quadkey", "left")
    .select("quadkey", "tile_x", "tile_y", "h3_cell", "country_code", "ne_adm0_a3", "method",
            F.lit(BOUNDARIES_VERSION).alias("boundaries_version"),
            F.lit(H3_RESOLUTION).alias("h3_resolution"))
)
mapping.createOrReplaceTempView("src_tile_country")
spark.sql(f"""
    MERGE INTO {SILVER_TILE_COUNTRY} t USING src_tile_country s ON t.quadkey = s.quadkey
    WHEN NOT MATCHED THEN INSERT (quadkey, tile_x, tile_y, h3_cell, country_code, ne_adm0_a3,
                                  method, boundaries_version, h3_resolution, mapped_at)
        VALUES (s.quadkey, s.tile_x, s.tile_y, s.h3_cell, s.country_code, s.ne_adm0_a3,
                s.method, s.boundaries_version, s.h3_resolution, current_timestamp())
""")
timings["pass 2 (neighbours) + MERGE"] = time.time() - start

check = spark.sql(f"""
    SELECT count(*) AS rows, count(DISTINCT quadkey) AS tiles,
           count_if(country_code IS NULL) AS unassigned
    FROM {SILVER_TILE_COUNTRY}
""").first()
assert check.rows == check.tiles, "A tile is mapped to more than one country"
missing = bronze.select("quadkey").distinct() \
    .join(spark.table(SILVER_TILE_COUNTRY), "quadkey", "left_anti").count()
assert missing == 0, f"{missing:,} Bronze tiles have no mapping"
print(f"{SILVER_TILE_COUNTRY}: {check.tiles:,} tiles, {check.unassigned:,} outside every country "
      f"| this run: {n_new:,} new tiles")
display(spark.table(SILVER_TILE_COUNTRY).groupBy("method").count().orderBy(F.desc("count")))

# COMMAND ----------

# MAGIC %md ## 3 · Country × quarter aggregate (additive measures)

# COMMAND ----------

start = time.time()
(
    bronze.join(spark.table(SILVER_TILE_COUNTRY).select("quadkey", "country_code"),
                "quadkey", "left")
    .groupBy("conn_type", "year", "quarter", "period_start", "country_code")
    .agg(F.count("*").alias("tiles"), F.sum("tests").alias("tests"),
         F.sum("devices").alias("devices"),
         F.sum(F.col("avg_d_kbps") * F.col("tests")).alias("sum_d_kbps_x_tests"),
         F.sum(F.col("avg_u_kbps") * F.col("tests")).alias("sum_u_kbps_x_tests"),
         F.sum(F.col("avg_lat_ms") * F.col("tests")).alias("sum_lat_ms_x_tests"))
    .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(STAGE_PER_COUNTRY)
)
timings["aggregate"] = time.time() - start
per_country = spark.table(STAGE_PER_COUNTRY)

# Row conservation: every Bronze tile counted exactly once, per file.
conservation = (
    per_country.groupBy("conn_type", "period_start")
    .agg(F.sum("tiles").alias("tiles"), F.sum("tests").alias("tests"),
         F.sum(F.when(F.col("country_code").isNull(), F.col("tests"))).alias("unassigned_tests"))
    .join(completed, ["conn_type", "period_start"], "full")
    .withColumn("unassigned_pct", F.round(100 * F.col("unassigned_tests") / F.col("tests"), 3))
    .orderBy("conn_type", "period_start")
)
display(conservation)
bad = conservation.where("tiles IS NULL OR expected_tiles IS NULL OR tiles <> expected_tiles") \
                  .collect()
assert not bad, f"Tile count differs from ingestion_versions: {bad}"
worst = conservation.agg(F.max("unassigned_pct")).first()[0]
print(f"Largest unassigned share of tests in any file: {worst}%")
assert worst / 100 <= UNASSIGNED_MAX_SHARE, \
    f"Unassigned share {worst}% above {100 * UNASSIGNED_MAX_SHARE}%"

# COMMAND ----------

KEY = ["conn_type", "year", "quarter", "country_code"]
VALUE_COLS = ["period_start", "tiles", "tests", "devices", "sum_d_kbps_x_tests",
              "sum_u_kbps_x_tests", "sum_lat_ms_x_tests"]
src = (per_country.where("country_code IS NOT NULL")
       .withColumn("boundaries_version", F.lit(BOUNDARIES_VERSION))
       .withColumn("row_hash", F.sha2(F.concat_ws("||", *[F.col(c).cast("string")
                                                         for c in VALUE_COLS]), 256)))
src.createOrReplaceTempView("src_country_quarter")
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {SILVER_COUNTRY_QUARTER} (
        conn_type STRING NOT NULL, year INT NOT NULL, quarter INT NOT NULL,
        country_code STRING NOT NULL, period_start DATE,
        tiles BIGINT, tests BIGINT, devices BIGINT,
        sum_d_kbps_x_tests BIGINT, sum_u_kbps_x_tests BIGINT, sum_lat_ms_x_tests BIGINT,
        boundaries_version STRING, row_hash STRING, updated_at TIMESTAMP
    ) COMMENT 'Ookla tests per country and quarter; additive sums, divide by tests for averages'
""")
cols = KEY + VALUE_COLS + ["boundaries_version", "row_hash"]
metrics = spark.sql(f"""
    MERGE INTO {SILVER_COUNTRY_QUARTER} t USING src_country_quarter s
    ON {" AND ".join(f"t.{k} = s.{k}" for k in KEY)}
    WHEN MATCHED AND t.row_hash <> s.row_hash THEN
        UPDATE SET {", ".join(f"t.{c} = s.{c}" for c in cols)}, t.updated_at = current_timestamp()
    WHEN NOT MATCHED THEN
        INSERT ({", ".join(cols)}, updated_at) VALUES ({", ".join(f"s.{c}" for c in cols)},
                                                       current_timestamp())
    WHEN NOT MATCHED BY SOURCE THEN DELETE
""").first().asDict()
print(f"{SILVER_COUNTRY_QUARTER}: {metrics}")
assert spark.table(SILVER_COUNTRY_QUARTER).count() == src.count(), "Silver row count mismatch"

# COMMAND ----------

# MAGIC %md ## 4 · Cross-checks

# COMMAND ----------

# (a) Independent shapely assignment of 2026 Q3.
rows = []
for conn_type, reference in REFERENCE.items():
    ref = per_country.where((F.col("conn_type") == conn_type)
                            & (F.col("period_start") == F.lit(REFERENCE_PERIOD).cast("date")))
    actual = {r.country_code: (r.tiles, r.tests) for r in ref.collect()}
    assert actual, f"Reference quarter {conn_type} {REFERENCE_PERIOD} is not loaded"
    total_tiles = sum(tiles for tiles, _ in actual.values())
    assert total_tiles == reference["tiles"], \
        f"{conn_type}: {total_tiles:,} tiles vs {reference['tiles']:,} in the local file"
    expected = reference["tests"] | {"(unassigned)": reference["unassigned_tests"]}
    for code, tests in expected.items():
        got = actual.get(None if code == "(unassigned)" else code, (0, 0))[1]
        rows.append((conn_type, code, tests, got))
comparison = (spark.createDataFrame(rows, "conn_type STRING, country_code STRING, "
                                          "shapely_tests LONG, tests LONG")
              .withColumn("diff", F.col("tests") - F.col("shapely_tests"))
              .withColumn("diff_pct", F.round(100 * F.col("diff") / F.col("shapely_tests"), 4)))
display(comparison)
off = [r for r in rows
       if abs(r[3] - r[2]) > max(TOLERANCE_TESTS, TOLERANCE_SHARE * r[2])]
assert not off, f"Differs from the shapely reference beyond tolerance: {off}"

# COMMAND ----------

# (b) The Netherlands vs week 3: Dutch tiles found by country boundary (Natural Earth, this
# notebook) and by municipality boundaries (PDOK, H3 resolution 9, Notebook 33).
lon_min, lon_max, lat_min, lat_max = NL_BBOX
nl_global = (
    bronze.join(spark.table(SILVER_TILE_COUNTRY).where("country_code = 'NLD'")
                .select("quadkey"), "quadkey")
    .where(F.col("tile_x").between(lon_min, lon_max) & F.col("tile_y").between(lat_min, lat_max))
    .groupBy("conn_type", "period_start").agg(F.sum("tests").alias("tests_country"))
)
nl_week3 = spark.table(SILVER_NL_TILES).groupBy("conn_type", "period_start") \
    .agg(F.sum("tests").alias("tests_municipalities"))
nl = (nl_global.join(nl_week3, ["conn_type", "period_start"], "full")
      .withColumn("diff_pct",
                  F.round(100 * (F.col("tests_country") / F.col("tests_municipalities") - 1), 3))
      .orderBy("conn_type", "period_start"))
display(nl)
off = [r for r in nl.collect()
       if r["diff_pct"] is None or abs(r["diff_pct"]) > 100 * NL_TOLERANCE[r["conn_type"]]]
assert not off, f"NLD differs from the municipality Silver beyond {NL_TOLERANCE}: {off}"

# COMMAND ----------

# MAGIC %md ## 5 · Performance: chips vs bounding box + `st_contains` (0.1% sample)
# MAGIC
# MAGIC Same tiles, two methods. The bounding-box method tests each tile against every country
# MAGIC part whose box contains it, using the full part geometry. Run after the steps above, so
# MAGIC both are measured warm.

# COMMAND ----------

latest = completed.where("conn_type = 'fixed'").agg(F.max("period_start")).first()[0]
sample = (bronze.where((F.col("conn_type") == "fixed") & (F.col("period_start") == latest))
          .where("pmod(xxhash64(quadkey), 1000) = 0")
          .select("quadkey", "tile_x", "tile_y"))
n_sample = sample.count()

start = time.time()
parts = (spark.table(SILVER_BOUNDARIES).where("NOT excluded")
         .select("country_code",
                 F.explode(F.expr("st_dump(st_geomfromtext(boundary_wkt))")).alias("part"))
         .select("country_code", F.expr("st_asbinary(part)").alias("wkb"),
                 F.expr("st_xmin(part)").alias("xmin"), F.expr("st_xmax(part)").alias("xmax"),
                 F.expr("st_ymin(part)").alias("ymin"), F.expr("st_ymax(part)").alias("ymax")))
by_box = (
    sample.join(F.broadcast(parts),
                F.col("tile_x").between(F.col("xmin"), F.col("xmax"))
                & F.col("tile_y").between(F.col("ymin"), F.col("ymax")))
    .where(F.expr(f"st_contains(st_geomfromwkb(wkb), {point})"))
    .select("quadkey", F.col("country_code").alias("box_country"))
).collect()
box_seconds = time.time() - start

start = time.time()
s_located = sample.withColumn("h3_cell", F.expr(f"h3_longlatash3(tile_x, tile_y, {H3_RESOLUTION})"))
by_chip = (
    s_located.join(chip_cols, F.col("h3_cell") == F.col("chip_cell"))
    .where(in_chip)
    .select("quadkey", F.col("country_code").alias("chip_country"))
).collect()
chip_seconds = time.time() - start

box_map = {r.quadkey: r.box_country for r in by_box}
chip_map = {r.quadkey: r.chip_country for r in by_chip}
same = sum(1 for q in set(box_map) | set(chip_map) if box_map.get(q) == chip_map.get(q))
print(f"Sample: {n_sample:,} tiles (fixed {latest}) | bounding box + st_contains: "
      f"{box_seconds:.1f} s | chips (pass 1): {chip_seconds:.1f} s | "
      f"same result for {same:,} of {len(set(box_map) | set(chip_map)):,} assigned tiles")
timings["sample: bounding box"] = box_seconds
timings["sample: chips"] = chip_seconds

# COMMAND ----------

display(spark.createDataFrame([(k, round(v, 1)) for k, v in timings.items()],
                              "step STRING, seconds DOUBLE"))
for table in (STAGE_NEW_TILES, STAGE_PASS1, STAGE_PER_COUNTRY):
    spark.sql(f"DROP TABLE IF EXISTS {table}")

# COMMAND ----------

# MAGIC %md ## 6 · Preview: latest quarter by country (Gold comes in week 5)

# COMMAND ----------

display(spark.sql(f"""
    SELECT q.conn_type, q.country_code, w.country_name, w.income_name, q.tiles, q.tests,
           round(q.sum_d_kbps_x_tests / q.tests / 1000, 1) AS down_mbps,
           round(q.sum_u_kbps_x_tests / q.tests / 1000, 1) AS up_mbps,
           round(q.sum_lat_ms_x_tests / q.tests, 0) AS latency_ms
    FROM {SILVER_COUNTRY_QUARTER} q
    LEFT JOIN {SILVER_WB_COUNTRIES} w ON w.country_iso3 = q.country_code
    WHERE q.period_start = (SELECT max(period_start) FROM {SILVER_COUNTRY_QUARTER})
      AND q.tests >= 1000
    ORDER BY q.conn_type, down_mbps DESC
"""))
