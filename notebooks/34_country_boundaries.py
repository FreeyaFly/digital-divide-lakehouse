# Databricks notebook source
# MAGIC %md
# MAGIC # 34 · Country boundaries (Natural Earth) → Silver, tessellated with H3
# MAGIC
# MAGIC 1. Download Natural Earth 1:10m Admin 0 countries (v5.1.2, public domain), verify its
# MAGIC    SHA-256 and archive the raw GeoJSON in the Volume.
# MAGIC 2. `silver_country_boundaries`: one row per Natural Earth unit with a WDI-compatible
# MAGIC    country code (`country_codes.py`), repaired where the geometry is invalid.
# MAGIC 3. Reconcile the codes with the 217 WDI countries in both directions.
# MAGIC 4. `silver_country_h3_chips`: each country cut along H3 resolution-5 cells (~250 km²).
# MAGIC    A **core** cell lies entirely inside one country; a **border** cell carries the "chip"
# MAGIC    where it overlaps the country. Notebook 35 assigns a tile in a core cell by cell id
# MAGIC    alone and tests only tiles in border cells against their small chip, so the result is
# MAGIC    exact but the expensive geometry work stays near borders and coasts.
# MAGIC
# MAGIC Re-running replaces both tables. Antarctica is left out (see `country_codes.EXCLUDED`).

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "digital_divide_dev")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
T = f"{CATALOG}.{SCHEMA}"
SILVER_BOUNDARIES = f"{T}.silver_country_boundaries"
SILVER_CHIPS = f"{T}.silver_country_h3_chips"
SILVER_WB_COUNTRIES = f"{T}.silver_worldbank_countries"

NE_VERSION = "v5.1.2"
NE_URL = (f"https://raw.githubusercontent.com/nvkelso/natural-earth-vector/{NE_VERSION}/"
          "geojson/ne_10m_admin_0_countries.geojson")
NE_SHA256 = "239eec57ac17f100a11e2536cffc56752c318b50ae765b0918ff7aab4ce8f255"
NE_FEATURES = 258
BOUNDARIES_VERSION = f"naturalearth-10m-{NE_VERSION}"
H3_RESOLUTION = 5

# Expected values computed independently with shapely (GEOS) on the same file, 2026-10-11:
# total planar area of all units except Antarctica, in square degrees, after repairing the one
# invalid geometry (Egypt: ring self-intersection at 35.62 E, 23.14 N).
EXPECTED_INVALID = {"EGY"}
EXPECTED_AREA_DEG2 = 15_378.276

# COMMAND ----------

# MAGIC %md ## 1 · Download, verify and archive

# COMMAND ----------

import hashlib
import json
import os
import sys
import time

import requests
from pyspark.sql import functions as F

# country_codes.py sits next to this notebook as a workspace file. Serverless does not guarantee
# the working directory, so put the notebook's folder on the path explicitly.
try:
    notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext() \
        .notebookPath().get()
    sys.path.insert(0, "/Workspace" + os.path.dirname(notebook_path))
except Exception:  # context API unavailable: fall back to the working directory
    sys.path.insert(0, os.getcwd())
try:
    from country_codes import EXCLUDED, EXPECTED_NOT_IN_WDI, resolve
except ImportError as e:
    raise ImportError("country_codes.py must be imported into the same workspace folder as "
                      "this notebook (databricks workspace import --format AUTO)") from e

response = requests.get(NE_URL, timeout=300)
response.raise_for_status()
digest = hashlib.sha256(response.content).hexdigest()
assert digest == NE_SHA256, f"Natural Earth file changed: sha256 {digest}"

archive = (f"/Volumes/{CATALOG}/{SCHEMA}/landing/naturalearth/{NE_VERSION}/"
           "ne_10m_admin_0_countries.geojson")
os.makedirs(os.path.dirname(archive), exist_ok=True)
with open(archive, "wb") as f:
    f.write(response.content)

features = json.loads(response.content)["features"]
print(f"Natural Earth {NE_VERSION}: {len(features)} units, {len(response.content) / 1e6:.1f} MB, "
      f"sha256 verified; archived to {archive}")
assert len(features) == NE_FEATURES, f"Expected {NE_FEATURES} units, got {len(features)}"

# COMMAND ----------

# MAGIC %md ## 2 · Boundaries with country codes, repaired where invalid

# COMMAND ----------

codes = resolve(features)  # raises on duplicate units or colliding codes
rows = [(r["ne_adm0_a3"], r["country_code"], r["code_source"], r["name"], r["excluded"],
         json.dumps(feat["geometry"]))
        for r, feat in zip(codes, features, strict=True)]
raw = (
    spark.createDataFrame(rows, "ne_adm0_a3 STRING, country_code STRING, code_source STRING, "
                                "name STRING, excluded BOOLEAN, geojson STRING")
    .withColumn("geom", F.expr("st_geomfromgeojson(geojson)"))
    .withColumn("was_valid", F.expr("st_isvalid(geom)"))
    # A zero-width buffer rebuilds a self-intersecting ring as valid polygons.
    .withColumn("geom", F.expr("CASE WHEN was_valid THEN geom ELSE st_buffer(geom, 0.0) END"))
    .withColumn("is_valid", F.expr("st_isvalid(geom)"))
    .withColumn("area_deg2", F.expr("st_area(geom)"))
    .withColumn("raw_area_deg2", F.expr("st_area(st_geomfromgeojson(geojson))"))
)
repaired = raw.where("NOT was_valid").select("ne_adm0_a3", "raw_area_deg2", "area_deg2",
                                             "is_valid").collect()
for r in repaired:
    print(f"Repaired {r.ne_adm0_a3}: area {r.raw_area_deg2:.6f} → {r.area_deg2:.6f} deg², "
          f"valid {r.is_valid}")
assert {r.ne_adm0_a3 for r in repaired} == EXPECTED_INVALID, \
    f"Invalid geometries {sorted(r.ne_adm0_a3 for r in repaired)}, expected {EXPECTED_INVALID}"
assert all(r.is_valid and abs(r.area_deg2 / r.raw_area_deg2 - 1) < 1e-4 for r in repaired), \
    "Repair left an invalid geometry or changed its area by more than 0.01%"

boundaries = raw.select(
    "ne_adm0_a3", "country_code", "code_source", "name", "excluded",
    F.expr("st_astext(geom)").alias("boundary_wkt"),
    F.expr("st_numgeometries(geom)").alias("parts"),
    "area_deg2",
    F.col("was_valid").alias("source_geometry_valid"),
    F.lit(BOUNDARIES_VERSION).alias("boundaries_version"),
)
boundaries.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(SILVER_BOUNDARIES)
spark.sql(f"COMMENT ON TABLE {SILVER_BOUNDARIES} IS 'Natural Earth 1:10m Admin 0 units "
          f"({NE_VERSION}) with WDI-compatible country codes, WGS84 WKT'")

area = spark.table(SILVER_BOUNDARIES).where("NOT excluded").agg(F.sum("area_deg2")).first()[0]
print(f"{SILVER_BOUNDARIES}: {spark.table(SILVER_BOUNDARIES).count()} units | "
      f"area excl. {sorted(EXCLUDED)} {area:,.3f} deg² vs {EXPECTED_AREA_DEG2:,.3f} from shapely "
      f"({100 * (area / EXPECTED_AREA_DEG2 - 1):+.4f}%)")
assert abs(area / EXPECTED_AREA_DEG2 - 1) < 1e-4, "Boundary area differs from the shapely value"

# COMMAND ----------

# MAGIC %md ## 3 · Reconcile with the WDI countries

# COMMAND ----------

wdi = {r[0] for r in spark.table(SILVER_WB_COUNTRIES).where("NOT is_aggregate")
       .select("country_iso3").collect()}
ne = {r[0] for r in spark.table(SILVER_BOUNDARIES).where("NOT excluded")
      .select("country_code").distinct().collect()}
print(f"WDI countries: {len(wdi)} | boundary codes: {len(ne)} | "
      f"boundary codes not in WDI: {len(ne - wdi)} | "
      f"WDI countries without boundary: {len(wdi - ne)}")
print("Not in WDI:", sorted(ne - wdi))
assert not wdi - ne, f"WDI countries without a boundary: {sorted(wdi - ne)}"
assert ne - wdi == EXPECTED_NOT_IN_WDI, (
    f"Unexpected codes not in WDI: {sorted((ne - wdi) - EXPECTED_NOT_IN_WDI)}; "
    f"expected but now in WDI: {sorted(EXPECTED_NOT_IN_WDI - (ne - wdi))}")

# COMMAND ----------

# MAGIC %md ## 4 · H3 tessellation

# COMMAND ----------

# Tessellate each polygon part separately: parts are small enough to stay well inside one
# hemisphere (largest: Russia's mainland, 153° of longitude), and failures are easy to locate.
# inline_outer keeps a part whose tessellation failed (NULL) as one row with a NULL cell, so
# the table is written in one pass and failures are checked afterwards.
# Spark sizes partitions by input bytes: the 258-row boundary table is a single partition, so
# without repartitioning every part is tessellated by one task (first run: >9 min, interrupted).
# Spreading the ~4,100 parts over 32 partitions lets them run in parallel; the largest single
# part (Russia's mainland) still sets the lower bound.
start = time.time()
(
    spark.table(SILVER_BOUNDARIES).where("NOT excluded")
    .select("country_code", "ne_adm0_a3",
            F.posexplode(F.expr("st_dump(st_geomfromtext(boundary_wkt))")).alias("part_id", "part"))
    .repartition(32)
    .select("country_code", "ne_adm0_a3", "part_id",
            F.inline_outer(F.expr(f"h3_try_tessellateaswkb(st_asbinary(part), {H3_RESOLUTION})")))
    .select("country_code", "ne_adm0_a3", "part_id",
            F.col("cellid").alias("h3_cell"), "core", "chip",
            F.lit(H3_RESOLUTION).alias("h3_resolution"),
            F.lit(BOUNDARIES_VERSION).alias("boundaries_version"))
    .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(SILVER_CHIPS)
)
seconds = time.time() - start
failed = spark.table(SILVER_CHIPS).where("h3_cell IS NULL").select("ne_adm0_a3", "part_id") \
    .collect()
assert not failed, f"Tessellation failed for {len(failed)} parts: {failed[:20]}"
spark.sql(f"COMMENT ON TABLE {SILVER_CHIPS} IS 'Country parts cut along H3 resolution-"
          f"{H3_RESOLUTION} cells: core cells lie inside one country, "
          f"border cells carry a WKB chip'")

stats = spark.sql(f"""
    SELECT count(*) AS chips, count_if(core) AS core_chips, count_if(NOT core) AS border_chips,
           count(DISTINCT CASE WHEN core THEN h3_cell END) AS distinct_core_cells,
           count(DISTINCT country_code) AS countries,
           sum(st_area(st_geomfromwkb(chip))) AS chip_area_deg2,
           round(sum(length(chip)) / 1e6, 1) AS chip_mb
    FROM {SILVER_CHIPS}
""").first().asDict()
print(f"Tessellation at resolution {H3_RESOLUTION} in {seconds:.0f} s: {stats}")
assert stats["core_chips"] == stats["distinct_core_cells"], \
    "A core cell belongs to more than one country part"
assert stats["countries"] == len(ne), "Some country got no H3 cells"
# The chips must tile the boundaries exactly: same total area as the polygons.
print(f"Chip area {stats['chip_area_deg2']:,.3f} deg² vs boundaries {area:,.3f} deg² "
      f"({100 * (stats['chip_area_deg2'] / area - 1):+.4f}%)")
assert abs(stats["chip_area_deg2"] / area - 1) < 1e-3, "Chips do not cover the boundaries exactly"

# COMMAND ----------

# MAGIC %md ## 5 · Resolution comparison (record only, for ADR 0004)
# MAGIC
# MAGIC Coarser cells mean fewer chips but larger, more complex border chips and more tiles that
# MAGIC need a geometry test; finer cells the opposite. Measured on five contrasting countries.

# COMMAND ----------

sample_countries = ["NLD", "NOR", "IDN", "BRA", "CHE"]  # small, fjords, islands, large, inland
comparison = []
for resolution in (4, 5, 6):
    start = time.time()
    r = (spark.table(SILVER_BOUNDARIES).where(F.col("country_code").isin(sample_countries))
         .select(F.explode(F.expr("st_dump(st_geomfromtext(boundary_wkt))")).alias("part"))
         .repartition(16)
         .select(F.inline(F.expr(f"h3_tessellateaswkb(st_asbinary(part), {resolution})")))
         .agg(F.count("*").alias("chips"), F.expr("count_if(NOT core)").alias("border_chips"),
              F.avg(F.when(~F.col("core"), F.length("chip"))).alias("avg_border_chip_bytes"))
         .first())
    comparison.append((resolution, r.chips, r.border_chips,
                       round(100 * r.border_chips / r.chips, 1), int(r.avg_border_chip_bytes),
                       round(time.time() - start, 1)))
display(spark.createDataFrame(comparison, "resolution INT, chips LONG, border_chips LONG, "
                                          "border_pct DOUBLE, avg_border_chip_bytes INT, "
                                          "seconds DOUBLE"))

# COMMAND ----------

display(spark.sql(f"""
    SELECT b.country_code, b.name, b.parts, count(*) AS chips, count_if(c.core) AS core_chips
    FROM {SILVER_BOUNDARIES} b JOIN {SILVER_CHIPS} c USING (ne_adm0_a3)
    GROUP BY ALL ORDER BY chips DESC LIMIT 15
"""))
