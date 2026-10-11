# Databricks notebook source
# MAGIC %md
# MAGIC # 32 · Dutch municipality boundaries (PDOK) → Silver
# MAGIC
# MAGIC 1. Download the current municipality boundaries (`gemeentegebied`, Kadaster via PDOK OGC API,
# MAGIC    WGS84) and archive the raw GeoJSON in the Volume.
# MAGIC 2. `silver_nl_municipalities`: one row per municipality with its boundary as WKT.
# MAGIC 3. `silver_nl_municipality_h3`: each municipality filled with H3 cells at resolution 9
# MAGIC    (~0.1 km²). A cell belongs to the polygon containing its centre, so it maps to exactly one
# MAGIC    municipality; Notebook 33 then joins Ookla tiles to municipalities by cell id.
# MAGIC
# MAGIC All quarters use the current boundaries, so periods stay comparable (ADR 0003).
# MAGIC Re-running replaces both tables with the latest boundaries.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "digital_divide_dev")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
T = f"{CATALOG}.{SCHEMA}"
SILVER_MUNICIPALITIES = f"{T}.silver_nl_municipalities"
SILVER_MUNICIPALITY_H3 = f"{T}.silver_nl_municipality_h3"
H3_RESOLUTION = 9
API = ("https://api.pdok.nl/kadaster/bestuurlijkegebieden/ogc/v1/"
       "collections/gemeentegebied/items?f=json&limit=1000")

# COMMAND ----------

# MAGIC %md ## 1 · Download and archive the boundaries

# COMMAND ----------

import json
import os
from datetime import date

import requests
from pyspark.sql import functions as F

features, next_url = [], API
while next_url:
    response = requests.get(next_url, timeout=120)
    response.raise_for_status()
    page = response.json()
    features += page["features"]
    next_url = next((link["href"] for link in page.get("links", []) if link.get("rel") == "next"),
                    None)

RETRIEVED = date.today().isoformat()
archive = (f"/Volumes/{CATALOG}/{SCHEMA}/landing/pdok/gemeentegebied/"
           f"retrieved={RETRIEVED}/gemeentegebied.geojson")
os.makedirs(os.path.dirname(archive), exist_ok=True)
with open(archive, "w", encoding="utf-8") as f:
    json.dump({"type": "FeatureCollection", "features": features}, f)

types = {feat["geometry"]["type"] for feat in features}
print(f"PDOK: {len(features)} municipalities, geometry types {types}; archived to {archive}")
assert features, "PDOK returned no municipalities"

# COMMAND ----------

# MAGIC %md ## 2 · Municipalities

# COMMAND ----------

rows = [
    (p["code"], p["identificatie"], p["naam"], p["ligt_in_provincie_code"],
     p["ligt_in_provincie_naam"], json.dumps(feat["geometry"]))
    for feat in features
    for p in [feat["properties"]]
]
municipalities = (
    spark.createDataFrame(rows, "gemeente_code STRING, gemeente_id STRING, gemeente_naam STRING, "
                                "provincie_code STRING, provincie_naam STRING, geojson STRING")
    .withColumn("boundary_wkt", F.expr("st_astext(st_geomfromgeojson(geojson))"))
    .withColumn("boundaries_retrieved", F.to_date(F.lit(RETRIEVED)))
    .drop("geojson")
)
assert municipalities.select("gemeente_code").distinct().count() == len(features), \
    "Duplicate municipality codes from PDOK"

municipalities.write.mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable(SILVER_MUNICIPALITIES)
spark.sql(f"COMMENT ON TABLE {SILVER_MUNICIPALITIES} IS "
          f"'Current Dutch municipalities (PDOK gemeentegebied, WGS84 WKT)'")
print(f"{SILVER_MUNICIPALITIES}: {spark.table(SILVER_MUNICIPALITIES).count()} rows")

# COMMAND ----------

# MAGIC %md ## 3 · H3 cells per municipality

# COMMAND ----------

cells = (
    spark.table(SILVER_MUNICIPALITIES)
    .select("gemeente_code",
            F.explode(F.expr(f"h3_polyfillash3(boundary_wkt, {H3_RESOLUTION})")).alias("h3_r9"))
)
cells.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(SILVER_MUNICIPALITY_H3)
spark.sql(f"COMMENT ON TABLE {SILVER_MUNICIPALITY_H3} IS "
          f"'H3 resolution-{H3_RESOLUTION} cells per municipality (cell centre inside the boundary)'")

stats = spark.sql(f"""
    SELECT count(*) AS cells, count(DISTINCT h3_r9) AS distinct_cells,
           count(DISTINCT gemeente_code) AS municipalities_with_cells
    FROM {SILVER_MUNICIPALITY_H3}
""").first().asDict()
print(stats)
assert stats["cells"] == stats["distinct_cells"], "An H3 cell maps to more than one municipality"
assert stats["municipalities_with_cells"] == len(features), "Some municipality got no H3 cells"
# Area check. H3 cell size varies with latitude: the global average at resolution 9 is 0.105 km²,
# but at Dutch latitudes it is ~0.0896 km² (Groningen 0.0879, Utrecht 0.0890, Maastricht 0.0918).
# Municipal territory includes inland and coastal water, so compare with the total area of the
# Netherlands including water, ~41,500 km² (CBS).
NL_CELL_KM2, NL_TOTAL_KM2 = 0.0896, 41_543
covered = stats["cells"] * NL_CELL_KM2
print(f"Covered area ≈ {covered:,.0f} km² vs {NL_TOTAL_KM2:,} km² total "
      f"({100 * (covered / NL_TOTAL_KM2 - 1):+.1f}%)")
assert abs(covered / NL_TOTAL_KM2 - 1) < 0.05, "H3 coverage deviates more than 5% from NL area"

# COMMAND ----------

display(spark.sql(f"""
    SELECT m.provincie_naam, count(DISTINCT m.gemeente_code) AS municipalities,
           count(c.h3_r9) AS h3_cells
    FROM {SILVER_MUNICIPALITIES} m JOIN {SILVER_MUNICIPALITY_H3} c USING (gemeente_code)
    GROUP BY ALL ORDER BY municipalities DESC
"""))
