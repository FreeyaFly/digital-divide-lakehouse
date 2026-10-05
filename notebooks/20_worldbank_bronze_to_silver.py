# Databricks notebook source
# MAGIC %md
# MAGIC # 20 · World Bank: Bronze → Silver
# MAGIC
# MAGIC Builds two Silver tables from the **latest release recorded as complete** in
# MAGIC `ingestion_versions` (written by Notebook 10 after reconciliation), never from whatever
# MAGIC happens to be newest in Bronze. Bronze must hold exactly the indicators of that record.
# MAGIC
# MAGIC | Table | Key | Notes |
# MAGIC |---|---|---|
# MAGIC | `silver_worldbank_countries` | `country_iso3` | `is_aggregate` marks regions and income groups (region `NA`) |
# MAGIC | `silver_worldbank_indicator_values` | `country_iso3, indicator_code, year` | typed; null values dropped (missing = no row) |
# MAGIC
# MAGIC Each WDI release is a full snapshot of the selected indicators, so the MERGE
# MAGIC updates rows whose content changed (`row_hash`), inserts new ones and deletes rows the
# MAGIC new release no longer contains. Re-running with unchanged Bronze changes nothing.
# MAGIC Data quality checks at the end fail the notebook if anything is off.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "digital_divide_dev")
dbutils.widgets.text("start_year", "2000")
dbutils.widgets.text("end_year", "2025")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
START_YEAR = int(dbutils.widgets.get("start_year"))
END_YEAR = int(dbutils.widgets.get("end_year"))

T = f"{CATALOG}.{SCHEMA}"
BRONZE_OBSERVATIONS = f"{T}.bronze_worldbank_observations"
BRONZE_COUNTRIES = f"{T}.bronze_worldbank_countries"
SILVER_COUNTRIES = f"{T}.silver_worldbank_countries"
SILVER_VALUES = f"{T}.silver_worldbank_indicator_values"
INGESTION_VERSIONS = f"{T}.ingestion_versions"

# COMMAND ----------

from pyspark.sql import Window
from pyspark.sql import functions as F


# The release to process: the newest one Notebook 10 recorded as fully landed and reconciled.
release = (
    spark.table(INGESTION_VERSIONS)
    .where("source = 'worldbank'")
    .orderBy(F.col("source_updated").desc())
    .first()
)
assert release, f"No completed World Bank release in {INGESTION_VERSIONS}; run Notebook 10 first"
RELEASE = release["source_updated"]
RELEASE_INDICATORS = set(release["indicators"])
print(f"Processing release {RELEASE}: {len(RELEASE_INDICATORS)} indicators, "
      f"countries={release['countries']}")


def latest_rows(table: str):
    """All Bronze rows of the completed release."""
    return spark.table(table).where(F.col("_source_updated") == RELEASE), RELEASE


def dedupe(df, key: list[str], label: str):
    """Keep one row per key (the most recently loaded) and fail if anything was dropped.

    Within one source version every key should appear once; a drop here means the key
    itself is wrong (e.g. blank codes collapsing distinct entities), not real duplicates.
    """
    w = Window.partitionBy(*key).orderBy(F.col("_loaded_at").desc())
    out = df.withColumn("_rn", F.row_number().over(w)).where("_rn = 1").drop("_rn")
    removed = df.count() - out.count()
    assert removed == 0, f"{label}: de-duplication dropped {removed} rows; check the key {key}"
    return out


def row_hash(*cols: str):
    # Change detection: only a different hash triggers an UPDATE.
    return F.sha2(F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("∅")) for c in cols]), 256)


def merge(target: str, source_view: str, key: list[str], columns: list[str]) -> dict:
    on = " AND ".join(f"t.{k} = s.{k}" for k in key)
    set_clause = ", ".join(f"t.{c} = s.{c}" for c in columns)
    cols = ", ".join(columns)
    vals = ", ".join(f"s.{c}" for c in columns)
    metrics = spark.sql(f"""
        MERGE INTO {target} t
        USING {source_view} s
        ON {on}
        WHEN MATCHED AND t.row_hash <> s.row_hash THEN
            UPDATE SET {set_clause}, t.updated_at = current_timestamp()
        WHEN NOT MATCHED THEN
            INSERT ({cols}, updated_at) VALUES ({vals}, current_timestamp())
        WHEN NOT MATCHED BY SOURCE THEN DELETE
    """).first().asDict()
    print(f"{target}: {metrics}")
    return metrics

# COMMAND ----------

# MAGIC %md ## 1 · Countries

# COMMAND ----------

countries_raw, countries_version = latest_rows(BRONZE_COUNTRIES)
assert countries_raw.count() > 0, f"No country rows in Bronze for release {RELEASE}"
countries_raw = dedupe(countries_raw, ["id"], "countries")

countries = (
    countries_raw.select(
        F.col("id").alias("country_iso3"),
        F.col("iso2Code").alias("iso2"),
        F.col("name").alias("country_name"),
        F.col("region.id").alias("region_code"),
        F.col("region.value").alias("region_name"),
        F.col("incomeLevel.id").alias("income_code"),
        F.col("incomeLevel.value").alias("income_name"),
        F.col("lendingType.id").alias("lending_code"),
        F.col("lendingType.value").alias("lending_name"),
        F.expr("nullif(capitalCity, '')").alias("capital_city"),
        F.expr("try_cast(nullif(longitude, '') AS DOUBLE)").alias("longitude"),
        F.expr("try_cast(nullif(latitude, '') AS DOUBLE)").alias("latitude"),
        (F.col("region.id") == "NA").alias("is_aggregate"),
        F.to_date(F.lit(countries_version)).alias("source_updated"),
    )
)
country_cols = [c for c in countries.columns if c != "source_updated"]
countries = countries.withColumn("row_hash", row_hash(*country_cols))
countries.createOrReplaceTempView("src_countries")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {SILVER_COUNTRIES} (
        country_iso3 STRING NOT NULL, iso2 STRING, country_name STRING,
        region_code STRING, region_name STRING, income_code STRING, income_name STRING,
        lending_code STRING, lending_name STRING, capital_city STRING,
        longitude DOUBLE, latitude DOUBLE, is_aggregate BOOLEAN,
        source_updated DATE, row_hash STRING, updated_at TIMESTAMP
    ) COMMENT 'World Bank countries and aggregates, current snapshot (history: dbt snapshot in week 5)'
""")
_ = merge(SILVER_COUNTRIES, "src_countries", ["country_iso3"], countries.columns)

# COMMAND ----------

# MAGIC %md ## 2 · Indicator values

# COMMAND ----------

obs_raw, obs_version = latest_rows(BRONZE_OBSERVATIONS)
bronze_rows = obs_raw.count()

# Completeness gate, checked against the completion record rather than against Bronze itself:
# the MERGE below deletes rows missing from the source, so a partial release must never get here.
bronze_indicators = {r[0] for r in obs_raw.select("indicator.id").distinct().collect()}
missing = sorted(RELEASE_INDICATORS - bronze_indicators)
unexpected = sorted(bronze_indicators - RELEASE_INDICATORS)
assert not missing and not unexpected, (
    f"Bronze does not match the completion record for {RELEASE}: "
    f"missing {missing}, unexpected {unexpected}")

# WDI quirk: income-group aggregates (High income, Low income, ...) come with an empty
# countryiso3code; only their 2-letter country.id (e.g. XD) is set. Resolve those through
# the country table's iso2 code (XD -> HIC) BEFORE de-duplicating, otherwise all blank codes
# collapse into one "entity". The join only touches blank rows, so it cannot fan out.
iso2_to_iso3 = spark.table(SILVER_COUNTRIES).select(
    F.col("iso2").alias("_iso2"), F.col("country_iso3").alias("_iso3_from_iso2"))
blank_iso3 = F.coalesce(F.col("countryiso3code"), F.lit("")) == ""
resolved = (
    obs_raw.join(iso2_to_iso3, blank_iso3 & (F.col("country.id") == F.col("_iso2")), "left")
    .withColumn("country_iso3",
                F.when(blank_iso3, F.col("_iso3_from_iso2")).otherwise(F.col("countryiso3code")))
)
assert resolved.count() == bronze_rows, "iso2 lookup changed the row count"
iso3_from_iso2 = resolved.where(blank_iso3 & F.col("country_iso3").isNotNull()).count()
unresolved = resolved.where(F.col("country_iso3").isNull()).count()
assert unresolved == 0, f"{unresolved} observations have no ISO3 code, even via iso2"

resolved = dedupe(resolved, ["country_iso3", "indicator.id", "date"], "observations")

typed = resolved.select(
    "country_iso3",
    F.col("indicator.id").alias("indicator_code"),
    F.col("indicator.value").alias("indicator_name"),
    # Backticks: `date` and `decimal` are also SQL type keywords.
    F.expr("try_cast(`date` AS INT)").alias("year"),
    F.col("value").alias("value_raw"),
    F.expr("try_cast(`value` AS DOUBLE)").alias("value"),
    F.expr("nullif(unit, '')").alias("unit"),
    F.expr("nullif(obs_status, '')").alias("obs_status"),
    F.expr("try_cast(`decimal` AS INT)").alias("decimal_places"),
    F.to_date(F.lit(obs_version)).alias("source_updated"),
)

# Quality gates before anything is written.
bad_casts = typed.where("value_raw IS NOT NULL AND value IS NULL").count()
bad_years = typed.where("year IS NULL").count()
null_values = typed.where("value_raw IS NULL").count()
print(f"Bronze rows {bronze_rows:,} | ISO3 resolved via iso2 {iso3_from_iso2:,} | "
      f"null values dropped {null_values:,} | failed numeric casts {bad_casts:,} | "
      f"bad years {bad_years:,}")
assert bad_casts == 0, f"{bad_casts} values could not be parsed as numbers"
assert bad_years == 0, f"{bad_years} rows have a non-numeric year"

aggregates = spark.table(SILVER_COUNTRIES).select("country_iso3", "is_aggregate")
values = (
    typed.where("value IS NOT NULL")
    .drop("value_raw")
    .join(aggregates, "country_iso3", "left")
)
expected_rows = bronze_rows - null_values
assert values.count() == expected_rows, (
    f"Silver source has {values.count():,} rows, expected {expected_rows:,} (Bronze - nulls)")
value_cols = ["country_iso3", "indicator_code", "indicator_name", "year", "value",
              "unit", "obs_status", "decimal_places", "is_aggregate"]
values = values.withColumn("row_hash", row_hash(*value_cols))
values.createOrReplaceTempView("src_values")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {SILVER_VALUES} (
        country_iso3 STRING NOT NULL, indicator_code STRING NOT NULL, indicator_name STRING,
        year INT NOT NULL, value DOUBLE NOT NULL, unit STRING, obs_status STRING,
        decimal_places INT, is_aggregate BOOLEAN,
        source_updated DATE, row_hash STRING, updated_at TIMESTAMP
    ) COMMENT 'World Bank indicator observations, latest WDI release; one row per country, indicator and year'
""")
_ = merge(SILVER_VALUES, "src_values", ["country_iso3", "indicator_code", "year"], values.columns)

# COMMAND ----------

# MAGIC %md ## 3 · Data quality checks

# COMMAND ----------

checks = spark.sql(f"""
    SELECT
        count(*)                                                    AS rows,
        count(*) - count(DISTINCT country_iso3, indicator_code, year) AS duplicate_keys,
        count_if(year NOT BETWEEN {START_YEAR} AND {END_YEAR})      AS years_out_of_range,
        count_if(is_aggregate IS NULL)                              AS unknown_entities,
        count(DISTINCT indicator_code)                              AS indicators,
        count(DISTINCT CASE WHEN NOT is_aggregate THEN country_iso3 END) AS countries,
        count(DISTINCT CASE WHEN is_aggregate THEN country_iso3 END)     AS aggregates
    FROM {SILVER_VALUES}
""").first().asDict()
print(checks)

# Expected indicators come from the completion record (not from Silver or Bronze themselves);
# an indicator with no values at all in this release legitimately has no Silver rows.
indicators_with_values = {r[0] for r in typed.where("value IS NOT NULL")
                          .select("indicator_code").distinct().collect()}
silver_indicators = {r[0] for r in spark.table(SILVER_VALUES)
                     .select("indicator_code").distinct().collect()}
expected_indicators = RELEASE_INDICATORS & indicators_with_values
if RELEASE_INDICATORS - indicators_with_values:
    print(f"Note: no values at all for {sorted(RELEASE_INDICATORS - indicators_with_values)}")

assert checks["duplicate_keys"] == 0, "Duplicate (country, indicator, year) keys in Silver"
assert checks["years_out_of_range"] == 0, "Years outside the configured range"
assert silver_indicators == expected_indicators, (
    f"Silver indicators differ from the completion record: "
    f"missing {sorted(expected_indicators - silver_indicators)}, "
    f"unexpected {sorted(silver_indicators - expected_indicators)}")
assert checks["unknown_entities"] == 0, "Observations for entities missing from the country table"
print(f"All checks passed for WDI release {obs_version}")

# COMMAND ----------

# MAGIC %md ## 4 · Coverage summary (real countries only)

# COMMAND ----------

display(spark.sql(f"""
    SELECT indicator_code,
           any_value(indicator_name)                 AS indicator_name,
           count(*)                                  AS observations,
           count(DISTINCT country_iso3)              AS countries,
           min(year)                                 AS first_year,
           max(year)                                 AS last_year
    FROM {SILVER_VALUES}
    WHERE NOT is_aggregate
    GROUP BY indicator_code
    ORDER BY countries DESC
"""))
