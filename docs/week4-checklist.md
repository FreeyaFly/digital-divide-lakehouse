# Week 4: Ookla global country-level aggregation checklist

> Design and decisions: [ADR 0004](adr/0004-ookla-country-aggregation.md).

## A. Preparation
- [x] Boundary source chosen: Natural Earth 1:10m Admin 0 (public domain), pinned to v5.1.2 with SHA-256
- [x] Country codes reconciled with the 217 WDI countries in both directions; corrections in `notebooks/country_codes.py` with unit tests
- [x] Independent reference: 2026 Q3 fixed and mobile assigned locally with shapely
- [x] Spatial functions, antimeridian handling and cell-edge behaviour probed on Databricks SQL
- [x] Scope: 8 quarters kept; extending to 31 quarters deferred until after week 7

## B. Boundaries and H3 chips (Notebook 34)
- [x] 258 units; Egypt repaired, area unchanged; total area matches shapely (−0.0000%)
- [x] 568,683 chips at H3 resolution 5 (85.7% core); chip area equals boundary area (−0.0000%)
- [x] Resolution comparison 4 / 5 / 6 recorded
- [x] Single-partition tessellation found and fixed with `repartition` (>9 min interrupted → 279 s)

## C. Tile → country mapping and aggregate (Notebook 35)
- [x] 18,305,611 tiles mapped: 82.9% core, 14.7% border chip, 254 via neighbouring chips, 2.4% outside every country
- [x] Tile counts per file match `ingestion_versions`
- [x] 38 of 38 country test counts identical to the shapely reference
- [x] Unassigned share 1.79–2.50% per file (limit 4%)
- [x] Netherlands within 2% of the week-3 municipality Silver in every quarter
- [x] `silver_ookla_country_quarter`: 3,757 rows with additive measures
- [x] Timings recorded (mapping under 1 min for 18.3 M tiles)

## D. Idempotency
- [x] Notebook 34 re-run: identical outputs
- [x] Notebook 35 re-run: 0 new tiles, MERGE 0 / 0 / 0

## E. Repo
- [x] Run snapshots `docs/notebook-runs/2026-10-12_3*` exported (before the idempotency run), scan clean
- [x] Commit: notebooks 34–35, `country_codes.py`, tests
- [x] Commit: ADR 0004, checklist, README, snapshots
- [x] Push
