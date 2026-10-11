# ADR 0003: Ookla ingestion and Dutch municipalities

- **Status:** Accepted (validated 2026-10-11)
- **Date:** 2026-10-11

## Context
[Ookla Open Data](https://github.com/teamookla/ookla-open-data) publishes global speed-test results per quarter as one Parquet file per connection type (fixed, mobile). Each row is a zoom-16 tile (~600 m) with test-weighted download and upload speed, latency, test count and device count. The data is licensed CC BY-NC-SA 4.0.

The files are 140–365 MB each, about 14 GB for all 31 quarters since 2019. Databricks Free Edition publishes no storage or compute quotas, so scope had to be measured rather than assumed.

## Discovery (Notebook 30, one quarter: fixed 2026 Q3)

| Measure | Result |
|---|---|
| Download into the Volume | 365 MB in 7 s |
| Rows / tests | 6.64 M tiles / 105 M tests |
| Spark count / global aggregate | 2.7 s / 2.5 s |
| Centroid columns | `tile_x`, `tile_y` present |
| Netherlands bounding box | 102,809 tiles (1.55%), 289.5 Mbps test-weighted download |
| Spatial functions | `h3_longlatash3`, `h3_polyfillash3`, `h3_tessellateaswkb`, `st_contains`: all available |
| Municipality boundaries | PDOK `gemeentegebied` reachable (code, name, province) |

One quarter is light for Spark. Multi-quarter, multi-type and global processing is what justifies it.

## Decision
- **Scope:** week 3 covers the Netherlands at municipality level. Week 4 covers a global country-level aggregation.
- **Time range:** the latest **8 quarters** (2024 Q4 – 2026 Q3), for both **fixed and mobile**: 16 files. Extending to all 31 quarters is decided after measuring storage.
- **Bronze (Notebook 31):** `bronze_ookla_tiles` is partitioned by `conn_type, year, quarter`.
  - Each file overwrites exactly its partition (`replaceWhere`), so a re-run replaces rather than duplicates.
  - Rows written must equal rows in the file.
  - A completion record per file goes into `ingestion_versions` (source `ookla_fixed` / `ookla_mobile`), so a run that stops part-way resumes where it left off.
  - Older files lacking centroid columns get them derived with `st_centroid`.
- **Not `COPY INTO`:** raw files are deleted after loading, so COPY INTO's file tracking would add nothing. Partition overwrite plus completion records give the same idempotency.
- **Raw retention:** each raw Parquet file is **deleted from the Volume once its partition is reconciled**. Ookla's public S3 bucket is immutable and is the archive (a re-download takes seconds), and keeping both copies would double storage. This differs from World Bank, whose API serves only the current release, so we archive it ourselves (ADR 0002).
- **Municipalities (Notebook 32):** the current PDOK boundaries (342 municipalities) are used for **all quarters**, so periods stay comparable despite yearly mergers. The raw GeoJSON is archived in the Volume.
- **Spatial join with H3:** each municipality is filled with H3 cells at **resolution 9** (~0.09 km² at Dutch latitudes). A cell belongs to the polygon containing its centre, so it maps to exactly one municipality. Each tile centroid is converted to its H3 cell, which turns point-in-polygon into an equi-join that Spark distributes easily.
- **Silver (Notebook 33):** `silver_ookla_nl_tiles` holds one row per tile and quarter inside a municipality.
  - Tiles are pre-filtered by a Netherlands bounding box.
  - Rows are MERGEd on `(conn_type, year, quarter, quadkey)` with a row hash.
  - Aggregation per municipality is left to the dbt Gold layer (week 5).
- **Averages are test-weighted** (Σ speed × tests / Σ tests), so a tile with thousands of tests outweighs one with a single test.

## Results (2026-10-11)

| Layer | Result |
|---|---|
| Bronze | 16 files, **79,885,721 rows**, **2.74 GB** Delta (16 data files, ~170 MB each; smaller than the ~4.3 GB of raw Parquet). Loaded in ~6.7 min, every file reconciled, raw files deleted |
| Municipalities | 342 municipalities, **465,742 H3 cells**, no cell in two municipalities |
| Coverage check | cells × 0.0896 km² ≈ **41,730 km²** vs 41,543 km² total area of the Netherlands incl. water (CBS): **+0.5%** (fails above 5%) |
| Silver | 1,171,138 tiles in the bounding box = **606,495** matched + 564,643 outside the Netherlands (BE/DE/sea) |
| Coverage | **All 342 municipalities** have data in every quarter, for both fixed and mobile |
| Idempotency | Second run: Notebook 31 skipped all 16 files, Notebook 33 MERGE 0/0/0 |

### Accuracy of the H3 approximation (fixed 2026 Q3)
The H3 assignment was compared with an exact `st_contains` point-in-polygon assignment.

| Outcome | Tiles | Share of tiles assigned by either method |
|---|---|---|
| Same municipality | 52,489 | **98.7%** |
| Different municipality | 621 | 1.2% |
| Only H3 assigns | 30 | 0.06% |
| Only exact assigns | 27 | 0.05% |

Disagreements are tiles whose centroid lies within about one H3 cell (~170 m) of a municipal border. The exact method took ~2.5 min cold for a single quarter, even with a bounding-box pre-filter (34 M candidate pairs reduced to the one or two boxes containing each tile). The H3 join covers all 16 quarters in seconds. **Production uses H3; the exact method is a periodic check.**

## Issues found
- **The area check was misleading.** The first version multiplied cells by the *global* average H3 cell area (0.105 km²) and printed 48,903 km², 17% too high, without comparing it against anything. At Dutch latitudes a resolution-9 cell is ~0.0896 km² (Groningen 0.0879, Utrecht 0.0890, Maastricht 0.0918). The check now compares with the official area and fails above 5%. Lesson: a check needs an external expected value and must stop the run, as in ADR 0002.
- **The code changed after a run.** Fixing the check after Notebook 32 had run meant re-importing and re-running it, so that the snapshot code matched the repo. Validation logic should be complete before a run is requested.

## Consequences
- **Mobile is thin in the Netherlands:** about 50–74 k mobile tests per quarter against 1.1–1.3 M fixed. Only about half the municipalities reach 100 mobile tests in a quarter, so Gold needs a minimum sample size or multi-quarter pooling for mobile comparisons.
- Municipal territory includes inland and coastal water, which distorts per-area metrics such as tests per km².
- Extending to all 31 quarters is estimated at **9–10 GB** of Bronze and ~20–25 min, and is decided in week 4.
- Re-running Notebook 32 refreshes the boundaries. After a municipal merger, Notebook 33 re-assigns every tile to the new boundaries on its next run.
- Notebook 33's MERGE deletes Silver rows whose quarter is no longer in `ingestion_versions`, so removing a completion record removes that quarter from Silver.
- Attribution: speed tests © Ookla (CC BY-NC-SA 4.0); municipality boundaries © Kadaster via PDOK (CC BY 4.0).
