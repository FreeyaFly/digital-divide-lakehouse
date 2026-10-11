# Week 3: Ookla and Dutch municipalities checklist

> Design and decisions: [ADR 0003](adr/0003-ookla-ingestion.md).

## A. Discovery (Notebook 30)
- [x] One quarter (fixed 2026 Q3) downloaded in 7 s; 6.64 M tiles; Spark count 2.7 s, aggregate 2.5 s
- [x] `tile_x` / `tile_y` present; NL bounding box 102,809 tiles (1.55%)
- [x] H3 functions and `st_contains` available on Free Edition serverless
- [x] PDOK municipality boundaries reachable
- [x] Scope decided: NL municipalities (week 3), global countries (week 4), 8 quarters, fixed + mobile, delete raw after load, current boundaries

## B. Bronze (Notebook 31)
- [x] 16 files (2024 Q4 – 2026 Q3 × fixed, mobile) loaded: 79,885,721 rows, 2.74 GB, ~6.7 min
- [x] Rows per partition reconciled with each file; completion records in `ingestion_versions`
- [x] Raw Parquet deleted from the Volume after each load
- [x] No quota limit hit
- [x] Re-run: all 16 files skipped

## C. Municipalities (Notebook 32)
- [x] 342 municipalities (MultiPolygon), raw GeoJSON archived
- [x] 465,742 H3 cells at resolution 9, none shared between municipalities
- [x] Area check: ≈ 41,730 km² vs 41,543 km² (+0.5%, tolerance 5%)

## D. Silver (Notebook 33)
- [x] 1,171,138 tiles in the NL box = 606,495 matched + 564,643 outside NL
- [x] 606,495 rows MERGEd; re-run 0 / 0 / 0
- [x] All 342 municipalities covered in every quarter, fixed and mobile
- [x] H3 vs exact `st_contains`: 98.7% same municipality; disagreements are border tiles

## E. Repo
- [x] Run snapshots `docs/notebook-runs/2026-10-11_3*` exported, scan clean
- [x] Commit: Ookla notebooks
- [x] Commit: ADR 0003, checklist, README, snapshots
- [ ] Push
