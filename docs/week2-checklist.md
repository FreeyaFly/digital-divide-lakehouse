# Week 2: World Bank ingestion checklist

> Design and decisions: [ADR 0002](adr/0002-worldbank-ingestion.md). Commands: README → *How to run (World Bank)*.

## A. Extractor (Python)
- [x] Indicator list agreed (14) in `ingestion/config/worldbank.toml`
- [x] `pytest`: 15 passed; `ruff check ingestion tests`: clean
- [x] `--dry-run` against the live API: 14 indicators planned, WDI release 2026-07-13
- [x] `--target local`: 15 files, 96,755 rows; second run `status=skipped`
- [x] Switched landing to gzipped JSONL (24.7 MB → 0.92 MB) and re-landed after deleting the uncompressed files
- [x] `--target adls`: 15 files, 96,755 rows, 0.92 MB; second run `status=skipped`
- [x] Azure SDK request logging silenced (logger `azure` at WARNING)

## B. Databricks access
- [x] Databricks CLI OAuth login in a standalone terminal (`databricks auth login --profile <profile>`)
- [x] Secret scope `ddlake` created (Free Edition supports secret scopes)
- [x] User-delegation SAS on `landing`: `sp=rl`, signed, stored as `adls-landing-sas`, expires **2026-10-11**
- [ ] Regenerate the SAS before it expires, every week until week 6 automates it

## C. Bronze (Notebook 10)

| Check | Result |
|---|---|
| ADLS listing | 15 data files, 2 manifests |
| Copy to Volume, first / second run | 15 copied / 0 copied, 15 present |
| COPY INTO observations | 96,460 inserted; second run 0 |
| COPY INTO countries | 295 inserted; second run 0 |
| Reconciliation with manifests | 15 files, 96,755 rows, OK |

## D. Silver (Notebook 20)

| Check | Result |
|---|---|
| Countries MERGE | 295 inserted; re-runs 0/0/0 |
| Values MERGE, first run | 73,161 inserted, but **1,456 rows were lost** (see ADR 0002 → Issue found) |
| Values MERGE, after the fix | 1,098 inserted, 0 updated, 0 deleted; re-run 0/0/0 |
| Bronze rows / ISO3 resolved via iso2 / nulls dropped | 96,460 / 1,820 / 22,201 |
| Silver rows | 74,259 = 96,460 − 22,201 |
| Duplicate keys, years out of range, unknown entities, failed casts | 0 |
| Indicators / countries / aggregates | 14 / 217 / 47 |

## E. Repo
- [x] Commit: World Bank ingestion with watermark and tests
- [x] Commit: Databricks Bronze and Silver notebooks
- [x] Commit: ADR 0002, checklists, README
- [x] Push all three to GitHub
