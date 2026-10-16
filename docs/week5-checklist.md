# Week 5: dbt Gold layer checklist

> Design and decisions: [ADR 0005](adr/0005-dbt-gold.md).

## A. Preparation
- [x] Silver schemas inspected; dbt-core 1.12.5 + dbt-databricks 1.12.7 chosen
- [x] World Bank historical income classification found (OGHIST, FY89–FY27, WDI dataset, CC BY 4.0); FY27 = today's API income group for 217/217 economies
- [x] Measured: Caribbean Netherlands 0.2% of Dutch tests; units outside WDI 1.5–1.8% of tests; mobile municipal samples 51% ≥ 100 tests a quarter
- [x] Measured: own world internet share vs WDI within −1.1 to +2.1 points where coverage ≥ 94%; +7.5 points in 2025 (coverage 37%)

## B. Income history seed
- [x] `ingestion/worldbank/income_history.py`: download, pinned SHA-256, long CSV seed (8,095 rows, 224 economies)
- [x] 8 unit tests (45 in total)

## C. Connection
- [x] dbt installed in `.venv` (`pip install -e ".[dev,transform]"`)
- [x] Personal access token: `sql` scope, 30 days, auto-scope off; stored only in user environment variables
- [x] `profiles.yml` committed with `env_var()` only; `dbt debug` passes

## D. Models
- [x] Minimal slice (`dim_country`, `fct_speed_country_quarter`, seed): PASS=24
- [x] Staging views (6), dimensions (5, incl. SCD2), facts (3), aggregates (2), mart (1), snapshot
- [x] 54 tests incl. 6 singular tests with external expected values
- [x] Read-only dry run of every model and test (ephemeral compile) before the full build: 0 failures

## E. Runs
- [x] Full build: PASS=73 (96 s)
- [x] Second build: PASS=73 (64 s); snapshot unchanged, 217 current rows
- [x] `dbt docs generate`; lineage graph saved to `docs/images/week5-dbt-lineage.png`

## F. Repo
- [x] Commit: income history script, seed, dbt project, tests (`3e432db`)
- [x] Commit: ADR 0005, checklist, README, lineage image; tick week-4 checklist (`3d4c4f0`)
- [x] Push
