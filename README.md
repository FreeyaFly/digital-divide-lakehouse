# Global & Dutch Digital Divide Lakehouse

An end-to-end data platform that measures the digital divide, from global country-level ICT indicators down to broadband quality in Dutch municipalities.

> Status: **Week 5 done: dbt Gold layer**: star schema, region and income-group aggregates rebuilt from countries, and 37 years of World Bank income classifications as an SCD2 dimension; 73 dbt resources build and test cleanly. Next: orchestration with Airflow and CI.

| Week | Scope | Status |
|---|---|---|
| 1 | Terraform foundation, Databricks Free Edition validation | ✅ |
| 2 | World Bank: API → ADLS → Bronze → Silver, incremental and idempotent | ✅ |
| 3 | Ookla: 8 quarters × fixed/mobile into Bronze (80 M rows); Dutch municipalities via H3 | ✅ |
| 4 | Ookla global country-level aggregation (Natural Earth + H3 tessellation), performance notes | ✅ |
| 5 | dbt Gold star schema, SCD2 income history (FY89–FY27) + snapshot, 54 tests | ✅ |
| 6 | Airflow orchestration, GitHub Actions CI | ⏳ |
| 7 | Power BI report | ⏳ |

## Architecture

```
            Terraform (Azure resources + budget alert)
                              │
 World Bank API ─┐            ▼
 Ookla Parquet  ─┼─► Python ingest ─► Landing (ADLS Gen2 / UC Volume)
 CBS StatLine   ─┘   (Docker)                  │
                                               ▼
                     Databricks + Unity Catalog (Delta Lake)
                     Bronze ── PySpark ──► Silver ── dbt ──► Gold (star schema)
                                               │
                                               ▼
                                    Power BI Desktop (semantic model)

 Orchestration: Airflow (Docker)   CI/CD: GitHub Actions   Tests: pytest + dbt tests
```

## Data sources

| Source | Grain | Volume | Why it is here |
|---|---|---|---|
| [World Bank API](https://datahelpdesk.worldbank.org/knowledgebase/articles/889392) | country × year | ~100k rows | Incremental API ingestion; income-group reclassification drives an SCD2 snapshot |
| [Ookla Open Data](https://github.com/teamookla/ookla-open-data) | ~600 m tile × quarter | 80 M rows (8 quarters, fixed + mobile) | Distributed processing with PySpark, partitioned incremental loads |
| [PDOK Bestuurlijke Gebieden](https://api.pdok.nl/kadaster/bestuurlijkegebieden/ogc/v1) | Dutch municipality | 342 boundaries | Spatial join of speed tiles to municipalities via H3 |
| [Natural Earth Admin 0](https://www.naturalearthdata.com/downloads/10m-cultural-vectors/10m-admin-0-countries/) (1:10m, v5.1.2) | Country | 258 units | Exact global spatial join via H3 tessellation (core cells + border chips) |
| [CBS StatLine](https://opendata.cbs.nl/) | Dutch municipality | small | Local drill-down (optional) |

## Repository layout

```
infra/terraform/   Azure resources: resource group, ADLS Gen2, Key Vault, budget alert
ingestion/         Python extractors (World Bank indicators, income history) and their config
tests/             pytest unit tests (extractors, notebook helpers, tools)
notebooks/         Databricks notebooks (source format): validation, Bronze, Silver,
                   plus country_codes.py, a helper module imported by Notebook 34
tools/             Developer tools, e.g. exporting clean notebook run snapshots
transform/dbt/     dbt project: staging views, Gold star schema, SCD2, seed, snapshot
orchestration/     Airflow DAGs                             (week 6)
docs/              Decision records (ADRs) and weekly checklists
docs/notebook-runs/  Executed notebook snapshots (.ipynb with outputs)
```

Design decisions are recorded in [ADR 0001: platform choice](docs/adr/0001-platform-choice.md), [ADR 0002: World Bank ingestion](docs/adr/0002-worldbank-ingestion.md), [ADR 0003: Ookla ingestion and Dutch municipalities](docs/adr/0003-ookla-ingestion.md), [ADR 0004: global country-level aggregation](docs/adr/0004-ookla-country-aggregation.md) and [ADR 0005: dbt Gold layer](docs/adr/0005-dbt-gold.md).

## How to run (World Bank)

Prerequisites: Python 3.11+, Azure CLI signed in with a role that can write to the lake, Databricks CLI with a profile, and the Terraform outputs from `infra/terraform`.

```powershell
# 1. Install and test
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -e ".[dev]"
pytest

# 2. Land raw data: local folder first, then ADLS
python -m ingestion.worldbank --target local
$env:DDLAKE_STORAGE_ACCOUNT = "<storage-account-name>"
python -m ingestion.worldbank --target adls      # re-runs skip until WDI publishes a new release

# 3. Give Databricks a read/list SAS for the landing container (user delegation, max 7 days)
$expiry = (Get-Date).ToUniversalTime().AddDays(6).ToString("yyyy-MM-ddTHH:mmZ")
$sas = az storage container generate-sas --account-name <storage-account-name> --name landing `
       --permissions rl --expiry $expiry --auth-mode login --as-user -o tsv
databricks secrets create-scope ddlake --profile <profile>          # once
databricks secrets put-secret ddlake adls-landing-sas --string-value $sas --profile <profile>
Remove-Variable sas

# 4. Import the notebooks, then run 10 and 20 on serverless compute
databricks workspace import "/Users/<you>/ddlake/10_worldbank_land_to_bronze" `
  --file notebooks/10_worldbank_land_to_bronze.py --language PYTHON --format SOURCE --overwrite --profile <profile>
databricks workspace import "/Users/<you>/ddlake/20_worldbank_bronze_to_silver" `
  --file notebooks/20_worldbank_bronze_to_silver.py --language PYTHON --format SOURCE --overwrite --profile <profile>
```

Notebook 10 needs the `storage_account` widget set. Both notebooks are idempotent: a second run copies, inserts and merges nothing.

**See the actual output:** [docs/notebook-runs/](docs/notebook-runs/) holds executed snapshots of the notebooks, exported with `tools/export_notebook_snapshot.py` (code identical to `notebooks/`, Databricks metadata stripped, scanned for identifiers). The 2026-10-05 snapshots are a re-run with a fake partial release planted in the lake, so they show both idempotency (0 rows copied, inserted or merged) and the partial-release guard (`not covered by the watermark: 1 files`); first-load numbers are in [ADR 0002](docs/adr/0002-worldbank-ingestion.md#results-wdi-release-2026-07-13). The 2026-10-11 snapshots show the first real incremental update (WDI release 2026-10-08): 428 values revised, 233 added, 0 removed, all on regional and income-group aggregates.

## How to run (Ookla and Dutch municipalities)

No Azure access or secrets are needed: Ookla and PDOK are public. Import the notebooks as above, then run them on serverless compute in order:

| Notebook | What it does | Typical run |
|---|---|---|
| `30_ookla_discovery` | One-quarter probe: size, schema, Spark timing, spatial functions | ~1 min |
| `31_ookla_land_to_bronze` | Latest 8 quarters × fixed/mobile into `bronze_ookla_tiles`; resumable; deletes raw files after reconciling | ~7 min first run, seconds afterwards |
| `32_nl_municipalities` | PDOK boundaries → `silver_nl_municipalities` and H3 cells | ~1 min |
| `33_ookla_nl_silver` | Dutch tiles → municipalities (H3 equi-join) → `silver_ookla_nl_tiles`, with an exact-method accuracy check | ~3 min |
| `34_country_boundaries` | Natural Earth countries → `silver_country_boundaries`, reconciled with WDI, cut into H3 resolution-5 chips (`silver_country_h3_chips`) | ~5 min |
| `35_ookla_country_silver` | Every tile → country (`silver_ookla_tile_country`, incremental) → `silver_ookla_country_quarter`, checked against an independent shapely assignment | ~2 min first run |

Notebook 34 imports `notebooks/country_codes.py`, so import that file into the same workspace folder as a workspace file:

```powershell
databricks workspace import "/Users/<you>/ddlake/country_codes.py" `
  --file notebooks/country_codes.py --format AUTO --overwrite --profile <profile>
```

Snapshots of the first runs are in `docs/notebook-runs/2026-10-11_3*` (week 3) and `docs/notebook-runs/2026-10-12_3*` (week 4).

## How to run (dbt Gold)

The Gold layer is built with dbt on the Databricks SQL warehouse ([ADR 0005](docs/adr/0005-dbt-gold.md)).

```powershell
# 1. Install dbt into the project environment
.venv\Scripts\python -m pip install -e ".[dev,transform]"

# 2. Connection: profiles.yml reads three user environment variables.
#    Token: Databricks > Settings > Developer > Access tokens, scope "sql" (BI Tools).
[Environment]::SetEnvironmentVariable("DBT_DATABRICKS_HOST", "<workspace-host>", "User")
[Environment]::SetEnvironmentVariable("DBT_DATABRICKS_HTTP_PATH", "/sql/1.0/warehouses/<warehouse-id>", "User")
[Environment]::SetEnvironmentVariable("DBT_DATABRICKS_TOKEN", (Read-Host "Paste token"), "User")

# 3. Build, test and document (open a new terminal first so the variables are loaded)
cd transform/dbt
..\..\.venv\Scripts\dbt debug
..\..\.venv\Scripts\dbt build          # seed, 17 models, snapshot, 54 tests
..\..\.venv\Scripts\dbt docs generate
```

The income history seed is regenerated each July, when the World Bank publishes a new fiscal year: `python -m ingestion.worldbank.income_history` (it stops if the workbook's SHA-256 changed, so the new release is reviewed first).

![dbt lineage graph](docs/images/week5-dbt-lineage.png)

## Cost control

**Hard cap: under €1 for the whole project** (worst-case estimate: about €0.10).

- Heavy compute runs on Databricks Free Edition; Airflow and Docker run locally.
- The only billable Azure resources are ADLS Gen2, capped at 1 GB, and Key Vault.
- Azure budgets only alert, so the cap is enforced by the design rather than by monitoring. On top of that, Terraform adds a €1 budget alert and pins Defender for Storage to the Free tier.
- `terraform destroy` removes everything when the project is paused.

See the cost model in [docs/adr/0001-platform-choice.md](docs/adr/0001-platform-choice.md#cost-model).

## Attribution

- **World Bank, World Development Indicators**, licensed under [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/). Within WDI, the internet, broadband and mobile indicators originate from the ITU World Telecommunication/ICT Indicators Database, and secure Internet servers from Netcraft. The historical income classification (`OGHIST.xlsx`) is a resource of the same WDI dataset.
- **Speed test data © Ookla**, 2019–present, licensed under [CC BY-NC-SA 4.0](https://creativecommons.org/licenses/by-nc-sa/4.0/).
- **Municipality boundaries © Kadaster** (Bestuurlijke Gebieden, via PDOK), licensed under [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/).
- **Country boundaries: Made with Natural Earth.** Free vector and raster map data @ [naturalearthdata.com](https://www.naturalearthdata.com/) (public domain).
