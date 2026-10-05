# Global & Dutch Digital Divide Lakehouse

An end-to-end data platform that measures the digital divide, from global country-level ICT indicators down to broadband quality in Dutch municipalities.

> Status: **Week 2 done: World Bank ingestion** from API to Silver. Next: Ookla with PySpark.

| Week | Scope | Status |
|---|---|---|
| 1 | Terraform foundation, Databricks Free Edition validation | ✅ |
| 2 | World Bank: API → ADLS → Bronze → Silver, incremental and idempotent | ✅ |
| 3–4 | Ookla speed tiles with PySpark | ⏳ |
| 5 | dbt Gold star schema, SCD2 snapshot | ⏳ |
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
| [Ookla Open Data](https://github.com/teamookla/ookla-open-data) | ~600 m tile × quarter | tens of millions of rows | Distributed processing with PySpark, partitioned incremental loads |
| [CBS StatLine](https://opendata.cbs.nl/) | Dutch municipality | small | Local drill-down (optional) |

## Repository layout

```
infra/terraform/   Azure resources: resource group, ADLS Gen2, Key Vault, budget alert
ingestion/         Python extractors (World Bank) and their config
tests/             pytest unit tests for the extractors
notebooks/         Databricks notebooks (source format): validation, Bronze, Silver
transform/dbt/     dbt project                              (week 5)
orchestration/     Airflow DAGs                             (week 6)
docs/              Decision records (ADRs) and weekly checklists
```

Design decisions are recorded in [ADR 0001: platform choice](docs/adr/0001-platform-choice.md) and [ADR 0002: World Bank ingestion](docs/adr/0002-worldbank-ingestion.md).

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

## Cost control

**Hard cap: under €1 for the whole project** (worst-case estimate: about €0.10).

- Heavy compute runs on Databricks Free Edition; Airflow and Docker run locally.
- The only billable Azure resources are ADLS Gen2, capped at 1 GB, and Key Vault.
- Azure budgets only alert, so the cap is enforced by the design rather than by monitoring. On top of that, Terraform adds a €1 budget alert and pins Defender for Storage to the Free tier.
- `terraform destroy` removes everything when the project is paused.

See the cost model in [docs/adr/0001-platform-choice.md](docs/adr/0001-platform-choice.md#cost-model).

## Attribution

- **World Bank, World Development Indicators**, licensed under [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/). Within WDI, the internet, broadband and mobile indicators originate from the ITU World Telecommunication/ICT Indicators Database, and secure Internet servers from Netcraft.
- **Speed test data © Ookla**, 2019–present, licensed under [CC BY-NC-SA 4.0](https://creativecommons.org/licenses/by-nc-sa/4.0/).
