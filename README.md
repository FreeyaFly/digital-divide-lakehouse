# Global & Dutch Digital Divide Lakehouse

An end-to-end data platform that measures the digital divide, from global country-level ICT indicators down to broadband quality in Dutch municipalities.

> Status: **Week 1: foundation** (infrastructure as code + platform validation)

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
notebooks/         Databricks notebooks (source format)
ingestion/         Python extractors                        (week 2+)
transform/dbt/     dbt project                              (week 5)
orchestration/     Airflow DAGs                             (week 6)
docs/              Decision records and runbooks
```

## Cost control

**Hard cap: under €1 for the whole project** (worst-case estimate: about €0.10).

- Heavy compute runs on Databricks Free Edition; Airflow and Docker run locally.
- The only billable Azure resources are ADLS Gen2, capped at 1 GB, and Key Vault.
- Azure budgets only alert, so the cap is enforced by the design rather than by monitoring. On top of that, Terraform adds a €1 budget alert and pins Defender for Storage to the Free tier.
- `terraform destroy` removes everything when the project is paused.

See the cost model in [docs/adr/0001-platform-choice.md](docs/adr/0001-platform-choice.md#cost-model).

## Attribution

Speed test data © Ookla, 2019–present, licensed under [CC BY-NC-SA 4.0](https://creativecommons.org/licenses/by-nc-sa/4.0/). World Bank data is licensed under CC BY 4.0.
