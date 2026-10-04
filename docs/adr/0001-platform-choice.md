# ADR 0001: Platform choice

- **Status:** Accepted (validated 2026-10-05)
- **Date:** 2026-09-23

## Context
The project targets the Dutch data engineering job market, where the most common stack is Azure + Databricks + PySpark + dbt, followed by Airflow, Docker and Terraform.

**Hard constraint: total project spend must stay under €1.** The budget is personal, with no cloud credits. Azure budgets send alerts but never stop spending, so the cap has to be enforced by the architecture rather than by monitoring.

## Decision
- **Compute and lakehouse:** Databricks Free Edition with Unity Catalog and Delta Lake, using a medallion architecture. Large datasets (Ookla tiles) are landed in Unity Catalog Volumes inside Free Edition and never pass through Azure.
- **Storage and infrastructure:** Azure ADLS Gen2 and Key Vault, provisioned with Terraform. The lake is capped at **1 GB**: World Bank data in full, plus the Dutch subset of the Ookla data.
- **Transformations:** PySpark for the heavy work from bronze to silver, and dbt-databricks for modelling from silver to gold.
- **Orchestration:** Airflow, run locally in Docker. In a Databricks-only organisation, Databricks Jobs would be enough; Airflow was chosen for its cross-system orchestration and its prevalence in job postings.
- **Region:** North Europe (Ireland). The first apply in West Europe failed with `RequestDisallowedByAzure: The selected region is currently not accepting new customers` for the storage account and Key Vault. North Europe is West Europe's paired region, stays inside the EU for GDPR data residency, and is priced about the same.
- **Serving:** Power BI Desktop (free). The report is published as a `.pbix` file plus screenshots, so no Pro licence is required.

## Cost model

| Item | Worst case |
|---|---|
| ADLS Gen2, ≤ 1 GB hot LRS for about 3 months | ~€0.06 |
| Storage transactions (a few thousand) | ~€0.01 |
| Key Vault (inside the free 10k operations a month) | ~€0 |
| Resource group, budget, Defender (Free tier) | €0 |
| Databricks Free Edition, local Airflow and Docker, GitHub Actions on a public repo | €0 |
| **Total** | **~€0.10** |

**Guardrails**
1. A budget alert on the resource group at €1, with emails at 20%, 50% and 100% of actual spend and at 100% of forecast spend.
2. Defender for Storage is pinned to the Free tier in Terraform.
3. Excluded services: VMs, Azure SQL, paid Azure Databricks, Container Apps, Log Analytics and diagnostic settings.
4. A weekly look at Cost Management; anything other than €0.00 is investigated.
5. `terraform destroy` whenever the project is paused or finished.

## Alternatives considered
- **Azure Databricks (paid):** rejected. The workspace's managed NAT gateway alone bills about $32 a month, which breaks the cap. It is **not** a fallback.
- **Plain Blob storage instead of ADLS Gen2:** it probably falls inside the free 5 GB, but ADLS Gen2 is the standard for Databricks lakehouses and is named in job postings. At ≤ 1 GB the difference is a few cents.
- **Azure SQL + dbt-sqlserver:** cheap and simple, but it does not exercise Spark, and Spark is the most requested gap in the target market.
- **Snowflake / BigQuery:** less common in Azure-centric Dutch enterprises.
- **Kafka streaming:** out of scope, because none of the sources are event streams.

## Fallback if Free Edition fails validation
Run PySpark locally in Docker at €0, then revisit this ADR. No paid compute is ever created without an explicit cost estimate and approval.

## Consequences (validated 2026-10-05)
- Free Edition passes the go/no-go: Unity Catalog schema/volume creation works, and serverless compute can reach the World Bank API and Ookla S3 (one fixed-broadband quarter ≈ 362 MB) directly.
- Spark cannot read ADLS (`SparkKeyProviderException`): serverless compute on Free Edition does not accept storage credentials and cannot use an external location. HTTPS to the ADLS endpoint works (401 without credentials).
- Therefore: ADLS `landing` holds the raw-file archive (≤ 1 GB, written by Python with Entra auth); Python copies files over HTTPS into a UC Volume using a short-lived, read-only user-delegation SAS stored in a Databricks secret scope, and Spark reads only from Volumes. Bulk Ookla files go straight into the Volume. In a paid workspace this bridge would be replaced by a Unity Catalog external location.
- CBS returned 404 for the API root; reachability is confirmed and the table-specific endpoint will be used in week 2.
