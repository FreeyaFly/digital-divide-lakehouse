# Week 1: foundation checklist

> Budget: the whole project must stay under €1. See [ADR 0001](adr/0001-platform-choice.md#cost-model).

## A. Local tooling
- [x] Python 3.11, `python --version`
- [x] Terraform, `terraform -version` (1.16.2)
- [x] Azure CLI, `az version`, then sign in with
      `az login --tenant <your-tenant-id>`
      (find the tenant ID with `az account show --query tenantId -o tsv`)
      (choose "Microsoft account" in the Windows popup and approve in Microsoft Authenticator).
      Do **not** use `--use-device-code`: Security defaults on this tenant block device-code sign-in (`AADSTS530035`), even with MFA registered.
- [x] Databricks CLI, `databricks -v`
- [ ] Docker Desktop (needed in week 6, can wait)

## B. Azure infrastructure (Terraform)
- [x] `cp terraform.tfvars.example terraform.tfvars` and fill it in
- [x] `terraform init && terraform fmt && terraform validate`
- [x] `terraform plan -out ddlake.tfplan` shows **13 to add**: random suffix, RG, storage account, 4 containers, lifecycle policy, Key Vault, 2 role assignments, Defender pricing (Free), budget. **Nothing else.** If the plan contains any other resource type, stop and check it.
- [x] `terraform apply ddlake.tfplan` (applies exactly the reviewed plan; `*.tfplan` is git-ignored because it can contain secrets). The first apply in West Europe was rejected for new customers; the project moved to North Europe (see ADR 0001).
- [x] Budget (€1) visible in the Azure portal under Cost Management → Budgets
- [x] The next day: Cost Management → Cost analysis for the RG shows **€0.00** (2026-10-05: €0.00007 since 1 Sep, all storage and Key Vault)

## C. Databricks Free Edition: go/no-go
Sign up at databricks.com/learn/free-edition, import `notebooks/00_validate_free_edition.py`, set the `storage_account` widget, and run it.

Result (2026-10-05): **go**. Outcome recorded in [ADR 0001 → Consequences](adr/0001-platform-choice.md#consequences-validated-2026-10-05).

| Check | Result | Decision if it fails |
|---|---|---|
| uc_schema_and_volume | PASS | Blocker: fall back to local PySpark in Docker and revisit ADR 0001 |
| egress_world_bank_api | HTTP 200 | Ingest runs locally (Airflow/Docker) and uploads files |
| egress_ookla_s3 | HTTP 200, 362 MB (one fixed-broadband quarter) | Same as above |
| egress_cbs_statline | HTTP 404: reachable, but the API-root test URL does not exist; use a table endpoint in week 2 | Same as above |
| adls_https | HTTP 401 (reachable, no credentials sent) | Land files in a UC Volume instead of ADLS |
| adls_spark | FAIL as expected (`SparkKeyProviderException`) → architecture A: ADLS archive, copy to UC Volume, Spark reads Volumes | Expected to fail. Python uploads to landing, then Spark reads from the Volume |

- [x] SQL warehouse: copy the Server hostname and HTTP path, then connect from **Power BI Desktop** with a personal access token. Use the **"Databricks"** connector, not "Azure Databricks": Free Edition is hosted on Databricks' own cloud (`*.cloud.databricks.com`). Token: BI Tools scope (`sql` only), 7-day lifetime, revoked after the test. Verified against `samples.nyctaxi.trips`.
- [ ] Record the quota limits you notice (compute, warehouse size). Deferred to the first Ookla run.

## D. Repo
- [ ] Create a public GitHub repo `digital-divide-lakehouse` and push (public repos get free GitHub Actions)
- [x] Write the outcome of section C into `docs/adr/0001-platform-choice.md`

## Weekly cost check (every week until `terraform destroy`)
- [ ] Cost Management → Cost analysis, scoped to the RG: still €0.00 (or a few cents)?
- [ ] ADLS size under 1 GB (storage account → Monitoring → Metrics → "Used capacity")
