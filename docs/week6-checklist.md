# Week 6: Orchestration, task image and CI checklist

> Design and decisions: [ADR 0006](adr/0006-orchestration.md).

## A. Environment
- [x] WSL2 with Ubuntu 24.04; Docker Engine inside Ubuntu (no Docker Desktop)
- [x] Airflow 3.3.2 in its own virtual environment, `airflow standalone`

## B. Databricks jobs as code
- [x] `databricks.yml`: job `ddlake_refresh` (Notebooks 10, 20, 31, 33, 35) and job `ddlake_reference` (32, 34), serverless
- [x] Storage account from `BUNDLE_VAR_storage_account`; no widget cell needed any more
- [x] Deployed and run once: 9 min 16 s, all MERGEs wrote 0 rows
- [x] Notebook 35: Dutch comparison tolerance 2% fixed, 3% mobile
- [x] Bundle sync excludes `notebooks/ruff.toml`

## C. Task image
- [x] `Dockerfile` (multi-stage, digest-pinned base, non-root), allowlist `.dockerignore`
- [x] `requirements-tasks.txt`: uv lock with hashes, Python 3.12, 84 packages
- [x] `dbt debug` passes inside the container

## D. Identity and secrets
- [x] Service principal (Terraform): landing container write + account Blob Delegator only
- [x] Client secret created outside Terraform, not in the state
- [x] `ingestion/landing_sas.py`: user delegation SAS (landing, read + list, 6 days), verified before it replaces the secret; 11 unit tests (56 in total)
- [x] `orchestration/airflow/setup_connections.sh`: connections and variables, secrets with hidden input

## E. Airflow DAG
- [x] `ddlake_refresh`: extract → rotate SAS → Databricks job → dbt build; Mondays 07:00 Amsterdam, `catchup=False`, `max_active_runs=1`
- [x] Run 1: 4/4, 11 min 12 s; run 2: 4/4, 9 min 19 s, 0 rows written (idempotent)
- [x] Failure test: wrong storage account → 3 failed attempts, downstream `upstream_failed`, Databricks and dbt not started; variable restored
- [x] Run 3 after all changes: 4/4, 10 min 54 s
- [x] DAG left paused (unpause deliberately for scheduled weeks)

## F. CI
- [x] `.github/workflows/ci.yml`: ruff + pytest, `dbt parse`, DAG integrity test, Terraform fmt + validate, gitleaks
- [x] `.github/workflows/docker-image.yml`: image build when its inputs change
- [x] ruff configured for notebooks (`notebooks/ruff.toml`), snapshots excluded: 437 → 0 findings
- [x] `tests/orchestration/test_dag_integrity.py`: 7 tests (skipped where Airflow is not installed)
- [x] Local gitleaks scan: no leaks (14 commits, 107 files)
- [ ] First CI run on GitHub is green

## G. Remote state and budget
- [x] `infra/bootstrap/create_tfstate.ps1`: state account in its own resource group
- [x] `terraform init -migrate-state`: 18 resources, plan with no changes; local state files removed
- [x] €1 budget moved to subscription scope

## Not done this week (see ADR 0006, Consequences)
- [ ] Failure notification for the DAG (e-mail or chat)
- [ ] pre-commit hooks
- [ ] Image tags per commit instead of `latest`
- [ ] Shared keys off on the lake storage account
- [ ] Local Python environment on 3.12

## H. Repo
- [ ] Commit: Databricks jobs, Notebook 35 tolerance
- [ ] Commit: task image, SAS rotation, Airflow DAG
- [ ] Commit: Terraform pipeline identity, remote state, subscription budget
- [ ] Commit: CI, lint configuration, DAG test
- [ ] Commit: ADR 0006, checklists, README
- [ ] Local gitleaks scan of the new history before pushing
- [ ] Push
