# ADR 0006: Orchestration, task image, CI and remote state

- **Status:** Accepted (validated 2026-10-16)
- **Date:** 2026-10-16

## Context
Until week 5 every step ran by hand: the World Bank extractor from a terminal, the notebooks from the Databricks UI with a temporary widget cell, dbt from a terminal, and the landing SAS was regenerated manually before it expired. Week 6 turns this into one scheduled, repeatable pipeline and adds the engineering around it: a task image, a least-privilege identity, CI and remote Terraform state.

Constraints: the whole project stays under €1 (ADR 0001); the machine has 8 GB of RAM; the repository is public, so no environment identifiers or credentials may appear in code, logs or CI.

## Decision

### Shape of the pipeline
```
Airflow (local, WSL2)                         Databricks Free Edition
  extract_worldbank   ─ task image ─► ADLS landing
  rotate_landing_sas  ─ task image ─► secret scope (read+list SAS)
  run_databricks_job  ─────────────► job ddlake_refresh: Bronze + Silver notebooks
  dbt_build           ─ task image ─► SQL warehouse: Gold models, snapshot, tests
```
**Airflow only orchestrates.** Business logic lives in the task image and in the Databricks job, so each step can also run without Airflow (`docker run …`, `databricks bundle run …`).

### Databricks jobs as code (Asset Bundle)
`databricks.yml` defines two jobs on serverless compute:
- `ddlake_refresh` (weekly, triggered by Airflow): Notebooks 10 → 20 (World Bank) and 31 → 33 → 35 (Ookla). 35 runs after 33 because it compares its Dutch totals with the municipality Silver.
- `ddlake_reference` (on demand): boundaries (Notebooks 32 and 34) change rarely and take minutes to tessellate, so the weekly job only reads their tables.

The storage account name is a bundle variable set from the environment (`BUNDLE_VAR_storage_account`); the job ID is an Airflow variable. Neither is committed. This replaces the temporary widget cell.

### Airflow
- **Airflow 3.3.2, `airflow standalone` in WSL2 (Ubuntu 24.04)**, SQLite metadata database, LocalExecutor. Not the official Docker Compose setup: on 8 GB of RAM it would compete with Databricks, Docker builds and the browser. Docker Engine runs inside Ubuntu, without Docker Desktop.
- **Schedule:** Mondays 07:00 Europe/Amsterdam, `catchup=False`, `max_active_runs=1` (no two extractors writing the same release).
- **The DAG stays paused by default.** A laptop Airflow is not always running; with `catchup=False` an enabled DAG still starts the most recent missed interval as soon as Airflow starts. It is unpaused deliberately for a scheduled week.
- **Retries per step:** extractor 2 × 5 min (the World Bank API times out now and then), SAS 1 × 2 min, Databricks job 1 × 10 min (idempotent, so a rerun is safe), dbt none (a failing test should stop, not repeat). Every task has an execution timeout.

### Task image and DockerOperator
- One image, `ddlake-tasks`, holds the extractor, the SAS rotation and the dbt project. Multi-stage build on `python:3.12-slim` pinned by digest; dependencies installed from a **uv lock file with hashes** (`--require-hashes`, wheels only, one reviewed sdist exception); non-root user; allowlist `.dockerignore`. 254 MB compressed.
- Airflow runs it with a small `DockerOperator` subclass (`TaskContainer`). **Credentials are read from Airflow connections only in `execute()`**, so they are not in the DAG file, the rendered template fields or the logs, and containers are always removed (`auto_remove="force"`), also after a failure, because a stopped container keeps its environment visible to `docker inspect`.
- **dbt runs as one `dbt build` task in the image, not through Astronomer Cosmos** (this replaces the Cosmos plan noted in ADR 0005). The week-6 plan kept dbt and Airflow in separate environments: the Airflow environment holds only Airflow and its two providers, installed with the official constraints file, while dbt-databricks is pinned in the image's own lock file, so neither set has to be reconciled with the other. A resolution test on 2026-10-16 (`pip install --dry-run` in a throwaway environment) confirmed that this matters: with the official Airflow 3.3.2 constraints, dbt-databricks 1.12.7 cannot be installed at all (it needs `databricks-sql-connector` < 4.4.1 while the constraints pin 4.5.0; `packaging` 26.3 and `pydantic` 2.13.5 conflict next, and more after that). Without constraints pip finds a combination, but that is not the supported, reproducible way to install Airflow. dbt already orders models and runs tests itself, and the same image runs dbt by hand and from Airflow. The cost: Airflow shows one task instead of one per model, and a failed model cannot be retried on its own from Airflow; per-model results are in the dbt log of the task.

### Identity and secrets
- **Service principal `sp-ddlake-dev-pipeline`** (Terraform): Storage Blob Data Contributor on the **landing container only**, plus Storage Blob Delegator on the account (needed to request a user delegation key). A SAS it signs can never exceed what the principal itself may do.
- Its **client secret is created outside Terraform** (`az ad app credential reset`) and pasted with hidden input into Airflow, so it is not in the state file.
- **SAS rotation is a pipeline step:** every run signs a new user delegation SAS (landing container, read + list, HTTPS, 6 days), checks that it works, and only then replaces the Databricks secret. The SAS is never printed.
- The Airflow Databricks token has the **`jobs` and `secrets` scopes only**.

### CI (GitHub Actions)
Everything that needs **no cloud credentials** runs on every push and pull request (`.github/workflows/ci.yml`), on Python 3.12 like Databricks and the image:

| Job | Checks |
|---|---|
| Python | `ruff check`, `pytest` |
| dbt | `dbt parse` with a placeholder profile: refs, Jinja and YAML, no connection |
| Airflow | the DAG imports with Airflow 3.3.2 (official constraints) and keeps its schedule, order, retries and container settings (`tests/orchestration`) |
| Terraform | `fmt -check`, `validate` |
| Secrets | gitleaks over the whole history, binary pinned by checksum |

A second workflow builds the task image when its inputs change. Actions are pinned to commit SHAs.

**Deliberately not in CI:**
- `dbt build` would need a Databricks token in the secrets of a public repository and spend Free Edition quota; Airflow runs it weekly instead.
- `terraform plan` would print resource names, subscription and tenant IDs, object IDs and the budget e-mail into public Actions logs. A plan with OIDC that only reports the exit code and summary was considered and left for later: it also needs subscription-wide read access and Microsoft Graph permissions for the CI identity. Plans run locally against the remote state.

Notebooks are linted with a folder-level `notebooks/ruff.toml`: `spark`, `dbutils` and `display` are runtime builtins there (and nowhere else), E402 is ignored because of the cell structure, and the line limit is 120 because wrapping Markdown tables would change the rendered notebook.

### Remote Terraform state
- State lives in a storage account in its **own resource group, `rg-ddlake-tfstate`**, created once by `infra/bootstrap/create_tfstate.ps1` and **not managed by the configuration it serves**: `terraform destroy` removes `rg-ddlake-dev` with everything in it, and a state must not live in a resource its own configuration can delete.
- The account has no hierarchical namespace (so blob versioning is available), versioning and 7-day soft delete, shared keys and public access off; Terraform uses Entra ID (`use_azuread_auth`). Its names are passed with a git-ignored `backend.hcl` (partial backend configuration).
- Migration was checked by resource count (18 before and after) and a plan with **no changes**.

### Budget
The €1 budget moved from the resource group to the **subscription**. The subscription holds only this project, so a subscription budget covers everything in it, including what Terraform does not manage (the state account) and anything created elsewhere by mistake. Limits: budgets only alert, with about a day of delay; the budget is **monthly**, while the project cap of €1 is a **total**, so a small cost that stays under the first alert (€0.20) every month would never alert. The cumulative spend is therefore still checked by hand once a week, together with the lake's used capacity (1 GB costs about €0.02 a month and would not trigger any alert). The cap itself is enforced by the design (ADR 0001).

## Results (2026-10-13 to 2026-10-16)
| Run | Result |
|---|---|
| Databricks job `ddlake_refresh` alone | 5 tasks, 9 min 16 s; all six MERGEs wrote 0 rows (idempotent) |
| Airflow end-to-end, run 1 | 4/4 success, 11 min 12 s |
| Airflow end-to-end, run 2 | 4/4 success, 9 min 19 s; Bronze, Silver and snapshot wrote 0 rows |
| Failure test (storage account variable set to a name that does not exist) | extractor failed on all 3 attempts (DNS: name not resolved, container exit code 1); the 3 downstream tasks `upstream_failed`; Databricks and dbt not started; 14 min 35 s including two 5-minute retry delays |
| Airflow end-to-end after all week-6 changes, run 3 | 4/4 success, 10 min 54 s (extract 5 s, SAS 6 s, Databricks 9 min 22 s, dbt 1 min 18 s), one attempt per task |
| Local checks before the first push | ruff 0 findings (437 before configuring notebooks and excluding snapshots), pytest 56 passed, DAG test 7 passed, `dbt parse`, `terraform validate`, gitleaks: no leaks in 14 commits and 107 files |

## Consequences
- Airflow runs only while `airflow standalone` is open; the DAG is unpaused on purpose. Start it only once: two standalone instances on one SQLite database would mean two schedulers, which SQLite does not support.
- Credentials expire and must be renewed by hand: the Airflow Databricks token, the dbt token and the service principal secret (re-run `orchestration/airflow/setup_connections.sh` afterwards). The landing SAS no longer needs manual renewal.
- Every `terraform init` needs `-backend-config=backend.hcl`; a new machine needs that file recreated from `backend.hcl.example`.
- **The landing SAS is only as fresh as the last Airflow run** (6 days). Running the Databricks job by hand after that fails at Notebook 10 until the SAS is rotated, e.g. with the task image: `docker run --rm … ddlake-tasks python -m ingestion.landing_sas`.
- **The image is tagged `latest` and the DAG does not pin a version.** It must be rebuilt after changes to `ingestion/` or `transform/dbt/`; a broken rebuild would be used by the next run. Tagging images by commit is a later improvement.
- **No failure notification yet.** A failed run is visible only in the Airflow UI; the roadmap's failure alert (e-mail or chat) was not built this week.
- Airflow connections are encrypted with a Fernet key that sits in the same local Airflow home, and tasks reach Docker through its socket (root-equivalent on that machine). Acceptable for a single-user laptop setup, not for a shared server.
- Notebook snapshots were not exported this week: the notebooks run as a job now, and their logic only changed in one tolerance (Notebook 35: 2% fixed, 3% mobile). The existing snapshots match the notebook code as it was on their export date.
- Not done this week: pre-commit hooks (the secret scan runs in CI and by hand instead), disabling shared keys on the lake account, and moving the local Python environment from 3.11 to 3.12 (CI and the image use 3.12).
