"""Weekly refresh of the Digital Divide Lakehouse: land, rotate the SAS, Databricks, dbt.

1. `extract_worldbank`: the World Bank extractor lands a new release in ADLS (no-op if unchanged).
2. `rotate_landing_sas`: a fresh read-only SAS for the landing container, stored in Databricks.
3. `run_databricks_job`: the bundle job `ddlake_refresh` (Bronze and Silver notebooks).
4. `dbt_build`: Gold models, snapshot and tests.

Steps 1, 2 and 4 run in the task image (`ddlake-tasks`, see the Dockerfile); step 3 runs on
Databricks. Every step is idempotent, so a week without new data changes nothing.

Airflow setup (no secrets in this file):
- connection `databricks_default`: host + token (runs the job, writes the secret)
- connection `azure_pipeline`: login = client ID, password = client secret,
  extra {"tenant_id": ...} (the pipeline service principal from Terraform)
- connection `dbt_databricks`: host + token, extra {"http_path": ...}
- variables `ddlake_storage_account`, `ddlake_refresh_job_id`

Design: docs/adr/0006-orchestration.md.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import timedelta

import pendulum
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.sdk import DAG, BaseHook

IMAGE = "ddlake-tasks:latest"
STORAGE_ACCOUNT = {"DDLAKE_STORAGE_ACCOUNT": "{{ var.value.ddlake_storage_account }}"}


def _https(host: str) -> str:
    return host if host.startswith("https://") else f"https://{host}"


def azure_env() -> dict[str, str]:
    conn = BaseHook.get_connection("azure_pipeline")
    return {
        "AZURE_TENANT_ID": conn.extra_dejson["tenant_id"],
        "AZURE_CLIENT_ID": conn.login,
        "AZURE_CLIENT_SECRET": conn.password,
    }


def databricks_env() -> dict[str, str]:
    conn = BaseHook.get_connection("databricks_default")
    return {"DATABRICKS_HOST": _https(conn.host), "DATABRICKS_TOKEN": conn.password}


def dbt_env() -> dict[str, str]:
    conn = BaseHook.get_connection("dbt_databricks")
    return {
        "DBT_DATABRICKS_HOST": conn.host.removeprefix("https://"),
        "DBT_DATABRICKS_HTTP_PATH": conn.extra_dejson["http_path"],
        "DBT_DATABRICKS_TOKEN": conn.password,
    }


class TaskContainer(DockerOperator):
    """Runs one command in the task image.

    Credentials are read from Airflow connections only when the task runs and are added to the
    container environment then, so they are not in this file, in the rendered template fields
    or in the logs. Containers are always removed, also after a failure, because a stopped
    container keeps its environment (and so the credentials) visible to `docker inspect`.
    """

    def __init__(self, *, secrets: Callable[[], dict[str, str]] | None = None, **kwargs) -> None:
        super().__init__(
            image=IMAGE,
            docker_url="unix://var/run/docker.sock",
            network_mode="bridge",
            mount_tmp_dir=False,
            auto_remove="force",
            **kwargs,
        )
        self.secrets = secrets

    def execute(self, context):
        if self.secrets is not None:
            self.environment = {**self.environment, **self.secrets()}
        return super().execute(context)


with DAG(
    dag_id="ddlake_refresh",
    description="World Bank + Ookla: land, Databricks Bronze/Silver, dbt Gold",
    schedule="0 7 * * 1",  # Mondays 07:00 in the start date's time zone
    start_date=pendulum.datetime(2026, 10, 12, tz="Europe/Amsterdam"),
    catchup=False,  # sources change slowly; a missed week is caught up by the next run
    max_active_runs=1,  # one refresh at a time: no two extractors writing the same release
    default_args={"owner": "ddlake", "retries": 0},
    doc_md=__doc__,
    tags=["ddlake"],
) as dag:
    extract_worldbank = TaskContainer(
        task_id="extract_worldbank",
        command="python -m ingestion.worldbank --target adls",
        environment=STORAGE_ACCOUNT,
        secrets=azure_env,
        retries=2,  # the World Bank API times out now and then
        retry_delay=timedelta(minutes=5),
        execution_timeout=timedelta(minutes=30),
    )

    rotate_landing_sas = TaskContainer(
        task_id="rotate_landing_sas",
        command="python -m ingestion.landing_sas",
        environment=STORAGE_ACCOUNT,
        secrets=lambda: {**azure_env(), **databricks_env()},
        retries=1,
        retry_delay=timedelta(minutes=2),
        execution_timeout=timedelta(minutes=10),
    )

    run_databricks_job = DatabricksRunNowOperator(
        task_id="run_databricks_job",
        job_id="{{ var.value.ddlake_refresh_job_id }}",
        databricks_conn_id="databricks_default",
        retries=1,  # the job is idempotent, so a rerun is safe
        retry_delay=timedelta(minutes=10),
        execution_timeout=timedelta(hours=2),
    )

    dbt_build = TaskContainer(
        task_id="dbt_build",
        command="dbt build",
        secrets=dbt_env,
        execution_timeout=timedelta(hours=1),
    )

    extract_worldbank >> rotate_landing_sas >> run_databricks_job >> dbt_build
