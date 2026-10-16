"""The Airflow DAG imports cleanly and keeps the settings the design relies on (ADR 0006).

Needs Apache Airflow with the Databricks and Docker providers, so it runs in CI and in the
WSL Airflow environment; elsewhere it is skipped. Parsing needs no metadata database and
reads no connections: credentials are only looked up when a task runs.
"""

from __future__ import annotations

from datetime import timedelta
from pathlib import Path

import pytest

pytest.importorskip("airflow.providers.docker")
pytest.importorskip("airflow.providers.databricks")

from airflow.dag_processing.dagbag import DagBag  # noqa: E402

DAG_FOLDER = Path(__file__).resolve().parents[2] / "orchestration" / "airflow" / "dags"
SECRET_HINTS = ("SECRET", "TOKEN", "PASSWORD", "KEY")


@pytest.fixture(scope="module")
def dag():
    bag = DagBag(dag_folder=str(DAG_FOLDER))
    assert bag.import_errors == {}
    assert list(bag.dags) == ["ddlake_refresh"]
    return bag.dags["ddlake_refresh"]


def test_schedule(dag):
    assert dag.timetable.expression == "0 7 * * 1"
    assert str(dag.timezone) == "Europe/Amsterdam"
    assert dag.catchup is False
    assert dag.max_active_runs == 1


def test_tasks_run_in_order(dag):
    order = ["extract_worldbank", "rotate_landing_sas", "run_databricks_job", "dbt_build"]
    assert sorted(dag.task_ids) == sorted(order)
    for upstream, downstream in zip(order, order[1:], strict=False):
        assert dag.get_task(upstream).downstream_task_ids == {downstream}
    assert dag.get_task(order[-1]).downstream_task_ids == set()


@pytest.mark.parametrize(
    ("task_id", "retries", "retry_delay"),
    [
        ("extract_worldbank", 2, timedelta(minutes=5)),
        ("rotate_landing_sas", 1, timedelta(minutes=2)),
        ("run_databricks_job", 1, timedelta(minutes=10)),
        ("dbt_build", 0, None),
    ],
)
def test_retries(dag, task_id, retries, retry_delay):
    task = dag.get_task(task_id)
    assert task.retries == retries
    if retry_delay is not None:
        assert task.retry_delay == retry_delay
    assert task.execution_timeout is not None


def test_containers_are_removed_and_hold_no_secrets(dag):
    containers = [t for t in dag.tasks if type(t).__name__ == "TaskContainer"]
    assert len(containers) == 3
    for task in containers:
        # A stopped container keeps its environment visible to `docker inspect`.
        assert task.auto_remove == "force"
        # Credentials are added in execute(), never at parse time (template fields, UI, logs).
        assert not any(hint in name for name in task.environment for hint in SECRET_HINTS)
