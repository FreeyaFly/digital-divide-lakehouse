import json

import pytest

from tools.export_notebook_snapshot import (
    DBX_CELL,
    DBX_NOTEBOOK,
    SnapshotError,
    check_matches_repo,
    clean,
    scan,
    source_cells,
)

# Fake values only; nothing here is a real account, user or workspace.
SOURCE_PY = """# Databricks notebook source
# MAGIC %md
# MAGIC # Title
# MAGIC
# MAGIC Some text.

# COMMAND ----------

dbutils.widgets.text("storage_account", "")
ACCOUNT = dbutils.widgets.get("storage_account")

# COMMAND ----------

# MAGIC %md ## Step 1

# COMMAND ----------

print("rows: 42")
"""


def exported(storage_default: str = "", widget_value: str = "fakestorage01") -> dict:
    """Shape of a Databricks JUPYTER export, trimmed to what the tool touches."""
    return {
        "nbformat": 4,
        "metadata": {
            DBX_NOTEBOOK: {
                "notebookName": "10_demo",
                "language": "python",
                "environmentMetadata": {"environment_version": "6"},
                "widgets": {"storage_account": {"currentValue": widget_value,
                                                "nuid": "11111111-2222-3333-4444-555555555555"}},
                "computePreferences": {"x": 1},
                "dashboards": [],
            }
        },
        "cells": [
            {"cell_type": "markdown", "metadata": {}, "source": ["# Title\n", "\n", "Some text."]},
            {"cell_type": "code", "execution_count": 1, "outputs": [],
             "metadata": {DBX_CELL: {"nuid": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
                                     "startTime": 1, "inputWidgets": {}, "title": ""}},
             "source": [f'dbutils.widgets.text("storage_account", "{storage_default}")\n',
                        'ACCOUNT = dbutils.widgets.get("storage_account")']},
            {"cell_type": "markdown", "metadata": {}, "source": ["## Step 1"]},
            {"cell_type": "code", "execution_count": 2,
             "metadata": {DBX_CELL: {"title": "Count", "finishTime": 2}},
             "outputs": [{"output_type": "stream", "name": "stdout", "text": ["rows: 42\n"]}],
             "source": ['print("rows: 42")']},
        ],
    }


def test_source_cells_parses_code_and_markdown() -> None:
    cells = source_cells(SOURCE_PY)

    assert cells == [
        "# Title\n\nSome text.",
        'dbutils.widgets.text("storage_account", "")\n'
        'ACCOUNT = dbutils.widgets.get("storage_account")',
        "## Step 1",
        'print("rows: 42")',
    ]


def test_matching_export_passes() -> None:
    check_matches_repo(exported(), source_cells(SOURCE_PY))


def test_code_edited_in_the_workspace_is_rejected() -> None:
    with pytest.raises(SnapshotError, match="Cell 1 differs"):
        check_matches_repo(exported(storage_default="fakestorage01"), source_cells(SOURCE_PY))


def test_cell_count_mismatch_is_rejected() -> None:
    nb = exported()
    nb["cells"].pop()

    with pytest.raises(SnapshotError, match="3 cells in the workspace vs 4"):
        check_matches_repo(nb, source_cells(SOURCE_PY))


def test_clean_drops_widget_values_ids_and_timings_but_keeps_outputs() -> None:
    nb = clean(exported())

    assert nb["metadata"][DBX_NOTEBOOK] == {
        "notebookName": "10_demo",
        "language": "python",
        "environmentMetadata": {"environment_version": "6"},
    }
    assert nb["cells"][1]["metadata"] == {}  # empty title, nuid, timings, widgets all gone
    assert nb["cells"][3]["metadata"] == {DBX_CELL: {"title": "Count"}}
    assert nb["cells"][3]["outputs"][0]["text"] == ["rows: 42\n"]
    text = json.dumps(nb)
    assert "fakestorage01" not in text and "11111111-2222" not in text


@pytest.mark.parametrize(
    ("snippet", "finding"),
    [
        ("contact someone@example.com", "e-mail"),
        ("id 12345678-1234-1234-1234-123456789abc", "GUID"),
        ("https://dbc-0000aaaa-1111.cloud.databricks.com/", "workspace host"),
        ("/Users/someone/ddlake", "user path"),
        ("https://x.blob.core.windows.net/c?sv=1&sig=abc", "SAS signature"),
        ("https://fakestorage01.dfs.core.windows.net/landing", "storage endpoint"),
    ],
)
def test_scan_finds_generic_patterns(snippet: str, finding: str) -> None:
    assert finding in scan(snippet, [])


def test_scan_finds_runtime_values_without_echoing_them() -> None:
    findings = scan("loaded from FakeStorage01 today", ["fakestorage01"])

    assert findings == ["runtime value #1"]
    assert "fakestorage01" not in " ".join(findings)


def test_clean_export_scans_clean() -> None:
    text = json.dumps(clean(exported()))

    assert scan(text, ["fakestorage01"]) == []
