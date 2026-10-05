"""Export executed Databricks notebooks as clean .ipynb snapshots for the public repo.

The .py files in notebooks/ stay the source of truth. A snapshot only shows what one run
produced, so it must be safe to publish:

1. Export the notebook from the workspace with the Databricks CLI (JUPYTER format).
2. Refuse if its code differs from the repo's .py (e.g. edited in the browser):
   re-import with --overwrite, re-run, then export again.
3. Strip Databricks metadata: widget values, cell ids, timings, compute preferences.
4. Scan the result for anything identifying: e-mails, GUIDs, workspace hosts, /Users/
   paths, SAS signatures, storage endpoints, plus values known only at runtime (the storage
   account from $DDLAKE_STORAGE_ACCOUNT and the current Databricks user). Any hit aborts.
5. Write docs/notebook-runs/<date>_<notebook>.ipynb.

Sensitive values are never written in this file; they come from the environment.

    $env:DDLAKE_STORAGE_ACCOUNT = "<storage-account-name>"
    python -m tools.export_notebook_snapshot 10_worldbank_land_to_bronze \
        20_worldbank_bronze_to_silver
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import tempfile
from datetime import date
from pathlib import Path
from typing import Any

REPO = Path(__file__).resolve().parent.parent
NOTEBOOKS = REPO / "notebooks"
SNAPSHOTS = REPO / "docs" / "notebook-runs"

DBX_NOTEBOOK = "application/vnd.databricks.v1+notebook"
DBX_CELL = "application/vnd.databricks.v1+cell"
KEEP_NOTEBOOK_KEYS = {"notebookName", "language", "environmentMetadata"}

GENERIC_PATTERNS = {
    "e-mail": r"[\w.+-]+@[\w-]+\.[\w.-]+",
    "GUID": r"\b[0-9a-fA-F]{8}-(?:[0-9a-fA-F]{4}-){3}[0-9a-fA-F]{12}\b",
    "workspace host": r"[\w-]+\.cloud\.databricks\.com|[\w-]+\.azuredatabricks\.net",
    "user path": r"/Users/[^\s\"'/]+",
    "SAS signature": r"[?&]sig=",
    "storage endpoint": r"[a-z0-9]{3,24}\.(?:dfs|blob)\.core\.windows\.net",
}


class SnapshotError(RuntimeError):
    pass


def source_cells(py_text: str) -> list[str]:
    """Split a Databricks source-format .py into normalized cell bodies."""
    text = py_text.replace("\r\n", "\n").removeprefix("# Databricks notebook source\n")
    cells = []
    for chunk in text.split("\n# COMMAND ----------\n"):
        lines = chunk.strip("\n").split("\n")
        if lines and all(line.startswith("# MAGIC") for line in lines if line):
            lines = [line.removeprefix("# MAGIC").removeprefix(" ") for line in lines]
            lines[0] = lines[0].removeprefix("%md").lstrip()
        cells.append(_normalize("\n".join(lines)))
    return cells


def _normalize(text: str) -> str:
    return "\n".join(line.rstrip() for line in text.strip().splitlines())


def _cell_text(cell: dict[str, Any]) -> str:
    src = cell.get("source", "")
    return _normalize("".join(src) if isinstance(src, list) else src)


def check_matches_repo(notebook: dict[str, Any], repo_cells: list[str]) -> None:
    exported = [_cell_text(c) for c in notebook["cells"]]
    if len(exported) != len(repo_cells):
        raise SnapshotError(
            f"{len(exported)} cells in the workspace vs {len(repo_cells)} in the repo. "
            "Re-import the notebook with --overwrite, re-run it, then export again."
        )
    for i, (got, want) in enumerate(zip(exported, repo_cells, strict=True)):
        if got != want:
            first = next(
                (g, w) for g, w in zip(got.splitlines() + [""], want.splitlines() + [""],
                                       strict=False) if g != w
            )
            raise SnapshotError(
                f"Cell {i} differs from the repo (workspace: {first[0]!r}, repo: {first[1]!r}). "
                "Re-import the notebook with --overwrite, re-run it, then export again."
            )


def clean(notebook: dict[str, Any]) -> dict[str, Any]:
    """Drop Databricks metadata that is noisy or identifying; keep code and outputs."""
    meta = notebook.get("metadata", {})
    dbx = meta.get(DBX_NOTEBOOK, {})
    meta[DBX_NOTEBOOK] = {k: v for k, v in dbx.items() if k in KEEP_NOTEBOOK_KEYS}
    for cell in notebook["cells"]:
        cell_dbx = cell.get("metadata", {}).get(DBX_CELL, {})
        title = cell_dbx.get("title")
        cell["metadata"] = {DBX_CELL: {"title": title}} if title else {}
    return notebook


def scan(text: str, sensitive_values: list[str]) -> list[str]:
    findings = [name for name, pattern in GENERIC_PATTERNS.items() if re.search(pattern, text)]
    findings += [
        "runtime value #" + str(i)  # never echo the value itself
        for i, value in enumerate(sensitive_values, 1)
        if value and value.lower() in text.lower()
    ]
    return findings


def _databricks(*args: str, profile: str) -> str:
    result = subprocess.run(
        ["databricks", *args, "--profile", profile], capture_output=True, text=True, check=False
    )
    if result.returncode != 0:
        raise SnapshotError(f"databricks {' '.join(args)} failed: {result.stderr.strip()}")
    return result.stdout


def export_snapshot(
    name: str, workspace_dir: str, profile: str, sensitive_values: list[str], run_date: str
) -> Path:
    repo_file = NOTEBOOKS / f"{name}.py"
    if not repo_file.exists():
        raise SnapshotError(f"No notebook {repo_file.relative_to(REPO)} in the repo")

    with tempfile.TemporaryDirectory() as tmp:
        raw_file = Path(tmp) / f"{name}.ipynb"
        _databricks("workspace", "export", f"{workspace_dir}/{name}", "--format", "JUPYTER",
                    "--file", str(raw_file), profile=profile)
        notebook = json.loads(raw_file.read_text(encoding="utf-8"))

    check_matches_repo(notebook, source_cells(repo_file.read_text(encoding="utf-8")))
    if not any(c.get("outputs") for c in notebook["cells"]):
        raise SnapshotError(f"{name} has no outputs; run it in the workspace first")

    text = json.dumps(clean(notebook), indent=1, ensure_ascii=False) + "\n"
    findings = scan(text, sensitive_values)
    if findings:
        raise SnapshotError(f"{name}: not written, sensitive content found: {findings}")

    SNAPSHOTS.mkdir(parents=True, exist_ok=True)
    target = SNAPSHOTS / f"{run_date}_{name}.ipynb"
    target.write_text(text, encoding="utf-8", newline="\n")
    return target


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m tools.export_notebook_snapshot",
                                     description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("notebooks", nargs="+", help="notebook names without .py")
    parser.add_argument("--profile", default=os.environ.get("DATABRICKS_CONFIG_PROFILE", "ddlake"))
    parser.add_argument("--workspace-subdir", default="ddlake",
                        help="folder under /Users/<you>/ (default: ddlake)")
    parser.add_argument("--date", default=date.today().isoformat())
    args = parser.parse_args(argv)

    storage = os.environ.get("DDLAKE_STORAGE_ACCOUNT", "")
    if not storage:
        parser.error("set DDLAKE_STORAGE_ACCOUNT so the scan can check for it")
    try:
        user = json.loads(_databricks("current-user", "me", "-o", "json",
                                      profile=args.profile))["userName"]
        workspace_dir = f"/Users/{user}/{args.workspace_subdir}"
        for name in args.notebooks:
            path = export_snapshot(name, workspace_dir, args.profile, [storage, user], args.date)
            print(f"wrote {path.relative_to(REPO).as_posix()}")
    except SnapshotError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
