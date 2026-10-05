"""Land World Bank indicators and country metadata as gzipped JSON Lines, incrementally.

Layout under the landing container (one partition per source version):

    worldbank/source_updated=<YYYY-MM-DD>/indicators/indicator=<code>/page-0001.jsonl.gz
    worldbank/source_updated=<YYYY-MM-DD>/countries/page-0001.jsonl.gz
    worldbank/_manifests/run_id=<UTC timestamp>.json     one per run, including skipped runs
    worldbank/_state/watermark.json                      written last = commit point

Incremental rule:
* WDI `lastupdated` changed, or year range changed  -> fetch everything into a new partition.
* Otherwise                                          -> fetch only indicators not landed yet.
* Nothing missing                                    -> skip (no data API calls).

Re-running is safe: file paths depend only on the source version, so a retry overwrites the
same files. If a run dies half-way, the watermark is not updated and the next run redoes it.
"""

from __future__ import annotations

import gzip
import json
import logging
import tomllib
from dataclasses import dataclass, field
from datetime import UTC, datetime
from importlib.resources import files
from pathlib import Path
from typing import Any

from ingestion.common.storage import Storage
from ingestion.worldbank.client import WorldBankClient

log = logging.getLogger(__name__)

PREFIX = "worldbank"
WATERMARK_PATH = f"{PREFIX}/_state/watermark.json"


@dataclass(frozen=True)
class WorldBankConfig:
    source_id: int
    start_year: int
    end_year: int
    per_page: int
    indicators: tuple[str, ...]

    @classmethod
    def load(cls, path: Path | None = None) -> WorldBankConfig:
        raw = (
            path.read_text(encoding="utf-8")
            if path
            else files("ingestion").joinpath("config/worldbank.toml").read_text(encoding="utf-8")
        )
        data = tomllib.loads(raw)
        indicators = tuple(dict.fromkeys(data["indicators"]))  # de-duplicate, keep order
        return cls(
            source_id=int(data["source_id"]),
            start_year=int(data["start_year"]),
            end_year=int(data["end_year"]),
            per_page=int(data["per_page"]),
            indicators=indicators,
        )


@dataclass
class RunResult:
    status: str  # "skipped" | "landed" | "dry_run"
    source_updated: str
    indicators: list[str] = field(default_factory=list)
    countries: bool = False
    files: list[dict[str, Any]] = field(default_factory=list)

    @property
    def rows(self) -> int:
        return sum(f["rows"] for f in self.files)

    @property
    def bytes(self) -> int:
        return sum(f["bytes"] for f in self.files)


def _to_jsonl_gz(records: list[dict[str, Any]]) -> bytes:
    # Records are kept exactly as the API returns them; only the envelope is dropped.
    text = "".join(json.dumps(r, ensure_ascii=False, sort_keys=True) + "\n" for r in records)
    # mtime=0 keeps the gzip header constant, so the same records always give the same bytes.
    return gzip.compress(text.encode("utf-8"), mtime=0)


def _land(storage: Storage, path: str, records: list[dict[str, Any]]) -> dict[str, Any]:
    data = _to_jsonl_gz(records)
    storage.write_bytes(path, data)
    return {"path": path, "rows": len(records), "bytes": len(data)}


def is_same_version(
    config: WorldBankConfig, watermark: dict[str, Any] | None, source_updated: str
) -> bool:
    """True when the watermark describes the same WDI release and the same year range."""
    return (
        watermark is not None
        and watermark.get("source_updated") == source_updated
        and watermark.get("year_range") == [config.start_year, config.end_year]
    )


def plan(
    config: WorldBankConfig, watermark: dict[str, Any] | None, source_updated: str, force: bool
) -> tuple[list[str], bool]:
    """Decide which indicators (and whether countries) need landing for this source version."""
    if force or not is_same_version(config, watermark, source_updated):
        return list(config.indicators), True
    landed = set(watermark.get("indicators", []))
    missing = [i for i in config.indicators if i not in landed]
    return missing, not watermark.get("countries", False)


def run(
    storage: Storage,
    client: WorldBankClient,
    config: WorldBankConfig,
    *,
    force: bool = False,
    dry_run: bool = False,
    now: datetime | None = None,
) -> RunResult:
    started = now or datetime.now(UTC)
    run_id = started.strftime("%Y%m%dT%H%M%SZ")

    raw_watermark = storage.read_text(WATERMARK_PATH)
    watermark = json.loads(raw_watermark) if raw_watermark else None
    source_updated = client.source_last_updated(config.source_id)
    indicators, countries = plan(config, watermark, source_updated, force)

    result = RunResult(
        status="skipped", source_updated=source_updated, indicators=indicators, countries=countries
    )
    if dry_run:
        # Plan only: no data calls and no writes at all, not even a manifest.
        result.status = "dry_run"
        log.info("Dry run: would land %d indicators, countries=%s", len(indicators), countries)
        return result
    if not indicators and not countries:
        log.info("WDI unchanged (%s) and all indicators landed: nothing to do", source_updated)
    else:
        result.status = "landed"
        partition = f"{PREFIX}/source_updated={source_updated}"
        for indicator in indicators:
            pages = client.iter_indicator_pages(
                indicator, config.start_year, config.end_year, config.per_page
            )
            folder = f"{partition}/indicators/indicator={indicator}"
            for page in pages:
                path = f"{folder}/page-{page.number:04d}.jsonl.gz"
                result.files.append(_land(storage, path, page.records))
            log.info("Landed %s", indicator)
        if countries:
            for page in client.iter_country_pages():
                path = f"{partition}/countries/page-{page.number:04d}.jsonl.gz"
                result.files.append(_land(storage, path, page.records))
            log.info("Landed country metadata")

    finished = datetime.now(UTC)
    manifest = {
        "run_id": run_id,
        "started_at": started.isoformat(),
        "finished_at": finished.isoformat(),
        "status": result.status,
        "source_id": config.source_id,
        "source_updated": source_updated,
        "year_range": [config.start_year, config.end_year],
        "indicators": result.indicators,
        "countries": result.countries,
        "files": result.files,
        "rows": result.rows,
        "bytes": result.bytes,
    }
    storage.write_text(f"{PREFIX}/_manifests/run_id={run_id}.json", json.dumps(manifest, indent=2))

    if result.status == "landed":
        # Same source version: extend the landed set. New version: start a fresh set.
        same_version = not force and is_same_version(config, watermark, source_updated)
        previous = set(watermark.get("indicators", [])) if same_version else set()
        new_watermark = {
            "source_id": config.source_id,
            "source_updated": source_updated,
            "year_range": [config.start_year, config.end_year],
            "indicators": sorted(previous | set(indicators)),
            "countries": bool(countries or (same_version and watermark.get("countries"))),
            "last_run_id": run_id,
        }
        storage.write_text(WATERMARK_PATH, json.dumps(new_watermark, indent=2))
    return result
