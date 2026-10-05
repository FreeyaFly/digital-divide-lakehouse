import gzip
import json
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path

from ingestion.common.storage import LocalStorage
from ingestion.worldbank.client import Page
from ingestion.worldbank.extract import WATERMARK_PATH, WorldBankConfig, run

NOW = datetime(2026, 10, 6, 12, 0, tzinfo=UTC)


class FakeClient:
    """Stands in for WorldBankClient and records which indicators were requested."""

    def __init__(self, source_updated: str = "2026-07-13") -> None:
        self.source_updated = source_updated
        self.indicator_calls: list[str] = []
        self.country_calls = 0

    def source_last_updated(self, source_id: int) -> str:
        return self.source_updated

    def iter_indicator_pages(self, indicator: str, *_: int, **__: int) -> Iterator[Page]:
        self.indicator_calls.append(indicator)
        yield Page(1, 1, 2, [{"countryiso3code": "NLD", "date": "2024", "value": 1.0},
                             {"countryiso3code": "NGA", "date": "2024", "value": None}])

    def iter_country_pages(self) -> Iterator[Page]:
        self.country_calls += 1
        yield Page(1, 1, 1, [{"id": "NLD", "region": {"id": "ECS"}}])


def config(*indicators: str) -> WorldBankConfig:
    return WorldBankConfig(source_id=2, start_year=2000, end_year=2025, per_page=20000,
                           indicators=indicators or ("A", "B"))


def landed_files(root: Path) -> list[str]:
    return sorted(p.relative_to(root).as_posix() for p in root.rglob("*.jsonl.gz"))


def read_jsonl_gz(path: Path) -> list[dict]:
    return [json.loads(line) for line in gzip.decompress(path.read_bytes()).splitlines()]


def watermark(root: Path) -> dict:
    return json.loads((root / WATERMARK_PATH).read_text())


def test_first_run_lands_everything_and_writes_watermark(tmp_path: Path) -> None:
    client = FakeClient()

    result = run(LocalStorage(tmp_path), client, config(), now=NOW)

    assert result.status == "landed"
    assert client.indicator_calls == ["A", "B"] and client.country_calls == 1
    assert landed_files(tmp_path) == [
        "worldbank/source_updated=2026-07-13/countries/page-0001.jsonl.gz",
        "worldbank/source_updated=2026-07-13/indicators/indicator=A/page-0001.jsonl.gz",
        "worldbank/source_updated=2026-07-13/indicators/indicator=B/page-0001.jsonl.gz",
    ]
    assert watermark(tmp_path)["indicators"] == ["A", "B"]
    assert result.rows == 5  # 2 + 2 indicator rows + 1 country


def test_gzipped_jsonl_keeps_records_unchanged(tmp_path: Path) -> None:
    result = run(LocalStorage(tmp_path), FakeClient(), config("A"), now=NOW)

    folder = tmp_path / "worldbank/source_updated=2026-07-13/indicators/indicator=A"
    records = read_jsonl_gz(folder / "page-0001.jsonl.gz")
    assert records == [{"countryiso3code": "NLD", "date": "2024", "value": 1.0},
                       {"countryiso3code": "NGA", "date": "2024", "value": None}]
    manifest = json.loads(next((tmp_path / "worldbank/_manifests").iterdir()).read_text())
    assert manifest["bytes"] == result.bytes > 0


def test_unchanged_source_is_skipped(tmp_path: Path) -> None:
    storage = LocalStorage(tmp_path)
    run(storage, FakeClient(), config(), now=NOW)
    client = FakeClient()

    result = run(storage, client, config(), now=NOW)

    assert result.status == "skipped"
    assert client.indicator_calls == [] and client.country_calls == 0
    assert len(list((tmp_path / "worldbank/_manifests").iterdir())) == 1  # same run_id overwrote


def test_new_indicator_in_config_fetches_only_that_one(tmp_path: Path) -> None:
    storage = LocalStorage(tmp_path)
    run(storage, FakeClient(), config("A", "B"), now=NOW)
    client = FakeClient()

    run(storage, client, config("A", "B", "C"), now=NOW)

    assert client.indicator_calls == ["C"] and client.country_calls == 0
    assert watermark(tmp_path)["indicators"] == ["A", "B", "C"]


def test_new_source_version_relands_into_new_partition(tmp_path: Path) -> None:
    storage = LocalStorage(tmp_path)
    run(storage, FakeClient("2026-07-13"), config(), now=NOW)
    client = FakeClient("2026-12-18")

    run(storage, client, config(), now=NOW)

    assert client.indicator_calls == ["A", "B"] and client.country_calls == 1
    assert watermark(tmp_path)["source_updated"] == "2026-12-18"
    assert any("source_updated=2026-12-18" in f for f in landed_files(tmp_path))


def test_changed_year_range_relands_everything(tmp_path: Path) -> None:
    storage = LocalStorage(tmp_path)
    run(storage, FakeClient(), config(), now=NOW)
    client = FakeClient()
    wider = WorldBankConfig(2, 1990, 2025, 20000, ("A", "B"))

    run(storage, client, wider, now=NOW)

    assert client.indicator_calls == ["A", "B"]
    assert watermark(tmp_path)["year_range"] == [1990, 2025]


def test_force_is_idempotent(tmp_path: Path) -> None:
    storage = LocalStorage(tmp_path)
    run(storage, FakeClient(), config(), now=NOW)
    before = {f: (tmp_path / f).read_bytes() for f in landed_files(tmp_path)}

    run(storage, FakeClient(), config(), force=True, now=NOW)

    after = {f: (tmp_path / f).read_bytes() for f in landed_files(tmp_path)}
    assert after == before  # same paths, byte-identical gzip (fixed mtime in the header)


def test_dry_run_writes_nothing(tmp_path: Path) -> None:
    client = FakeClient()

    result = run(LocalStorage(tmp_path), client, config(), dry_run=True, now=NOW)

    assert result.status == "dry_run" and result.indicators == ["A", "B"]
    assert client.indicator_calls == []
    assert not any(tmp_path.iterdir())


def test_packaged_config_loads() -> None:
    cfg = WorldBankConfig.load()

    assert cfg.source_id == 2
    assert len(cfg.indicators) == 14
    assert "IT.MLT.MAIN.P2" not in cfg.indicators  # fixed telephone was dropped
