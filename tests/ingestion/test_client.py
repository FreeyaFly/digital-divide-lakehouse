import pytest
import responses
from responses.registries import OrderedRegistry

from ingestion.worldbank.client import BASE_URL, WorldBankApiError, WorldBankClient

INDICATOR_URL = f"{BASE_URL}/country/all/indicator/IT.NET.USER.ZS"


def record(iso3: str, year: str, value: float | None) -> dict:
    return {
        "indicator": {"id": "IT.NET.USER.ZS", "value": "Individuals using the Internet"},
        "country": {"id": iso3[:2], "value": iso3},
        "countryiso3code": iso3,
        "date": year,
        "value": value,
    }


def meta(page: int, pages: int, total: int) -> dict:
    # The live API mixes strings and numbers here.
    return {"page": page, "pages": pages, "per_page": "2", "total": total,
            "lastupdated": "2026-07-13"}


def client() -> WorldBankClient:
    return WorldBankClient(retries=3, backoff_factor=0)


@responses.activate
def test_iter_pages_follows_pagination() -> None:
    responses.get(
        INDICATOR_URL,
        json=[meta(1, 2, 3), [record("NLD", "2024", 97.0), record("NLD", "2023", 96.5)]],
        match=[responses.matchers.query_param_matcher({"format": "json", "date": "2000:2025",
                                                       "per_page": "2", "page": "1"})],
    )
    responses.get(
        INDICATOR_URL,
        json=[meta(2, 2, 3), [record("NGA", "2024", None)]],
        match=[responses.matchers.query_param_matcher({"format": "json", "date": "2000:2025",
                                                       "per_page": "2", "page": "2"})],
    )

    pages = list(client().iter_indicator_pages("IT.NET.USER.ZS", 2000, 2025, per_page=2))

    assert [p.number for p in pages] == [1, 2]
    assert sum(len(p.records) for p in pages) == 3
    assert pages[1].records[0]["value"] is None  # missing values stay None, not 0


@responses.activate
def test_empty_result_yields_no_records() -> None:
    responses.get(INDICATOR_URL, json=[meta(1, 0, 0), None])

    pages = list(client().iter_indicator_pages("IT.NET.USER.ZS", 2000, 2025, per_page=2))

    assert len(pages) == 1 and pages[0].records == []


@responses.activate
def test_error_payload_with_http_200_raises() -> None:
    responses.get(
        INDICATOR_URL,
        json=[{"message": [{"id": "120", "key": "Invalid value", "value": "not valid"}]}],
    )

    with pytest.raises(WorldBankApiError, match="Invalid value"):
        list(client().iter_indicator_pages("IT.NET.USER.ZS", 2000, 2025, per_page=2))


@responses.activate
def test_non_json_response_raises() -> None:
    responses.get(INDICATOR_URL, body="<html>maintenance</html>")

    with pytest.raises(WorldBankApiError, match="Non-JSON"):
        list(client().iter_indicator_pages("IT.NET.USER.ZS", 2000, 2025, per_page=2))


@responses.activate(registry=OrderedRegistry)
def test_transient_503_is_retried() -> None:
    responses.get(INDICATOR_URL, status=503)
    responses.get(INDICATOR_URL, status=503)
    responses.get(INDICATOR_URL, json=[meta(1, 1, 1), [record("NLD", "2024", 97.0)]])

    pages = list(client().iter_indicator_pages("IT.NET.USER.ZS", 2000, 2025, per_page=2))

    assert pages[0].records[0]["countryiso3code"] == "NLD"
    assert len(responses.calls) == 3


@responses.activate
def test_source_last_updated() -> None:
    responses.get(
        f"{BASE_URL}/sources/2",
        json=[{"page": 1, "pages": 1}, [{"id": "2", "lastupdated": "2026-07-13"}]],
    )

    assert client().source_last_updated(2) == "2026-07-13"
