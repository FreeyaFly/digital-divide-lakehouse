"""Thin client for the World Bank Indicators API v2 (https://api.worldbank.org/v2).

Quirks handled here so the rest of the code can ignore them:
* Successful responses are a two-element list: [page_metadata, records].
* Errors also come back as HTTP 200, shaped as [{"message": [...]}].
* `per_page` and similar metadata fields are sometimes strings, sometimes numbers.
"""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

BASE_URL = "https://api.worldbank.org/v2"


class WorldBankApiError(RuntimeError):
    """The API answered, but not with usable data."""


@dataclass(frozen=True)
class Page:
    number: int
    pages: int
    total: int
    records: list[dict[str, Any]]


class WorldBankClient:
    def __init__(
        self,
        base_url: str = BASE_URL,
        timeout: float = 60,
        retries: int = 5,
        backoff_factor: float = 1.0,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.session = requests.Session()
        # Retry transient failures with exponential backoff (1s, 2s, 4s, ...).
        retry = Retry(
            total=retries,
            backoff_factor=backoff_factor,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=("GET",),
            raise_on_status=False,
        )
        self.session.mount("https://", HTTPAdapter(max_retries=retry))

    def _get(self, path: str, params: dict[str, Any]) -> Any:
        response = self.session.get(
            f"{self.base_url}/{path.lstrip('/')}",
            params={"format": "json", **params},
            timeout=self.timeout,
        )
        response.raise_for_status()
        try:
            body = response.json()
        except ValueError as exc:
            raise WorldBankApiError(f"Non-JSON response from {response.url}") from exc

        if isinstance(body, list) and len(body) == 1 and isinstance(body[0], dict):
            messages = body[0].get("message")
            if messages:
                raise WorldBankApiError(f"{response.url}: {messages}")
        if not (isinstance(body, list) and len(body) == 2 and isinstance(body[0], dict)):
            raise WorldBankApiError(f"Unexpected response shape from {response.url}")
        return body

    def iter_pages(self, path: str, params: dict[str, Any] | None = None) -> Iterator[Page]:
        """Yield every page of a paginated endpoint, starting at page 1."""
        page_number, pages = 1, 1
        while page_number <= pages:
            meta, records = self._get(path, {**(params or {}), "page": page_number})
            pages = int(meta.get("pages") or 0)
            yield Page(
                number=page_number,
                pages=pages,
                total=int(meta.get("total") or 0),
                # An empty result set comes back as `null` instead of [].
                records=records or [],
            )
            page_number += 1

    def source_last_updated(self, source_id: int) -> str:
        """Date (YYYY-MM-DD) the source was last refreshed; used as the ingestion watermark."""
        _, sources = self._get(f"sources/{source_id}", {})
        if not sources or not sources[0].get("lastupdated"):
            raise WorldBankApiError(f"Source {source_id} has no lastupdated field")
        return str(sources[0]["lastupdated"])

    def iter_indicator_pages(
        self, indicator: str, start_year: int, end_year: int, per_page: int
    ) -> Iterator[Page]:
        return self.iter_pages(
            f"country/all/indicator/{indicator}",
            {"date": f"{start_year}:{end_year}", "per_page": per_page},
        )

    def iter_country_pages(self, per_page: int = 1000) -> Iterator[Page]:
        return self.iter_pages("country", {"per_page": per_page})
