"""Rotate the read-only SAS that Databricks uses to read the landing container.

Signs a user delegation SAS (no account keys) for the `landing` container with read and list
permissions only, checks that it works, and stores it in the Databricks secret scope where
Notebook 10 reads it. The SAS itself is never printed or logged, only its expiry.

    python -m ingestion.landing_sas                      # needs DDLAKE_STORAGE_ACCOUNT
    python -m ingestion.landing_sas --hours 24

Azure: DefaultAzureCredential, in the task container a service principal from AZURE_TENANT_ID,
AZURE_CLIENT_ID and AZURE_CLIENT_SECRET. It needs "Storage Blob Delegator" on the account and
read access to the container. Databricks: DATABRICKS_HOST and DATABRICKS_TOKEN.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

import requests

# A user delegation key is valid for at most seven days, and the SAS starts a little in the
# past so that a clock difference between machines does not reject a fresh token.
CLOCK_SKEW = timedelta(minutes=15)
MAX_HOURS = 7 * 24 - 1


class SasError(RuntimeError):
    pass


@dataclass(frozen=True)
class SasWindow:
    start: datetime
    expiry: datetime


def sas_window(now: datetime, hours: int) -> SasWindow:
    if now.tzinfo is None:
        raise SasError("now must be timezone-aware")
    if not 1 <= hours <= MAX_HOURS:
        raise SasError(f"hours must be between 1 and {MAX_HOURS}, got {hours}")
    return SasWindow(start=now - CLOCK_SKEW, expiry=now + timedelta(hours=hours))


def sign_sas(account: str, container: str, window: SasWindow) -> str:
    """User delegation SAS for one container: read and list, HTTPS only."""
    # Imported here so unit tests do not need the Azure SDKs.
    from azure.identity import DefaultAzureCredential
    from azure.storage.blob import (
        BlobServiceClient,
        ContainerSasPermissions,
        generate_container_sas,
    )

    credential = DefaultAzureCredential(exclude_managed_identity_credential=True)
    service = BlobServiceClient(f"https://{account}.blob.core.windows.net", credential=credential)
    key = service.get_user_delegation_key(window.start, window.expiry)
    return generate_container_sas(
        account,
        container,
        user_delegation_key=key,
        permission=ContainerSasPermissions(read=True, list=True),
        start=window.start,
        expiry=window.expiry,
        protocol="https",
    )


def check_sas(account: str, container: str, sas: str) -> None:
    """List one path the way Notebook 10 does, so a broken SAS fails here and not in Databricks."""
    url = (
        f"https://{account}.dfs.core.windows.net/{container}"
        f"?resource=filesystem&recursive=false&maxResults=1&{sas}"
    )
    response = requests.get(url, timeout=60)
    if response.status_code != 200:
        # The URL holds the SAS, so report only the status.
        raise SasError(f"the new SAS cannot list '{container}': HTTP {response.status_code}")


def store_secret(scope: str, key: str, value: str) -> None:
    from databricks.sdk import WorkspaceClient

    WorkspaceClient().secrets.put_secret(scope=scope, key=key, string_value=value)


def rotate(
    account: str,
    container: str,
    scope: str,
    key: str,
    hours: int,
    now: datetime,
    sign: Callable[[str, str, SasWindow], str] = sign_sas,
    check: Callable[[str, str, str], None] = check_sas,
    store: Callable[[str, str, str], None] = store_secret,
) -> datetime:
    """Sign, check, then store; the secret is only replaced by a SAS that works."""
    window = sas_window(now, hours)
    sas = sign(account, container, window)
    check(account, container, sas)
    store(scope, key, sas)
    return window.expiry


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m ingestion.landing_sas", description=__doc__)
    parser.add_argument(
        "--storage-account",
        default=os.environ.get("DDLAKE_STORAGE_ACCOUNT"),
        help="ADLS account name (default: $DDLAKE_STORAGE_ACCOUNT)",
    )
    parser.add_argument("--container", default="landing")
    parser.add_argument("--scope", default="ddlake", help="Databricks secret scope")
    parser.add_argument("--key", default="adls-landing-sas", help="Databricks secret key")
    parser.add_argument("--hours", type=int, default=144, help="validity (default: 6 days)")
    args = parser.parse_args(argv)
    if not args.storage_account:
        parser.error("--storage-account or DDLAKE_STORAGE_ACCOUNT is required")

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    logging.getLogger("azure").setLevel(logging.WARNING)

    expiry = rotate(
        args.storage_account,
        args.container,
        args.scope,
        args.key,
        args.hours,
        now=datetime.now(UTC),
        sign=sign_sas,
        check=check_sas,
        store=store_secret,
    )
    print(
        f"rotated secret {args.scope}/{args.key}: read+list SAS on '{args.container}', "
        f"expires {expiry:%Y-%m-%dT%H:%MZ}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
