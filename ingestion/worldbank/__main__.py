"""Command line entry point.

    python -m ingestion.worldbank --target local            # writes to ./data/landing
    python -m ingestion.worldbank --target adls             # needs DDLAKE_STORAGE_ACCOUNT
    python -m ingestion.worldbank --target adls --dry-run   # show what would be landed
    python -m ingestion.worldbank --target adls --force     # re-land the current version
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path

from ingestion.common.storage import AdlsStorage, LocalStorage, Storage
from ingestion.worldbank.client import WorldBankClient
from ingestion.worldbank.extract import WorldBankConfig, run


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m ingestion.worldbank", description=__doc__)
    parser.add_argument("--target", choices=("local", "adls"), default="local")
    parser.add_argument("--local-root", type=Path, default=Path("data/landing"))
    parser.add_argument(
        "--storage-account",
        default=os.environ.get("DDLAKE_STORAGE_ACCOUNT"),
        help="ADLS account name (default: $DDLAKE_STORAGE_ACCOUNT)",
    )
    parser.add_argument("--config", type=Path, help="override the packaged worldbank.toml")
    parser.add_argument("--force", action="store_true", help="ignore the watermark")
    parser.add_argument("--dry-run", action="store_true", help="plan only, write nothing")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    # The Azure SDK logs every HTTP request at INFO; keep only its warnings and errors.
    logging.getLogger("azure").setLevel(logging.WARNING)

    storage: Storage
    if args.target == "adls":
        if not args.storage_account:
            parser.error("--storage-account or DDLAKE_STORAGE_ACCOUNT is required for adls")
        storage = AdlsStorage(args.storage_account)
    else:
        storage = LocalStorage(args.local_root)

    result = run(
        storage,
        WorldBankClient(),
        WorldBankConfig.load(args.config),
        force=args.force,
        dry_run=args.dry_run,
    )
    print(
        f"status={result.status} source_updated={result.source_updated} "
        f"indicators={len(result.indicators)} countries={result.countries} "
        f"files={len(result.files)} rows={result.rows} size={result.bytes / 1e6:.1f}MB"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
