"""Where landed files go: a local folder for development, or the ADLS `landing` container.

Both targets share one tiny interface so the extract logic never knows which one it uses.
Paths are POSIX-style and relative to the container root, e.g. "worldbank/_state/watermark.json".
"""

from __future__ import annotations

from pathlib import Path
from typing import Protocol


class Storage(Protocol):
    def read_text(self, path: str) -> str | None:
        """Return the file content, or None if the file does not exist."""
        ...

    def write_text(self, path: str, text: str) -> None:
        """Create or overwrite the file with UTF-8 text."""
        ...

    def write_bytes(self, path: str, data: bytes) -> None:
        """Create or overwrite the file with raw bytes (e.g. gzip)."""
        ...


class LocalStorage:
    def __init__(self, root: Path) -> None:
        self.root = root

    def read_text(self, path: str) -> str | None:
        file = self.root / path
        return file.read_text(encoding="utf-8") if file.exists() else None

    def write_text(self, path: str, text: str) -> None:
        self.write_bytes(path, text.encode("utf-8"))

    def write_bytes(self, path: str, data: bytes) -> None:
        file = self.root / path
        file.parent.mkdir(parents=True, exist_ok=True)
        file.write_bytes(data)


class AdlsStorage:
    """ADLS Gen2 file system, authenticated with the caller's Entra identity (no account keys).

    Locally this resolves to the `az login` session; in a container it can use a service
    principal from environment variables. Requires "Storage Blob Data Contributor".
    """

    def __init__(self, account_name: str, file_system: str = "landing") -> None:
        # Imported here so local runs and unit tests do not need the Azure SDKs.
        from azure.identity import DefaultAzureCredential
        from azure.storage.filedatalake import DataLakeServiceClient

        credential = DefaultAzureCredential(exclude_managed_identity_credential=True)
        service = DataLakeServiceClient(
            account_url=f"https://{account_name}.dfs.core.windows.net", credential=credential
        )
        self._fs = service.get_file_system_client(file_system)

    def read_text(self, path: str) -> str | None:
        from azure.core.exceptions import ResourceNotFoundError

        try:
            return self._fs.get_file_client(path).download_file().readall().decode("utf-8")
        except ResourceNotFoundError:
            return None

    def write_text(self, path: str, text: str) -> None:
        self.write_bytes(path, text.encode("utf-8"))

    def write_bytes(self, path: str, data: bytes) -> None:
        self._fs.get_file_client(path).upload_data(data, overwrite=True)
