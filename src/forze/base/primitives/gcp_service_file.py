"""Temp service-account JSON files for GCP clients that require a filesystem path."""

import os
import tempfile
from pathlib import Path

# ----------------------- #


def materialize_service_account_json(raw: str, *, prefix: str) -> tuple[str, bool]:
    """Write *raw* JSON to a temp file and return ``(path, owned=True)``."""

    fd, path = tempfile.mkstemp(prefix=prefix, suffix=".json")

    try:
        os.write(fd, raw.encode("utf-8"))
    finally:
        os.close(fd)

    return path, True


# ....................... #


def release_service_file(path: str | None, *, owned: bool) -> None:
    """Remove *path* when Forze created it; ignore missing files."""

    if not owned or path is None:
        return

    try:
        Path(path).unlink(missing_ok=True)
    except OSError:
        pass
