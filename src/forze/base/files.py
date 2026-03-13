"""File I/O helpers for YAML, text, and chunked byte iteration."""

import io
from pathlib import Path
from typing import Any, Iterator

import yaml

from .logging import getLogger

# ----------------------- #

logger = getLogger(__name__).bind(scope="files")

# ....................... #


def read_yaml(path: str | Path) -> dict[str, Any]:
    """Read a YAML file and return a dictionary.

    Empty files are normalized to an empty dict.

    :param path: Path to the YAML file.
    :returns: Parsed YAML document as a dictionary.
    """

    logger.trace("Reading YAML file %s", path)

    with open(path, "r") as f:
        r = yaml.safe_load(f)

    return r or {}


# ....................... #


def read_text(path: str | Path) -> str:
    """Read a text file and return its full contents.

    :param path: Path to the text file.
    :returns: File contents as a string.
    """

    logger.trace("Reading text file %s", path)

    with open(path, "r") as f:
        return f.read()


# ....................... #


def _iter_bytes(data: bytes, chunk_size: int = 32 * 1024) -> Iterator[bytes]:
    """Yield ``data`` in fixed-size chunks."""

    for i in range(0, len(data), chunk_size):
        yield data[i : i + chunk_size]


def _iter_fileobj(
    f: io.BufferedReader | io.BytesIO,
    chunk_size: int = 32 * 1024,
) -> Iterator[bytes]:
    """Yield chunks from a file-like object and close it when exhausted."""

    try:
        while True:
            chunk = f.read(chunk_size)

            if not chunk:
                break

            yield chunk

    finally:
        f.close()


def iter_file(b: bytes | io.BytesIO) -> Iterator[bytes]:
    """Yield chunks from raw bytes or a file-like object.

    Uses 32 KB chunks. File-like objects are closed when exhausted.

    :param b: Raw bytes or a readable file-like object.
    :returns: Iterator over byte chunks.
    """

    logger.trace("Iterating file from %s", type(b).__name__)

    if isinstance(b, bytes):
        return _iter_bytes(b)

    else:
        return _iter_fileobj(b)
