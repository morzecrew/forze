"""File helper functions used across the base layer."""

import io
from pathlib import Path
from typing import Any, Iterator

import yaml

# ----------------------- #


def read_yaml(path: str | Path) -> dict[str, Any]:
    """Read a YAML file and return a dictionary.

    Empty files are normalized to an empty dict.

    :param path: Path to the YAML file.
    :returns: Parsed YAML document as a dictionary.
    """

    with open(path, "r") as f:
        r = yaml.safe_load(f)

    return r or {}


# ....................... #


def read_text(path: str | Path) -> str:
    """Read a text file and return its full contents."""

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
    """Return an iterator over file bytes from raw bytes or a file-like object."""

    if isinstance(b, bytes):
        return _iter_bytes(b)

    else:
        return _iter_fileobj(b)
