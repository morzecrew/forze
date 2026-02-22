import io
from pathlib import Path
from typing import Any, Iterator

import yaml

# ----------------------- #


def read_yaml(path: str | Path) -> dict[str, Any]:
    with open(path, "r") as f:
        r = yaml.safe_load(f)

    return r or {}


# ....................... #


def read_text(path: str | Path) -> str:
    with open(path, "r") as f:
        return f.read()


# ....................... #


def _iter_bytes(data: bytes, chunk_size: int = 32 * 1024) -> Iterator[bytes]:
    for i in range(0, len(data), chunk_size):
        yield data[i : i + chunk_size]


def _iter_fileobj(
    f: io.BufferedReader | io.BytesIO,
    chunk_size: int = 32 * 1024,
) -> Iterator[bytes]:
    try:
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            yield chunk
    finally:
        f.close()


def iter_file(b: bytes | io.BytesIO) -> Iterator[bytes]:
    if isinstance(b, bytes):
        return _iter_bytes(b)

    else:
        return _iter_fileobj(b)
