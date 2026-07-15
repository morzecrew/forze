"""The archive's on-disk shape: canonical JSONL rows, gzip files, streamed checksums.

Everything here is bounded-memory and deterministic. A row is written the instant it is produced
(one chunk in flight per plane, the ``reencrypt_objects`` discipline), the file's checksum is
accumulated over the compressed bytes as they are written rather than by re-reading, and the gzip
header carries no timestamp — so the same corpus exported twice yields byte-identical files, which
is what lets an operator diff two archives and a re-export stand in as an equality observable.
"""

from __future__ import annotations

import gzip
import hashlib
from collections.abc import AsyncGenerator, AsyncIterator, Iterator
from pathlib import Path
from typing import BinaryIO, Final

import orjson

from forze.base.exceptions import exc
from forze.base.primitives import JsonDict

# ----------------------- #

_CANONICAL: Final = orjson.OPT_SORT_KEYS | orjson.OPT_APPEND_NEWLINE
"""Sorted keys + trailing newline: one canonical JSONL line per row, stable across processes.

No ``default`` — a value orjson cannot serialize is a bug in the caller's ``mode="json"`` encode,
and it must raise here rather than be silently stringified into the artifact (the ``default=str``
memory-address trap that a fingerprint or an archive can never afford)."""

_GZIP_MTIME: Final = 0
"""Pin the gzip header timestamp so an archive is a pure function of its rows."""


# ....................... #


class _HashingSink:
    """A binary sink that mirrors every byte into a running SHA-256 as it passes through.

    Wrapping the *compressed* stream means the digest is of the file exactly as it lands on disk,
    so a reader can verify the bytes it holds without decompressing them first — and the hash
    costs one pass, not a re-read of what was just written.
    """

    def __init__(self, raw: BinaryIO) -> None:
        self._raw = raw
        self.digest = hashlib.sha256()

    def write(self, data: bytes) -> int:
        self.digest.update(data)
        return self._raw.write(data)

    def flush(self) -> None:
        self._raw.flush()

    def close(self) -> None:
        self._raw.close()


# ....................... #


class JsonlGzipWriter:
    """Stream canonical-JSON rows into one gzip file, tracking the row count and the file digest.

    Use as a context manager; :attr:`sha256` and :attr:`rows` are final only after it closes.
    """

    def __init__(self, path: Path) -> None:
        self._path = path
        self.rows = 0
        self._sink: _HashingSink | None = None
        self._gzip: gzip.GzipFile | None = None

    def __enter__(self) -> JsonlGzipWriter:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._sink = _HashingSink(self._path.open("wb"))
        self._gzip = gzip.GzipFile(fileobj=self._sink, mode="wb", mtime=_GZIP_MTIME)
        return self

    def write(self, row: JsonDict) -> None:
        if self._gzip is None:  # pragma: no cover - misuse outside the context manager
            raise exc.internal("JsonlGzipWriter written to before entering its context")

        self._gzip.write(orjson.dumps(row, option=_CANONICAL))
        self.rows += 1

    def __exit__(self, *_: object) -> None:
        if self._gzip is not None:
            self._gzip.close()

        if self._sink is not None:
            self._sink.close()

    @property
    def sha256(self) -> str:
        if self._sink is None:  # pragma: no cover - misuse
            raise exc.internal("JsonlGzipWriter digest read before any write")

        return self._sink.digest.hexdigest()


# ....................... #


def file_sha256(path: Path) -> str:
    """SHA-256 of a file's bytes, read in bounded chunks."""

    digest = hashlib.sha256()

    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)

    return digest.hexdigest()


# ....................... #


def verify_file(path: Path, expected_sha256: str) -> None:
    """Fail closed unless *path* exists and hashes to *expected_sha256*.

    The import side calls this before decoding a single row: a truncated download, a corrupted
    file, or a manifest that does not match its own payload must stop the import loudly, never
    surface as a handful of silently missing documents.
    """

    if not path.exists():
        raise exc.precondition(f"Archive file is missing: {path.name}")

    actual = file_sha256(path)

    if actual != expected_sha256:
        raise exc.precondition(
            f"Archive file {path.name} failed its checksum "
            f"(manifest {expected_sha256[:12]}…, file {actual[:12]}…) — the archive is corrupt "
            f"or was modified after it was written."
        )


# ....................... #


def read_rows(path: Path) -> AsyncGenerator[JsonDict]:
    """Yield each row of a ``.jsonl.gz`` file, one decoded mapping at a time.

    An ``async`` generator so a plane's import reads the same shape it writes — a stream, one row
    of memory — and so the loop yields control between rows.
    """

    async def _gen() -> AsyncGenerator[JsonDict]:
        for row in _iter_rows(path):
            yield row

    return _gen()


def _iter_rows(path: Path) -> Iterator[JsonDict]:
    with gzip.open(path, "rb") as handle:
        for line in handle:
            if line.strip():
                yield orjson.loads(line)


# ....................... #


class _BlobSink:
    """A content-addressed raw-blob file: written under a temp name, renamed to its own sha256.

    Sync (like :class:`JsonlGzipWriter`) so its filesystem I/O stays out of the ``async`` body that
    drives it — the blob writer just feeds it chunks. Content-addressed: the final name is the hash
    of the bytes, so identical objects share one file (dedup) and a storage key — which may hold a
    ``/`` or a ``..`` — never dictates a path. Blobs are stored **raw** (already binary), no gzip.
    """

    def __init__(self, objects_dir: Path) -> None:
        self._dir = objects_dir
        self._digest = hashlib.sha256()
        self.size = 0
        self._tmp = objects_dir / f".partial-{id(self):x}"
        self._handle: BinaryIO | None = None

    def __enter__(self) -> _BlobSink:
        self._dir.mkdir(parents=True, exist_ok=True)
        self._handle = self._tmp.open("wb")
        return self

    def write(self, chunk: bytes) -> None:
        if self._handle is None:  # pragma: no cover - misuse outside the context manager
            raise exc.internal("_BlobSink written to before entering its context")

        self._digest.update(chunk)
        self.size += len(chunk)
        self._handle.write(chunk)

    def __exit__(self, *_: object) -> None:
        if self._handle is not None:
            self._handle.close()
            # Atomic publish; a re-run or a dedup just overwrites byte-identical content.
            self._tmp.replace(self._dir / self.sha256)

    @property
    def sha256(self) -> str:
        return self._digest.hexdigest()


# ....................... #


async def write_blob(chunks: AsyncIterator[bytes], objects_dir: Path) -> tuple[str, int]:
    """Stream a blob to ``objects_dir/<sha256>`` in bounded memory, returning ``(sha256, bytes)``."""

    with _BlobSink(objects_dir) as sink:
        async for chunk in chunks:
            sink.write(chunk)

    return sink.sha256, sink.size


# ....................... #


def read_blob(path: Path, *, expected_sha256: str, chunk_size: int) -> AsyncGenerator[bytes]:
    """Stream a stored blob back out in bounded chunks, after verifying its hash.

    A content file whose bytes do not match the sha256 the index recorded raises **before the
    stream is consumed for upload** — the object is hashed first (bounded memory), so a corrupt
    blob cannot be re-uploaded under a key that still looks intact.
    """

    verify_file(path, expected_sha256)

    async def _gen() -> AsyncGenerator[bytes]:
        for chunk in _iter_file_chunks(path, chunk_size):
            yield chunk

    return _gen()


def _iter_file_chunks(path: Path, chunk_size: int) -> Iterator[bytes]:
    with path.open("rb") as handle:
        yield from iter(lambda: handle.read(chunk_size), b"")
