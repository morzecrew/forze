"""The archive's on-disk shape: canonical JSONL rows, compressed files, streamed checksums.

Everything here is bounded-memory and deterministic. A row is written the instant it is produced
(one chunk in flight per plane, the ``reencrypt_objects`` discipline), the file's checksum is
accumulated over the *compressed* bytes as they are written rather than by re-reading, and no codec
carries a timestamp — so the same corpus exported twice yields byte-identical files, which is what
lets an operator diff two archives and a re-export stand in as an equality observable.

The codec is one of :data:`Compression`, chosen once per archive and recorded in the manifest.
``gzip`` is stdlib and always available; ``zstd`` needs the optional ``zstandard`` extra and fails
closed with a clear error naming it; ``none`` stores rows uncompressed (debugging, or already-tiny
archives). Blobs are always raw regardless — they are usually already-compressed media.
"""

from __future__ import annotations

import gzip
import hashlib
import io
from collections.abc import AsyncGenerator, AsyncIterator, Iterator
from pathlib import Path
from typing import Any, BinaryIO, Final, Literal, Protocol

import attrs
import orjson

from forze.base.exceptions import exc
from forze.base.primitives import JsonDict

# ----------------------- #

Compression = Literal["gzip", "zstd", "none"]
"""Codec for the JSONL data files, declared in the manifest so an importer fails closed on one it
lacks. ``gzip`` (stdlib, always readable), ``zstd`` (optional ``zstandard`` extra, faster + tighter),
``none`` (uncompressed)."""

_DATA_SUFFIX: Final[dict[str, str]] = {
    "gzip": ".jsonl.gz",
    "zstd": ".jsonl.zst",
    "none": ".jsonl",
}
"""The extension a data file carries under each codec — the codec is visible in the file name, and
an importer strips the right one to recover the spec/kind it names."""

_ZSTD_LEVEL: Final = 10
"""A middle zstd level: well past gzip on ratio, still fast. Fixed so an archive is reproducible."""

_CANONICAL: Final = orjson.OPT_SORT_KEYS | orjson.OPT_APPEND_NEWLINE
"""Sorted keys + trailing newline: one canonical JSONL line per row, stable across processes.

No ``default`` — a value orjson cannot serialize is a bug in the caller's ``mode="json"`` encode,
and it must raise here rather than be silently stringified into the artifact (the ``default=str``
memory-address trap that a fingerprint or an archive can never afford)."""

_GZIP_MTIME: Final = 0
"""Pin the gzip header timestamp so an archive is a pure function of its rows."""


# ....................... #


def data_suffix(compression: Compression) -> str:
    """The file extension a JSONL data file carries under *compression*."""

    return _DATA_SUFFIX[compression]


# ....................... #


def _require_zstd() -> Any:
    """The ``zstandard`` module, or a clear refusal naming the extra it needs.

    zstd is an optional codec: an archive that declares it is unreadable — and an export that asks
    for it unproduceable — without the ``zstandard`` wheel. Fail closed with the install line rather
    than an opaque ``ModuleNotFoundError`` three frames down. Typed ``Any``: the module ships no
    stubs, and it is the single deliberate escape hatch this file allows.
    """

    try:
        import zstandard
    except ImportError as error:  # pragma: no cover - exercised only where the extra is absent
        raise exc.precondition(
            "This archive is zstd-compressed, but the 'zstd' extra is not installed. Install "
            "forze[zstd] (or `pip install zstandard`) to read or write it, or use gzip."
        ) from error

    return zstandard


# ....................... #


class _ByteSink(Protocol):
    """The write side of a codec layer: canonical bytes in, closed when the file is done.

    gzip's ``GzipFile``, zstd's stream writer, and the raw :class:`_HashingSink` (the ``none`` codec)
    all satisfy it — so :func:`_open_compressor` returns one concrete type instead of ``Any``, and
    :class:`JsonlWriter` writes through a typed handle rather than a bare object.
    """

    def write(self, data: bytes, /) -> int: ...

    def close(self) -> None: ...


# ....................... #


class _Hasher(Protocol):
    """The slice of a hashlib digest this file uses: feed the running hash, read it as hex.

    A structural type rather than the private ``hashlib._Hash`` — ``hashlib.sha256()`` satisfies it,
    and the sinks below carry it without reaching into a private name."""

    def update(self, data: bytes, /) -> None: ...

    def hexdigest(self) -> str: ...


# ....................... #


@attrs.define
class _HashingSink:
    """A binary sink that mirrors every byte into a running SHA-256 as it passes through.

    Wrapping the *compressed* stream means the digest is of the file exactly as it lands on disk,
    so a reader can verify the bytes it holds without decompressing them first — and the hash
    costs one pass, not a re-read of what was just written.
    """

    raw: BinaryIO
    digest: _Hasher = attrs.field(factory=hashlib.sha256, init=False)

    def write(self, data: bytes) -> int:
        self.digest.update(data)
        return self.raw.write(data)

    def flush(self) -> None:
        self.raw.flush()

    def close(self) -> None:
        self.raw.close()


# ....................... #


def _open_compressor(sink: _HashingSink, compression: Compression) -> _ByteSink:
    """The writable codec layer wrapping *sink*, or the sink itself for ``none``.

    ``closefd=False`` on the zstd writer, and gzip's own not-closing-the-fileobj behavior, both
    leave the underlying sink for :class:`JsonlWriter` to close once — so the digest is finalized
    exactly once, whatever the codec.
    """

    if compression == "gzip":
        return gzip.GzipFile(fileobj=sink, mode="wb", mtime=_GZIP_MTIME)

    if compression == "zstd":
        return _require_zstd().ZstdCompressor(level=_ZSTD_LEVEL).stream_writer(sink, closefd=False)

    return sink


# ....................... #


@attrs.define
class JsonlWriter:
    """Stream canonical-JSON rows into one compressed file, tracking the row count and file digest.

    Use as a context manager; :attr:`sha256` and :attr:`rows` are final only after it closes. The
    codec is fixed for the file's lifetime and shared across the whole archive.
    """

    path: Path
    compression: Compression = attrs.field(default="gzip", kw_only=True)

    rows: int = attrs.field(default=0, init=False)
    _sink: _HashingSink | None = attrs.field(default=None, init=False)
    _stream: _ByteSink | None = attrs.field(default=None, init=False)

    def __enter__(self) -> JsonlWriter:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._sink = _HashingSink(self.path.open("wb"))
        self._stream = _open_compressor(self._sink, self.compression)
        return self

    def write(self, row: JsonDict) -> None:
        if self._stream is None:  # pragma: no cover - misuse outside the context manager
            raise exc.internal("JsonlWriter written to before entering its context")

        self._stream.write(orjson.dumps(row, option=_CANONICAL))
        self.rows += 1

    def __exit__(self, *_: object) -> None:
        # For gzip/zstd the codec layer wraps the sink; close it to flush the frame, then close the
        # sink ourselves. For ``none`` the stream *is* the sink, so close it once.
        if self._stream is not None and self._stream is not self._sink:
            self._stream.close()

        if self._sink is not None:
            self._sink.close()

    @property
    def sha256(self) -> str:
        if self._sink is None:  # pragma: no cover - misuse
            raise exc.internal("JsonlWriter digest read before any write")

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


def read_rows(path: Path, *, compression: Compression = "gzip") -> AsyncGenerator[JsonDict]:
    """Yield each row of a compressed JSONL file, one decoded mapping at a time.

    An ``async`` generator so a plane's import reads the same shape it writes — a stream, one row
    of memory — and so the loop yields control between rows. The codec must match what the archive
    was written with (the manifest's ``compression``).
    """

    async def _gen() -> AsyncGenerator[JsonDict]:
        for row in _iter_rows(path, compression):
            yield row

    return _gen()


def _iter_rows(path: Path, compression: Compression) -> Iterator[JsonDict]:
    for line in _iter_lines(path, compression):
        if line.strip():
            yield orjson.loads(line)


def _iter_lines(path: Path, compression: Compression) -> Iterator[bytes]:
    if compression == "gzip":
        with gzip.open(path, "rb") as handle:
            yield from handle

    elif compression == "none":
        with path.open("rb") as handle:
            yield from handle

    else:  # zstd
        with path.open("rb") as raw:
            reader = _require_zstd().ZstdDecompressor().stream_reader(raw)
            yield from io.BufferedReader(reader)


# ....................... #


@attrs.define
class _BlobSink:
    """A content-addressed raw-blob file: written under a temp name, renamed to its own sha256.

    Sync (like :class:`JsonlWriter`) so its filesystem I/O stays out of the ``async`` body that
    drives it — the blob writer just feeds it chunks. Content-addressed: the final name is the hash
    of the bytes, so identical objects share one file (dedup) and a storage key — which may hold a
    ``/`` or a ``..`` — never dictates a path. Blobs are stored **raw** (already binary), no codec.
    """

    objects_dir: Path

    _digest: _Hasher = attrs.field(factory=hashlib.sha256, init=False)
    size: int = attrs.field(default=0, init=False)
    _tmp: Path = attrs.field(
        init=False,
        default=attrs.Factory(
            lambda self: self.objects_dir / f".partial-{id(self):x}", takes_self=True
        ),
    )
    _handle: BinaryIO | None = attrs.field(default=None, init=False)

    def __enter__(self) -> _BlobSink:
        self.objects_dir.mkdir(parents=True, exist_ok=True)
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
            self._tmp.replace(self.objects_dir / self.sha256)

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
