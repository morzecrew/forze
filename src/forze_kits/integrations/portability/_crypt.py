"""Optional envelope encryption for the archive at rest (RFC 0017 §9).

The archive is plaintext by construction — decrypt-on-read is what makes it portable — so a
full-system archive on disk is a **credential store**. This seals it. A single random data key
(DEK), minted per archive, encrypts every data file and blob object as an ``FZEc`` chunked-AEAD
stream (bounded memory, per-chunk nonce, AAD-bound chunk index so truncation and reordering fail
authentication). The DEK is itself **wrapped by a key-encryption key** — a KMS/CMK whose plaintext
never leaves the KMS — and the wrapped DEK rides in the manifest; import unwraps it once and
decrypts. Every frame is bound to its file's archive path through the AAD, so a frame cannot be
moved between files even under one DEK.

``migrate`` needs none of this: it is the ports-to-ports path with nothing at rest.

The KMS boundary is :class:`ArchiveSealer` (mint/unwrap through a ``KeyManagementPort`` — any wired
BYOK backend, or the mock); the symmetric half is :class:`ArchiveCipher`, which ``format.py`` drives
once the DEK is (un)wrapped.
"""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path
from typing import Protocol, cast

import attrs

from forze.application.contracts.crypto import DataKey, KeyManagementPort, KeyRef
from forze.base.crypto import (
    DEFAULT_CHUNK_SIZE,
    Aead,
    AesGcmAead,
    chunk_frame_stride,
    open_chunk,
    parse_frame,
    seal_chunk,
)
from forze.base.exceptions import exc

# ----------------------- #

_READ_CHUNK = 1 << 16
"""File-read size while reassembling frames — a frame spans at most one chunk_size + tag, so the
reader never buffers more than that whatever the read granularity."""


# ....................... #


class _Writable(Protocol):
    """The narrow inner a sealing sink writes ciphertext into — it never closes it (the caller, which
    owns the file handle, does). The hashing sink and the blob sink both satisfy it."""

    def write(self, data: bytes, /) -> int: ...


# ....................... #


@attrs.frozen(kw_only=True)
class ArchiveSealer:
    """The KEK boundary: mint + wrap a per-archive data key on export, unwrap it on import.

    Backed by a :class:`KeyManagementPort` — any wired KMS/BYOK backend (AWS/GCP/YC/Vault) or the
    mock. Export needs :attr:`key_ref` (which CMK to wrap the archive's data key under); import reads
    the ref off the manifest, so an importer's sealer can leave it unset.
    """

    kms: KeyManagementPort
    key_ref: KeyRef | None = None
    aead: Aead = attrs.field(factory=AesGcmAead)
    chunk_size: int = DEFAULT_CHUNK_SIZE

    # ....................... #

    async def mint(self) -> DataKey:
        """Mint a fresh data key and wrap it under the KEK in one KMS call."""

        if self.key_ref is None:
            raise exc.precondition(
                "An ArchiveSealer needs a key_ref (which KEK to wrap under) to encrypt an archive."
            )

        return await self.kms.generate_data_key(self.key_ref)

    # ....................... #

    async def unwrap(self, *, wrapped: bytes, key_id: str, key_version: str | None) -> bytes:
        """Recover the plaintext data key from its wrapped form, via the KEK named in the archive."""

        return await self.kms.unwrap_data_key(
            wrapped=wrapped,
            key_ref=KeyRef(key_id=key_id, version=key_version),
        )

    # ....................... #

    def cipher(self, dek: bytes) -> ArchiveCipher:
        """The symmetric cipher for a plaintext data key (freshly minted, or just unwrapped)."""

        return ArchiveCipher(dek=dek, aead=self.aead, chunk_size=self.chunk_size)


# ....................... #


@attrs.frozen(kw_only=True)
class ArchiveCipher:
    """A per-archive data key bound to an AEAD — seals/opens one file's byte stream as ``FZEc`` frames.

    Each frame is bound to the file's archive-relative path through the AAD, so a frame cannot be
    moved to another file even though every file shares the one archive data key.
    """

    dek: bytes = attrs.field(repr=False)
    aead: Aead
    chunk_size: int

    # ....................... #

    def sealing_sink(self, inner: _Writable, *, base_aad: str) -> _SealingSink:
        """A sink that seals what it is written and forwards the ciphertext to *inner*."""

        return _SealingSink(
            inner=inner,
            aead=self.aead,
            dek=self.dek,
            base_aad=base_aad.encode("utf-8"),
            chunk_size=self.chunk_size,
        )

    # ....................... #

    def open(self, path: Path, *, base_aad: str) -> Iterator[bytes]:
        """Yield the decrypted bytes of a sealed file, one frame's worth at a time."""

        return _decrypt_frames(
            path, aead=self.aead, dek=self.dek, base_aad=base_aad.encode("utf-8")
        )


# ....................... #


@attrs.define
class _SealingSink:
    """Seal bytes into ``FZEc`` frames and forward the ciphertext to an inner sink.

    Holds back up to one chunk so :meth:`close` always has a chunk to mark ``is_final`` — even when
    the plaintext is an exact multiple of ``chunk_size``, or empty. The final flag is what lets the
    reader detect truncation, so it must always be emitted. :meth:`close` does **not** close *inner*
    — the caller (the writer that owns the file handle) does.
    """

    _inner: _Writable = attrs.field(alias="inner")
    _aead: Aead = attrs.field(alias="aead")
    _dek: bytes = attrs.field(alias="dek")
    _base_aad: bytes = attrs.field(alias="base_aad")
    _chunk_size: int = attrs.field(alias="chunk_size")

    # ....................... #

    _buf: bytearray = attrs.field(factory=bytearray, init=False)
    _index: int = attrs.field(default=0, init=False)

    # ....................... #

    def write(self, data: bytes) -> int:
        self._buf.extend(data)

        # Strictly greater than a chunk, so at least one byte stays back for the final frame.
        while len(self._buf) > self._chunk_size:
            self._seal(bytes(self._buf[: self._chunk_size]), is_final=False)
            del self._buf[: self._chunk_size]

        return len(data)

    # ....................... #

    def flush(self) -> None:
        # A compressor may flush its fileobj; there is nothing to force here — a partial chunk waits
        # for more bytes or for close() to seal it as the final frame.
        pass

    # ....................... #

    def _seal(self, chunk: bytes, *, is_final: bool) -> None:
        self._inner.write(
            seal_chunk(
                self._aead,
                key=self._dek,
                base_aad=self._base_aad,
                index=self._index,
                is_final=is_final,
                plaintext=chunk,
            )
        )
        self._index += 1

    # ....................... #

    def close(self) -> None:
        self._seal(bytes(self._buf), is_final=True)
        self._buf.clear()


# ....................... #


def _decrypt_frames(path: Path, *, aead: Aead, dek: bytes, base_aad: bytes) -> Iterator[bytes]:
    """Yield the decrypted bytes of an ``FZEc``-sealed file, streaming and bounded-memory.

    A file whose stream ends before an ``is_final`` frame is a truncated archive and raises rather
    than returning a short read — the reader never treats a cut-off stream as complete.
    """

    buf = bytearray()
    index = 0
    final_seen = False

    with path.open("rb") as handle:
        while not final_seen:
            data = handle.read(_READ_CHUNK)

            if data:
                buf.extend(data)

            while True:
                view = cast("bytes", buf)  # runtime-safe: the parsers only read, never mutate
                stride = chunk_frame_stride(view, 0)

                if stride is None or stride > len(buf):
                    break

                frame, end = parse_frame(view, 0)
                del buf[:end]

                yield open_chunk(aead, key=dek, base_aad=base_aad, index=index, frame=frame)
                index += 1

                if frame.is_final:
                    final_seen = True
                    break

            if not data:
                break

    if not final_seen:
        raise exc.validation(
            f"Encrypted archive file {path.name} is truncated — its stream has no final chunk.",
            code="core.crypto.chunked_truncated",
        )
