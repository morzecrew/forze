"""End-to-end tests for streaming object storage (upload_stream / download_stream).

Drives the base :class:`ObjectStorageAdapter` against an in-memory fake client, so
the full orchestration is exercised: chunked encryption, part buffering, the
multipart lifecycle, ranged-GET download + decrypt, whole-payload back-compat, and
tenant key-scoping.
"""

from __future__ import annotations

import contextlib
from collections.abc import AsyncIterator
from uuid import uuid4

import attrs
import pytest

from forze.application.contracts.crypto import (
    AesGcmAead,
    KeyRef,
    StaticKeyDirectory,
)
from forze.application.contracts.tenancy import TenantIdentity
from forze.application.integrations.crypto import Keyring
from forze.application.integrations.storage.adapter import ObjectStorageAdapter
from forze.application.integrations.storage.client import (
    ObjectBody,
    ObjectStorageHead,
    ObjectStoragePartInfo,
)
from forze.base.crypto import is_chunked_envelope, is_envelope
from forze.base.exceptions import CoreException
from forze_mock import MockKeyManagement

# ----------------------- #


class _FakeClient:
    """Minimal in-memory ObjectStorageClientPort covering the streaming paths."""

    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}
        self.content_types: dict[str, str] = {}
        self.sessions: dict[str, dict[int, bytes]] = {}
        self.part_sizes: list[int] = []
        self.aborted: list[str] = []
        self.range_reads: list[tuple[int, int | None]] = []

    def client(self):  # type: ignore[no-untyped-def]
        @contextlib.asynccontextmanager
        async def _cm() -> AsyncIterator[object]:
            yield self

        return _cm()

    async def ensure_bucket(self, bucket: str) -> None:
        return None

    async def upload_bytes(
        self,
        bucket: str,
        key: str,
        data: bytes,
        *,
        content_type: str | None = None,
        metadata: dict[str, str] | None = None,
        tags: dict[str, str] | None = None,
        sse: object | None = None,
    ) -> None:
        self.objects[key] = bytes(data)
        self.content_types[key] = content_type or "application/octet-stream"

    async def download_bytes(self, bucket: str, key: str) -> ObjectBody:
        return ObjectBody(
            data=self.objects[key],
            content_type=self.content_types.get(key, "application/octet-stream"),
        )

    async def head_object(
        self, bucket: str, key: str, *, include_tags: bool = False
    ) -> ObjectStorageHead:
        data = self.objects[key]
        return ObjectStorageHead(
            content_type=self.content_types.get(key, "application/octet-stream"),
            size=len(data),
        )

    async def download_range_bytes(
        self, bucket: str, key: str, *, start: int, end: int | None = None
    ) -> tuple[ObjectBody, str, int]:
        self.range_reads.append((start, end))
        data = self.objects[key]
        total = len(data)
        last = total - 1 if end is None else min(end, total - 1)
        chunk = data[start : last + 1]
        return (
            ObjectBody(
                data=chunk,
                content_type=self.content_types.get(key, "application/octet-stream"),
            ),
            f"bytes {start}-{start + len(chunk) - 1}/{total}",
            total,
        )

    async def create_multipart_upload(
        self,
        bucket: str,
        key: str,
        *,
        content_type: str | None = None,
        sse: object | None = None,
    ) -> str:
        upload_id = f"upl-{len(self.sessions)}-{key}"
        self.sessions[upload_id] = {}
        self.content_types[key] = content_type or "application/octet-stream"
        return upload_id

    async def upload_multipart_part(
        self,
        bucket: str,
        key: str,
        *,
        upload_id: str,
        part_number: int,
        data: bytes,
        sse: object | None = None,
    ) -> ObjectStoragePartInfo:
        self.part_sizes.append(len(data))
        self.sessions[upload_id][part_number] = bytes(data)
        return ObjectStoragePartInfo(
            part_number=part_number, etag=f"e{part_number}", size=len(data)
        )

    async def complete_multipart_upload(
        self,
        bucket: str,
        key: str,
        *,
        upload_id: str,
        parts: object,
        content_type: str | None = None,
        sse: object | None = None,
    ) -> None:
        deposited = self.sessions.pop(upload_id)
        blob = b"".join(deposited[n] for n in sorted(deposited))
        self.objects[key] = blob

    async def abort_multipart_upload(
        self, bucket: str, key: str, *, upload_id: str
    ) -> None:
        self.aborted.append(upload_id)
        self.sessions.pop(upload_id, None)


# ....................... #


async def _resolve_bucket(spec: object, tenant_id: object) -> str:
    return "bucket"


def _keyring() -> Keyring:
    return Keyring(
        kms=MockKeyManagement(),
        aead=AesGcmAead(),
        directory=StaticKeyDirectory(KeyRef(key_id="cmk")),
    )


def _adapter(
    client: _FakeClient,
    *,
    cipher: Keyring | None = None,
    stream_part_size: int = 1024,
    tenant_aware: bool = False,
    tenant_provider=lambda: None,  # type: ignore[no-untyped-def]
) -> ObjectStorageAdapter:
    return ObjectStorageAdapter(
        client=client,  # type: ignore[arg-type]
        bucket_spec="bucket",
        resolve_bucket=_resolve_bucket,
        cipher=cipher,
        stream_part_size=stream_part_size,
        tenant_aware=tenant_aware,
        tenant_provider=tenant_provider,
    )


async def _aiter(data: bytes, *, piece: int = 100) -> AsyncIterator[bytes]:
    for i in range(0, len(data), piece):
        yield data[i : i + piece]


async def _collect(source: AsyncIterator[bytes]) -> bytes:
    out = bytearray()
    async for piece in source:
        out += piece
    return bytes(out)


# ....................... #


@pytest.mark.parametrize("size", [0, 500, 1024, 2048, 5000])
async def test_plaintext_stream_round_trip(size: int) -> None:
    client = _FakeClient()
    adapter = _adapter(client)
    data = bytes((i * 3) % 256 for i in range(size))

    stored = await adapter.upload_stream(_aiter(data), filename="f.bin")

    assert stored.size == size
    assert client.objects[stored.key] == data  # plaintext at rest (no cipher)

    dl = await adapter.download_stream(stored.key)
    assert await _collect(dl.chunks) == data


@pytest.mark.parametrize("size", [0, 500, 4096, 20000])
async def test_encrypted_stream_round_trip(size: int) -> None:
    client = _FakeClient()
    adapter = _adapter(client, cipher=_keyring())
    data = bytes((i * 7) % 256 for i in range(size))

    stored = await adapter.upload_stream(_aiter(data), filename="f.bin", chunk_size=256)

    assert stored.size == size  # logical (plaintext) size
    at_rest = client.objects[stored.key]
    assert is_chunked_envelope(at_rest)  # chunked-AEAD format on the wire
    assert data not in at_rest if data else True  # ciphertext, not plaintext

    dl = await adapter.download_stream(stored.key)
    assert await _collect(dl.chunks) == data


async def test_upload_stream_buffers_bounded_parts() -> None:
    """Non-final parts are exactly stream_part_size; only the last is smaller."""

    client = _FakeClient()
    adapter = _adapter(client, stream_part_size=1024)

    await adapter.upload_stream(_aiter(b"x" * 5000, piece=333), filename="f.bin")

    assert len(client.part_sizes) >= 2
    assert all(p == 1024 for p in client.part_sizes[:-1])  # non-final parts full
    assert client.part_sizes[-1] <= 1024
    assert sum(client.part_sizes) == 5000


async def test_download_stream_reads_object_with_ranged_gets() -> None:
    """The download issues bounded ranged GETs rather than one whole-object read."""

    client = _FakeClient()
    adapter = _adapter(client, stream_part_size=1000)
    data = b"y" * 3500
    stored = await adapter.upload_stream(_aiter(data), filename="f.bin")

    dl = await adapter.download_stream(stored.key)
    pieces = [p async for p in dl.chunks]

    assert b"".join(pieces) == data
    assert all(len(p) <= 1000 for p in pieces)  # each range bounded by part size


async def test_download_stream_back_compat_whole_payload_envelope() -> None:
    """A legacy whole-payload (FZEv) encrypted object still streams (buffered decrypt)."""

    client = _FakeClient()
    ring = _keyring()
    adapter = _adapter(client, cipher=ring)

    # Write via the whole-payload path (upload → cipher.encrypt → upload_bytes).
    from forze.application.contracts.storage import UploadedObject

    stored = await adapter.upload(
        UploadedObject(filename="legacy.bin", data=b"legacy secret payload")
    )
    assert is_envelope(client.objects[stored.key])  # FZEv whole-payload at rest

    dl = await adapter.download_stream(stored.key)
    assert await _collect(dl.chunks) == b"legacy secret payload"


async def test_download_stream_plaintext_migration_tolerance() -> None:
    """An unencrypted object on an encrypting route streams straight through."""

    client = _FakeClient()
    adapter = _adapter(client, cipher=_keyring())
    client.objects["raw/key"] = b"not encrypted at all"
    client.content_types["raw/key"] = "text/plain"

    dl = await adapter.download_stream("raw/key")
    assert await _collect(dl.chunks) == b"not encrypted at all"


async def test_stream_tenant_isolation_on_download_key() -> None:
    """A streamed object's key is tenant-prefixed; a cross-tenant key is refused."""

    client = _FakeClient()
    tenant = TenantIdentity(tenant_id=uuid4())
    adapter = _adapter(
        client, tenant_aware=True, tenant_provider=lambda: tenant
    )

    stored = await adapter.upload_stream(_aiter(b"z" * 200), filename="f.bin")
    assert stored.key.startswith(f"tenant_{tenant.tenant_id}/")

    with pytest.raises(CoreException) as ei:
        await adapter.download_stream("tenant_00000000-0000-0000-0000-0000deadbeef/x")
    assert ei.value.code == "core.storage.key_outside_tenant"


async def test_upload_stream_aborts_multipart_on_error() -> None:
    """A failure mid-stream aborts the multipart upload (no orphaned session)."""

    client = _FakeClient()
    adapter = _adapter(client, stream_part_size=256)

    async def _boom() -> AsyncIterator[bytes]:
        yield b"a" * 300
        raise RuntimeError("stream failed")

    with pytest.raises(RuntimeError):
        await adapter.upload_stream(_boom(), filename="f.bin")

    assert client.aborted  # abort_multipart_upload was called
    assert not client.sessions  # no dangling session


async def test_streaming_cipher_required_when_encrypting() -> None:
    """A route wired with a non-streaming cipher rejects streaming with a clear error."""

    @attrs.define
    class _WholeOnlyCipher:
        async def encrypt(self, plaintext, *, tenant, aad=b""):  # type: ignore[no-untyped-def]
            return plaintext

        async def decrypt(self, blob, *, aad=b"", tenant=None):  # type: ignore[no-untyped-def]
            return blob

    client = _FakeClient()
    adapter = _adapter(client, cipher=_WholeOnlyCipher())  # type: ignore[arg-type]

    with pytest.raises(CoreException) as ei:
        await adapter.upload_stream(_aiter(b"data"), filename="f.bin")
    assert ei.value.code == "core.storage.streaming_cipher_required"


# ....................... #
# ranged reads over encrypted (chunked) objects (Phase 5)


@pytest.mark.parametrize(
    "start,end",
    [(0, 9), (5, 40), (0, None), (63, 66), (100, 100), (0, 299), (298, None)],
)
async def test_encrypted_ranged_read_matches_full(start: int, end: int | None) -> None:
    client = _FakeClient()
    adapter = _adapter(client, cipher=_keyring(), stream_part_size=1024)
    data = bytes((i * 13) % 256 for i in range(300))
    stored = await adapter.upload_stream(_aiter(data), filename="f.bin", chunk_size=32)

    rd = await adapter.download_range(stored.key, start=start, end=end)

    end_byte = len(data) - 1 if end is None else min(end, len(data) - 1)
    assert rd.data == data[start : end_byte + 1]
    assert rd.total_size == len(data)
    assert rd.content_range == f"bytes {start}-{end_byte}/{len(data)}"


async def test_encrypted_ranged_read_unsatisfiable_is_416() -> None:
    client = _FakeClient()
    adapter = _adapter(client, cipher=_keyring(), stream_part_size=1024)
    stored = await adapter.upload_stream(_aiter(b"x" * 50), filename="f.bin", chunk_size=32)

    with pytest.raises(CoreException) as ei:
        await adapter.download_range(stored.key, start=100)
    assert ei.value.code == "range_not_satisfiable"


async def test_encrypted_ranged_read_only_fetches_covering_chunks() -> None:
    client = _FakeClient()
    adapter = _adapter(client, cipher=_keyring(), stream_part_size=1 << 20)
    data = b"y" * 1000
    stored = await adapter.upload_stream(_aiter(data), filename="f.bin", chunk_size=100)

    client.range_reads.clear()
    rd = await adapter.download_range(stored.key, start=250, end=350)  # chunks 2,3 of 10

    assert rd.data == data[250:351]
    # probe + last-chunk (for total) + the two covering chunks — not all ten.
    assert len(client.range_reads) <= 5


async def test_ranged_read_refuses_whole_payload_encrypted() -> None:
    from forze.application.contracts.storage import UploadedObject

    client = _FakeClient()
    adapter = _adapter(client, cipher=_keyring())
    stored = await adapter.upload(
        UploadedObject(filename="a.bin", data=b"whole payload secret")
    )

    with pytest.raises(CoreException) as ei:
        await adapter.download_range(stored.key, start=0, end=3)
    assert ei.value.code == "core.storage.range_whole_payload_unsupported"


async def test_ranged_read_plaintext_passthrough_on_encrypting_route() -> None:
    client = _FakeClient()
    adapter = _adapter(client, cipher=_keyring())
    client.objects["raw/k"] = b"plain data here"
    client.content_types["raw/k"] = "text/plain"

    rd = await adapter.download_range("raw/k", start=6, end=9)
    assert rd.data == b"data"


async def test_encrypted_ranged_read_detects_tamper() -> None:
    client = _FakeClient()
    adapter = _adapter(client, cipher=_keyring(), stream_part_size=1 << 20)
    data = b"z" * 200
    stored = await adapter.upload_stream(_aiter(data), filename="f.bin", chunk_size=64)

    blob = bytearray(client.objects[stored.key])
    blob[-1] ^= 0x01  # corrupt the final chunk's tag
    client.objects[stored.key] = bytes(blob)

    with pytest.raises(CoreException):
        await adapter.download_range(stored.key, start=0, end=10)


# ....................... #
# ranged reads must reject a truncated / spliced chunked object


def _chunk_layout(blob: bytes) -> tuple[int, int]:
    """Return ``(header_len, stride)`` of a stored chunked object."""

    from forze.base.crypto import chunk_frame_stride, unpack_chunked_header

    _header, header_len = unpack_chunked_header(blob)
    stride = chunk_frame_stride(blob, header_len)
    assert stride is not None
    return header_len, stride


async def _truncated_chunked_object(
    client: _FakeClient, adapter: ObjectStorageAdapter, *, keep_frames: int
) -> str:
    """Store a 5-chunk encrypted object, then drop everything past *keep_frames*."""

    data = bytes((i * 11) % 256 for i in range(300))  # 5 chunks of 64 (last short)
    stored = await adapter.upload_stream(_aiter(data), filename="f.bin", chunk_size=64)

    blob = client.objects[stored.key]
    header_len, stride = _chunk_layout(blob)
    client.objects[stored.key] = blob[: header_len + keep_frames * stride]

    return stored.key


async def test_encrypted_ranged_read_detects_boundary_truncation() -> None:
    """A tail range over an object truncated at a frame boundary must not be served."""

    client = _FakeClient()
    adapter = _adapter(client, cipher=_keyring(), stream_part_size=1 << 20)
    key = await _truncated_chunked_object(client, adapter, keep_frames=2)

    with pytest.raises(CoreException) as ei:
        await adapter.download_range(key, start=100)
    assert ei.value.code == "core.crypto.chunked_truncated"


async def test_encrypted_ranged_read_detects_truncation_outside_window() -> None:
    """Even a range that never touches the tail must reject a truncated object: the
    layout-derived plaintext size is a lie, so no range over it is trustworthy."""

    client = _FakeClient()
    adapter = _adapter(client, cipher=_keyring(), stream_part_size=1 << 20)
    key = await _truncated_chunked_object(client, adapter, keep_frames=2)

    with pytest.raises(CoreException) as ei:
        await adapter.download_range(key, start=0, end=10)  # first chunk only
    assert ei.value.code == "core.crypto.chunked_truncated"


async def test_encrypted_ranged_read_detects_midframe_truncation() -> None:
    client = _FakeClient()
    adapter = _adapter(client, cipher=_keyring(), stream_part_size=1 << 20)
    data = bytes((i * 11) % 256 for i in range(300))
    stored = await adapter.upload_stream(_aiter(data), filename="f.bin", chunk_size=64)

    blob = client.objects[stored.key]
    header_len, stride = _chunk_layout(blob)
    client.objects[stored.key] = blob[: header_len + 2 * stride + 7]  # cut mid-frame

    with pytest.raises(CoreException) as ei:
        await adapter.download_range(stored.key, start=0, end=10)
    assert ei.value.code == "core.crypto.chunked_truncated"


async def test_encrypted_ranged_read_detects_header_only_truncation() -> None:
    """An object cut back to just its header (zero frames) is truncated, not empty."""

    client = _FakeClient()
    adapter = _adapter(client, cipher=_keyring(), stream_part_size=1 << 20)
    key = await _truncated_chunked_object(client, adapter, keep_frames=0)

    with pytest.raises(CoreException) as ei:
        await adapter.download_range(key, start=0, end=10)
    assert ei.value.code == "core.crypto.chunked_truncated"


async def test_encrypted_ranged_read_rejects_early_final_frame() -> None:
    """An authentic final frame *before* the layout's last frame (a splice) is
    rejected, mirroring the streaming path's trailing-data refusal."""

    from forze.application.contracts.crypto import KeyRef as _KeyRef
    from forze.base.crypto import ChunkedHeader, pack_chunked_header, seal_chunk

    client = _FakeClient()
    ring = _keyring()
    adapter = _adapter(client, cipher=ring, stream_part_size=1 << 20)

    key = "crafted/spliced"
    aad = adapter._encryption_aad("bucket", key)
    data_key = await ring.kms.generate_data_key(_KeyRef(key_id="cmk"))

    header = pack_chunked_header(
        ChunkedHeader(
            alg=ring.aead.algorithm,
            key_id=data_key.key_id,
            key_version=data_key.key_version,
            wrapped_dek=data_key.wrapped,
            chunk_size=64,
        )
    )
    # Frame 1 is honestly sealed as final, yet frames keep coming after it — the
    # last stored frame (3) also carries the flag, so sizing alone looks clean.
    flags = [False, True, False, True]
    frames = [
        seal_chunk(
            ring.aead,
            key=data_key.plaintext,
            base_aad=aad,
            index=i,
            is_final=flag,
            plaintext=bytes([i]) * 64,
        )
        for i, flag in enumerate(flags)
    ]
    client.objects[key] = header + b"".join(frames)
    client.content_types[key] = "application/octet-stream"

    with pytest.raises(CoreException) as ei:
        await adapter.download_range(key, start=64, end=100)  # covers frame 1
    assert ei.value.code == "core.crypto.chunked_trailing_data"


async def test_download_and_download_stream_still_detect_truncation() -> None:
    """The full-object paths' truncation refusal is unchanged by the ranged fix."""

    client = _FakeClient()
    adapter = _adapter(client, cipher=_keyring(), stream_part_size=1 << 20)
    key = await _truncated_chunked_object(client, adapter, keep_frames=2)

    with pytest.raises(CoreException) as ei:
        await adapter.download(key)
    assert ei.value.code == "core.crypto.chunked_truncated"

    with pytest.raises(CoreException) as ei:
        dl = await adapter.download_stream(key)
        await _collect(dl.chunks)
    assert ei.value.code == "core.crypto.chunked_truncated"


# ....................... #
# whole-object download of a streamed (chunked) object (review fix)


async def test_download_decrypts_chunked_object_from_upload_stream() -> None:
    """A non-ranged download() must decrypt an object written via upload_stream (FZEc),
    not return raw ciphertext."""

    client = _FakeClient()
    adapter = _adapter(client, cipher=_keyring(), stream_part_size=1 << 20)
    data = b"streamed-then-fully-downloaded-" * 40

    stored = await adapter.upload_stream(_aiter(data), filename="f.bin", chunk_size=256)

    downloaded = await adapter.download(stored.key)
    assert downloaded.data == data


async def test_download_if_changed_decrypts_chunked_object() -> None:
    client = _FakeClient()
    adapter = _adapter(client, cipher=_keyring(), stream_part_size=1 << 20)
    data = b"conditional-streamed-body-" * 30

    stored = await adapter.upload_stream(_aiter(data), filename="f.bin", chunk_size=256)

    # The fake client returns the stored body for a changed conditional GET.
    client.download_bytes_conditional = _conditional_body(client, stored.key)

    result = await adapter.download_if_changed(stored.key, if_none_match="stale")
    assert result is not None
    assert result.data == data


def _conditional_body(client: _FakeClient, key: str):  # type: ignore[name-defined]
    async def _download_bytes_conditional(
        bucket: str, key: str, *, if_none_match=None, if_modified_since=None
    ):
        return ObjectBody(data=client.objects[key], content_type="application/octet-stream")

    return _download_bytes_conditional
