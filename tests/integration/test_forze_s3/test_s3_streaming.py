"""Integration tests (MinIO) for bounded-memory streaming object storage.

Exercises the real S3 path end-to-end: ``upload_stream`` drives a native multipart
upload (app-provided ``UploadPart`` bytes, then ``CompleteMultipartUpload``),
``download_stream`` reads it back via ranged GETs, and ``download_range`` over a
client-side-encrypted (chunked-AEAD) object fetches and decrypts only the covering
chunks. Covers both the encrypted and plaintext routes and a genuine multi-part
upload (MinIO enforces the 5 MiB minimum non-final part).
"""

from collections.abc import AsyncIterator

import pytest

from forze.application.contracts.crypto import KeyRef, StaticKeyDirectory
from forze.application.contracts.storage import StorageSpec
from forze.application.execution import CryptoDepsModule
from forze.base.crypto import (
    chunk_frame_stride,
    is_chunked_envelope,
    unpack_chunked_header,
)
from forze.base.exceptions import CoreException
from forze_mock import MockKeyManagement
from forze_s3.execution.deps import S3DepsModule
from forze_s3.execution.deps.configs import S3StorageConfig
from forze_s3.kernel.client import S3Client
from tests.support.execution_context import context_from_deps, context_from_modules

# ----------------------- #

MIB = 1024 * 1024


def _encrypted_ctx(s3_client: S3Client, bucket: str):
    return context_from_modules(
        CryptoDepsModule(
            kms=MockKeyManagement(),
            directory=StaticKeyDirectory(KeyRef(key_id="cmk")),
        ),
        S3DepsModule(
            client=s3_client,
            storages={bucket: S3StorageConfig(bucket=bucket, encrypt=True)},
        ),
    )


def _plaintext_ctx(s3_client: S3Client, bucket: str):
    return context_from_deps(
        S3DepsModule(
            client=s3_client,
            storages={bucket: S3StorageConfig(bucket=bucket)},
        )()
    )


async def _aiter(data: bytes, *, piece: int = 64 * 1024) -> AsyncIterator[bytes]:
    for i in range(0, len(data), piece):
        yield data[i : i + piece]


async def _collect(source: AsyncIterator[bytes]) -> bytes:
    out = bytearray()
    async for piece in source:
        out += piece
    return bytes(out)


# ----------------------- #


@pytest.mark.integration
@pytest.mark.asyncio
async def test_s3_streamed_encrypted_round_trip(
    s3_client: S3Client, s3_bucket: str
) -> None:
    ctx = _encrypted_ctx(s3_client, s3_bucket)
    spec = StorageSpec(name=s3_bucket)
    query = ctx.storage.query(spec)
    command = ctx.storage.command(spec)

    data = b"streamed-secret-payload-" * 500  # ~12 KB, several chunks

    stored = await command.upload_stream(
        _aiter(data), filename="s.bin", chunk_size=4096
    )

    assert stored.size == len(data)

    # The bytes at rest are chunked-AEAD ciphertext, not the plaintext.
    async with s3_client.client():
        raw = (await s3_client.download_bytes(bucket=s3_bucket, key=stored.key)).data
    assert is_chunked_envelope(raw)
    assert data not in raw

    downloaded = await query.download_stream(stored.key)
    assert await _collect(downloaded.chunks) == data


@pytest.mark.integration
@pytest.mark.asyncio
async def test_s3_streamed_encrypted_ranged_read(
    s3_client: S3Client, s3_bucket: str
) -> None:
    ctx = _encrypted_ctx(s3_client, s3_bucket)
    spec = StorageSpec(name=s3_bucket)
    query = ctx.storage.query(spec)
    command = ctx.storage.command(spec)

    data = bytes((i * 7) % 256 for i in range(10_000))
    stored = await command.upload_stream(
        _aiter(data), filename="r.bin", chunk_size=1024
    )

    # A window spanning several chunks, with mid-chunk start and end.
    ranged = await query.download_range(stored.key, start=2500, end=5300)

    assert ranged.data == data[2500:5301]
    assert ranged.total_size == len(data)
    assert ranged.content_range == f"bytes 2500-5300/{len(data)}"

    # A tail range (end=None → to EOF).
    tail = await query.download_range(stored.key, start=9990)
    assert tail.data == data[9990:]


@pytest.mark.integration
@pytest.mark.asyncio
async def test_s3_streamed_encrypted_ranged_read_rejects_truncation(
    s3_client: S3Client, s3_bucket: str
) -> None:
    """A stored object that lost its tail (truncated at a frame boundary) must be
    refused by every ranged read — the layout-derived plaintext size under-reports,
    so even a window far from the tail would serve a lie."""

    ctx = _encrypted_ctx(s3_client, s3_bucket)
    spec = StorageSpec(name=s3_bucket)
    query = ctx.storage.query(spec)
    command = ctx.storage.command(spec)

    data = bytes((i * 13) % 256 for i in range(10_000))  # 10 chunks of 1024
    stored = await command.upload_stream(
        _aiter(data), filename="t.bin", chunk_size=1024
    )

    async with s3_client.client():
        raw = (await s3_client.download_bytes(bucket=s3_bucket, key=stored.key)).data
        _header, header_len = unpack_chunked_header(raw)
        stride = chunk_frame_stride(raw, header_len)
        assert stride is not None
        # Drop the last frames, cutting exactly at a frame boundary.
        await s3_client.upload_bytes(
            s3_bucket, stored.key, raw[: header_len + 4 * stride]
        )

    with pytest.raises(CoreException) as ei:  # a range covering the apparent tail
        await query.download_range(stored.key, start=3900)
    assert ei.value.code == "core.crypto.chunked_truncated"

    with pytest.raises(CoreException) as ei:  # a range far from the tail
        await query.download_range(stored.key, start=0, end=99)
    assert ei.value.code == "core.crypto.chunked_truncated"

    with pytest.raises(CoreException) as ei:  # the streaming path still refuses too
        downloaded = await query.download_stream(stored.key)
        await _collect(downloaded.chunks)
    assert ei.value.code == "core.crypto.chunked_truncated"


@pytest.mark.integration
@pytest.mark.asyncio
async def test_s3_streamed_plaintext_round_trip(
    s3_client: S3Client, s3_bucket: str
) -> None:
    ctx = _plaintext_ctx(s3_client, s3_bucket)
    spec = StorageSpec(name=s3_bucket)
    query = ctx.storage.query(spec)
    command = ctx.storage.command(spec)

    data = b"plain-streamed-content-" * 400

    stored = await command.upload_stream(_aiter(data), filename="p.bin")
    assert stored.size == len(data)

    async with s3_client.client():
        raw = (await s3_client.download_bytes(bucket=s3_bucket, key=stored.key)).data
    assert raw == data  # plaintext at rest (no cipher on this route)

    downloaded = await query.download_stream(stored.key)
    assert await _collect(downloaded.chunks) == data


@pytest.mark.integration
@pytest.mark.asyncio
async def test_s3_streamed_encrypted_multipart(
    s3_client: S3Client, s3_bucket: str
) -> None:
    """>8 MiB forces a genuine multi-part upload: a non-final ``UploadPart`` (>= 5 MiB,
    MinIO-enforced) plus the final part, assembled by ``CompleteMultipartUpload``."""

    ctx = _encrypted_ctx(s3_client, s3_bucket)
    spec = StorageSpec(name=s3_bucket)
    query = ctx.storage.query(spec)
    command = ctx.storage.command(spec)

    data = bytes((i * 131 + 7) % 256 for i in range(12 * MIB))  # 12 MiB → 2 parts

    stored = await command.upload_stream(_aiter(data, piece=MIB), filename="big.bin")
    assert stored.size == len(data)

    downloaded = await query.download_stream(stored.key)
    assert await _collect(downloaded.chunks) == data

    # A ranged read deep into the large object returns the exact window.
    ranged = await query.download_range(stored.key, start=7 * MIB, end=7 * MIB + 99)
    assert ranged.data == data[7 * MIB : 7 * MIB + 100]


@pytest.mark.asyncio
async def test_upload_stream_actually_applies_tags(s3_client: S3Client, s3_bucket: str) -> None:
    """A streamed upload must *write* its tags, not just report them on the returned object.

    Multipart completion carries no tagging, so ``upload_stream`` used to compute a tag map and
    drop it — the returned ``StoredObject`` claimed tags the object never got. The mock stored
    them, so only a real S3 ``head`` reveals the gap.
    """

    ctx = _plaintext_ctx(s3_client, s3_bucket)
    spec = StorageSpec(name=s3_bucket)
    command = ctx.storage.command(spec)
    query = ctx.storage.query(spec)

    stored = await command.upload_stream(
        _aiter(b"tagged bytes"), filename="t.bin", tags={"kind": "invoice", "team": "billing"}
    )

    head = await query.head(stored.key, include_tags=True)
    assert dict(head.tags) == {"kind": "invoice", "team": "billing"}
