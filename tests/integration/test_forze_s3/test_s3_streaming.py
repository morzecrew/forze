"""Integration tests (MinIO) for bounded-memory streaming object storage.

Exercises the real S3 path end-to-end: ``upload_stream`` drives a native multipart
upload (app-provided ``UploadPart`` bytes, then ``CompleteMultipartUpload``),
``download_stream`` reads it back via ranged GETs, and ``download_range`` over a
client-side-encrypted (chunked-AEAD) object fetches and decrypts only the covering
chunks. Covers both the encrypted and plaintext routes and a genuine multi-part
upload (MinIO enforces the 5 MiB minimum non-final part).
"""

from typing import AsyncIterator

import pytest

from forze.application.contracts.crypto import KeyRef, StaticKeyDirectory
from forze.application.contracts.storage import StorageSpec
from forze.application.execution import CryptoDepsModule
from forze.base.crypto import is_chunked_envelope
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
