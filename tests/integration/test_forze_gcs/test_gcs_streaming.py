"""Integration tests (fake-gcs-server) for bounded-memory streaming object storage.

Exercises the real GCS path end-to-end: ``upload_stream`` writes app-provided part
bytes to temp objects that ``compose`` assembles at completion; ``download_stream``
reads back via ranged GETs; ``download_range`` over a client-side-encrypted
(chunked-AEAD) object decrypts only the covering chunks. Covers the encrypted and
plaintext routes plus a multi-part (compose-chained) upload.
"""

from typing import AsyncIterator

import pytest

from forze.application.contracts.crypto import KeyRef, StaticKeyDirectory
from forze.application.contracts.storage import StorageSpec
from forze.application.execution import CryptoDepsModule
from forze.base.crypto import is_chunked_envelope
from forze_gcs.execution.deps.configs import GCSStorageConfig
from forze_gcs.execution.deps.module import GCSDepsModule
from forze_gcs.kernel.client.client import GCSClient
from forze_mock import MockKeyManagement
from tests.support.execution_context import context_from_deps, context_from_modules

# ----------------------- #

MIB = 1024 * 1024


def _encrypted_ctx(gcs_client: GCSClient, bucket: str):
    return context_from_modules(
        CryptoDepsModule(
            kms=MockKeyManagement(),
            directory=StaticKeyDirectory(KeyRef(key_id="cmk")),
        ),
        GCSDepsModule(
            client=gcs_client,
            storages={bucket: GCSStorageConfig(bucket=bucket, encrypt=True)},
        ),
    )


def _plaintext_ctx(gcs_client: GCSClient, bucket: str):
    return context_from_deps(
        GCSDepsModule(
            client=gcs_client,
            storages={bucket: GCSStorageConfig(bucket=bucket)},
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
async def test_gcs_streamed_encrypted_round_trip(
    gcs_client: GCSClient, gcs_bucket: str
) -> None:
    ctx = _encrypted_ctx(gcs_client, gcs_bucket)
    spec = StorageSpec(name=gcs_bucket)
    query = ctx.storage.query(spec)
    command = ctx.storage.command(spec)

    data = b"streamed-secret-payload-" * 500

    stored = await command.upload_stream(
        _aiter(data), filename="s.bin", chunk_size=4096
    )
    assert stored.size == len(data)

    async with gcs_client.client():
        raw = (await gcs_client.download_bytes(bucket=gcs_bucket, key=stored.key)).data
    assert is_chunked_envelope(raw)
    assert data not in raw

    downloaded = await query.download_stream(stored.key)
    assert await _collect(downloaded.chunks) == data


@pytest.mark.integration
@pytest.mark.asyncio
async def test_gcs_streamed_encrypted_ranged_read(
    gcs_client: GCSClient, gcs_bucket: str
) -> None:
    ctx = _encrypted_ctx(gcs_client, gcs_bucket)
    spec = StorageSpec(name=gcs_bucket)
    query = ctx.storage.query(spec)
    command = ctx.storage.command(spec)

    data = bytes((i * 7) % 256 for i in range(10_000))
    stored = await command.upload_stream(
        _aiter(data), filename="r.bin", chunk_size=1024
    )

    ranged = await query.download_range(stored.key, start=2500, end=5300)
    assert ranged.data == data[2500:5301]
    assert ranged.total_size == len(data)
    assert ranged.content_range == f"bytes 2500-5300/{len(data)}"

    tail = await query.download_range(stored.key, start=9990)
    assert tail.data == data[9990:]


@pytest.mark.integration
@pytest.mark.asyncio
async def test_gcs_streamed_plaintext_round_trip(
    gcs_client: GCSClient, gcs_bucket: str
) -> None:
    ctx = _plaintext_ctx(gcs_client, gcs_bucket)
    spec = StorageSpec(name=gcs_bucket)
    query = ctx.storage.query(spec)
    command = ctx.storage.command(spec)

    data = b"plain-streamed-content-" * 400

    stored = await command.upload_stream(_aiter(data), filename="p.bin")
    assert stored.size == len(data)

    async with gcs_client.client():
        raw = (await gcs_client.download_bytes(bucket=gcs_bucket, key=stored.key)).data
    assert raw == data

    downloaded = await query.download_stream(stored.key)
    assert await _collect(downloaded.chunks) == data


@pytest.mark.integration
@pytest.mark.asyncio
async def test_gcs_streamed_encrypted_multipart(
    gcs_client: GCSClient, gcs_bucket: str
) -> None:
    """>8 MiB forces multiple temp parts, assembled by ``compose`` at completion."""

    ctx = _encrypted_ctx(gcs_client, gcs_bucket)
    spec = StorageSpec(name=gcs_bucket)
    query = ctx.storage.query(spec)
    command = ctx.storage.command(spec)

    data = bytes((i * 131 + 7) % 256 for i in range(12 * MIB))

    stored = await command.upload_stream(_aiter(data, piece=MIB), filename="big.bin")
    assert stored.size == len(data)

    downloaded = await query.download_stream(stored.key)
    assert await _collect(downloaded.chunks) == data

    ranged = await query.download_range(stored.key, start=7 * MIB, end=7 * MIB + 99)
    assert ranged.data == data[7 * MIB : 7 * MIB + 100]


# ....................... #


@pytest.mark.integration
@pytest.mark.asyncio
async def test_gcs_reencrypt_objects_reseals_in_place_preserving_metadata(
    gcs_client: GCSClient, gcs_bucket: str
) -> None:
    """The blob break-glass sweep on GCS: same key, same plaintext, FRESH envelope.

    GCS has no native multipart session — the object is composed and its user metadata
    stamped on the destination — so this pins that path independently of S3's.
    """

    from forze.application.contracts.storage.value_objects import UploadedObject
    from forze.application.integrations.crypto import reencrypt_objects

    ctx = _encrypted_ctx(gcs_client, gcs_bucket)
    spec = StorageSpec(name=gcs_bucket)
    storage_q = ctx.storage.query(spec)
    storage_c = ctx.storage.command(spec)

    # `upload()` persists the metadata envelope (filename/description).
    stored = await storage_c.upload(
        UploadedObject(filename="secret.txt", data=b"top-secret-payload"),
    )

    async with gcs_client.client():
        before = (
            await gcs_client.download_bytes(bucket=gcs_bucket, key=stored.key)
        ).data

    assert (await reencrypt_objects(storage_q, storage_c)).rewritten == 1

    async with gcs_client.client():
        after = (
            await gcs_client.download_bytes(bucket=gcs_bucket, key=stored.key)
        ).data

    assert after != before  # re-sealed under a fresh data key
    assert (await storage_q.download(stored.key)).data == b"top-secret-payload"

    # The metadata envelope survived the compose round-trip.
    assert (await storage_q.download_stream(stored.key)).filename == "secret.txt"
