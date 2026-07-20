"""Integration tests (fake-gcs-server) for bounded-memory streaming object storage.

Exercises the real GCS path end-to-end: ``upload_stream`` writes app-provided part
bytes to temp objects that ``compose`` assembles at completion; ``download_stream``
reads back via ranged GETs; ``download_range`` over a client-side-encrypted
(chunked-AEAD) object decrypts only the covering chunks. Covers the encrypted and
plaintext routes plus a multi-part (compose-chained) upload.
"""

from collections.abc import AsyncIterator

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
async def test_gcs_streamed_encrypted_round_trip(gcs_client: GCSClient, gcs_bucket: str) -> None:
    ctx = _encrypted_ctx(gcs_client, gcs_bucket)
    spec = StorageSpec(name=gcs_bucket)
    query = ctx.storage.query(spec)
    command = ctx.storage.command(spec)

    data = b"streamed-secret-payload-" * 500

    stored = await command.upload_stream(_aiter(data), filename="s.bin", chunk_size=4096)
    assert stored.size == len(data)

    async with gcs_client.client():
        raw = (await gcs_client.download_bytes(bucket=gcs_bucket, key=stored.key)).data
    assert is_chunked_envelope(raw)
    assert data not in raw

    downloaded = await query.download_stream(stored.key)
    assert await _collect(downloaded.chunks) == data


@pytest.mark.integration
@pytest.mark.asyncio
async def test_gcs_streamed_encrypted_ranged_read(gcs_client: GCSClient, gcs_bucket: str) -> None:
    ctx = _encrypted_ctx(gcs_client, gcs_bucket)
    spec = StorageSpec(name=gcs_bucket)
    query = ctx.storage.query(spec)
    command = ctx.storage.command(spec)

    data = bytes((i * 7) % 256 for i in range(10_000))
    stored = await command.upload_stream(_aiter(data), filename="r.bin", chunk_size=1024)

    ranged = await query.download_range(stored.key, start=2500, end=5300)
    assert ranged.data == data[2500:5301]
    assert ranged.total_size == len(data)
    assert ranged.content_range == f"bytes 2500-5300/{len(data)}"

    tail = await query.download_range(stored.key, start=9990)
    assert tail.data == data[9990:]


@pytest.mark.integration
@pytest.mark.asyncio
async def test_gcs_streamed_plaintext_round_trip(gcs_client: GCSClient, gcs_bucket: str) -> None:
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
async def test_gcs_streamed_encrypted_multipart(gcs_client: GCSClient, gcs_bucket: str) -> None:
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
        before = (await gcs_client.download_bytes(bucket=gcs_bucket, key=stored.key)).data

    assert (await reencrypt_objects(storage_q, storage_c)).rewritten == 1

    async with gcs_client.client():
        after = (await gcs_client.download_bytes(bucket=gcs_bucket, key=stored.key)).data

    assert after != before  # re-sealed under a fresh data key
    assert (await storage_q.download(stored.key)).data == b"top-secret-payload"

    # The metadata envelope survived the compose round-trip.
    assert (await storage_q.download_stream(stored.key)).filename == "secret.txt"


# ....................... #
# Conditional write-back (the delete/overwrite race).
#
# GCS preconditions speak generations: the client resolves the destination's
# metadata at completion time — a vanished destination answers 404, a
# mismatched ETag refuses client-side — and threads ifGenerationMatch into the
# final compose/rewrite. fake-gcs-server was probed to IGNORE ifGenerationMatch
# server-side (it accepts a stale generation and would recreate a deleted
# destination), so these tests exercise the metadata-resolution seam that runs
# before it; the atomic server-side enforcement of the residual
# read-to-compose sub-window is real-GCS-only behavior.


@pytest.mark.integration
@pytest.mark.asyncio
async def test_gcs_reencrypt_objects_does_not_resurrect_a_delete_racing_the_write_back(
    gcs_client: GCSClient, gcs_bucket: str
) -> None:
    """Deleted AFTER the download, BEFORE the write-back: the conditional
    completion answers not_found, the sweep counts a skip, and the object
    stays gone."""

    from forze.application.contracts.storage.value_objects import UploadedObject
    from forze.application.integrations.crypto import reencrypt_objects

    ctx = _encrypted_ctx(gcs_client, gcs_bucket)
    spec = StorageSpec(name=gcs_bucket)
    storage_q = ctx.storage.query(spec)
    storage_c = ctx.storage.command(spec)

    survivor = await storage_c.upload(
        UploadedObject(filename="keep.txt", data=b"keep-me"),
    )
    victim = await storage_c.upload(
        UploadedObject(filename="gone.txt", data=b"delete-me"),
    )

    class _DeletesVictimAfterItsDownload:
        def __getattr__(self, name: str):
            return getattr(storage_c, name)

        async def overwrite_stream(self, key: str, chunks, **kwargs):
            if key != victim.key:
                return await storage_c.overwrite_stream(key, chunks, **kwargs)

            buffered = [piece async for piece in chunks]

            async with gcs_client.client():
                await gcs_client.delete_object(bucket=gcs_bucket, key=victim.key)

            async def _replay():
                for piece in buffered:
                    yield piece

            return await storage_c.overwrite_stream(key, _replay(), **kwargs)

    report = await reencrypt_objects(storage_q, _DeletesVictimAfterItsDownload())  # type: ignore[arg-type]

    assert report.rewritten == 1
    assert report.skipped_missing == 1

    assert (await storage_q.download(survivor.key)).data == b"keep-me"

    # The victim stays deleted — the write-back did not recreate it.
    async with gcs_client.client():
        assert not await gcs_client.object_exists(gcs_bucket, victim.key)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_gcs_reencrypt_objects_retries_once_when_the_object_changed_mid_rewrite(
    gcs_client: GCSClient, gcs_bucket: str
) -> None:
    """Replaced mid-rewrite: the ETag resolved at completion no longer matches the
    sweep's token, the write-back refuses as a conflict, and the sweep re-reads
    once so the concurrent writer's content survives."""

    from forze.application.contracts.storage.value_objects import UploadedObject
    from forze.application.integrations.crypto import reencrypt_objects

    ctx = _encrypted_ctx(gcs_client, gcs_bucket)
    spec = StorageSpec(name=gcs_bucket)
    storage_q = ctx.storage.query(spec)
    storage_c = ctx.storage.command(spec)

    victim = await storage_c.upload(
        UploadedObject(filename="hot.txt", data=b"sweep-read-me"),
    )
    attempts = 0

    async def _one(data: bytes):
        yield data

    class _ReplacesVictimBeforeItsFirstWriteBack:
        def __getattr__(self, name: str):
            return getattr(storage_c, name)

        async def overwrite_stream(self, key: str, chunks, **kwargs):
            nonlocal attempts

            if key != victim.key:
                return await storage_c.overwrite_stream(key, chunks, **kwargs)

            attempts += 1
            buffered = [piece async for piece in chunks]

            if attempts == 1:
                await storage_c.overwrite_stream(victim.key, _one(b"concurrent"))

            async def _replay():
                for piece in buffered:
                    yield piece

            return await storage_c.overwrite_stream(key, _replay(), **kwargs)

    report = await reencrypt_objects(
        storage_q,
        _ReplacesVictimBeforeItsFirstWriteBack(),  # type: ignore[arg-type]
    )

    assert report.rewritten == 1
    assert report.skipped_missing == 0
    assert attempts == 2

    assert (await storage_q.download(victim.key)).data == b"concurrent"


@pytest.mark.integration
@pytest.mark.asyncio
async def test_gcs_conditional_completion_refuses_a_stale_etag_client_side(
    gcs_client: GCSClient, gcs_bucket: str
) -> None:
    """The client's own ETag resolution refuses before any compose is issued —
    this is the part of the condition an emulator cannot silently skip."""

    from forze.application.contracts.storage import OVERWRITE_PRECONDITION_FAILED_CODE
    from forze.application.integrations.storage.client import ObjectStoragePartInfo
    from forze.base.exceptions import CoreException

    key = "conditional/stale.bin"

    async with gcs_client.client():
        await gcs_client.upload_bytes(bucket=gcs_bucket, key=key, data=b"current")
        head = await gcs_client.head_object(bucket=gcs_bucket, key=key)

        upload_id = await gcs_client.create_multipart_upload(bucket=gcs_bucket, key=key)
        part = await gcs_client.upload_multipart_part(
            bucket=gcs_bucket, key=key, upload_id=upload_id, part_number=1, data=b"new"
        )

        # The object changes after the caller captured its ETag.
        await gcs_client.upload_bytes(bucket=gcs_bucket, key=key, data=b"changed")

        with pytest.raises(CoreException) as ei:
            await gcs_client.complete_multipart_upload(
                bucket=gcs_bucket,
                key=key,
                upload_id=upload_id,
                parts=[ObjectStoragePartInfo(part_number=1, etag=part.etag)],
                if_match=head.etag,
            )

        assert ei.value.code == OVERWRITE_PRECONDITION_FAILED_CODE

        body = await gcs_client.download_bytes(bucket=gcs_bucket, key=key)
        assert body.data == b"changed"
