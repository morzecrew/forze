"""Integration test: S3 client-side encryption end-to-end (MinIO + keyring)."""

import pytest

from forze.application.contracts.crypto import KeyRef, StaticKeyDirectory
from forze.application.contracts.storage import StorageSpec
from forze.application.contracts.storage.value_objects import UploadedObject
from forze.application.execution import CryptoDepsModule
from forze.base.crypto import is_envelope
from forze_mock import MockKeyManagement
from forze_s3.execution.deps import S3DepsModule
from forze_s3.execution.deps.configs import S3StorageConfig
from tests.support.execution_context import context_from_modules

# ----------------------- #


@pytest.mark.integration
@pytest.mark.asyncio
async def test_s3_encrypted_upload_is_ciphertext_at_rest(s3_client, s3_bucket: str) -> None:
    ctx = context_from_modules(
        CryptoDepsModule(
            kms=MockKeyManagement(),
            directory=StaticKeyDirectory(KeyRef(key_id="cmk")),
        ),
        S3DepsModule(
            client=s3_client,
            storages={s3_bucket: S3StorageConfig(bucket=s3_bucket, encrypt=True)},
        ),
    )
    spec = StorageSpec(name=s3_bucket)
    storage_q = ctx.storage.query(spec)
    storage_c = ctx.storage.command(spec)

    stored = await storage_c.upload(
        UploadedObject(filename="secret.txt", data=b"top-secret-payload"),
    )

    # Raw object in the bucket is an envelope, not the plaintext.
    async with s3_client.client():
        raw = (await s3_client.download_bytes(bucket=s3_bucket, key=stored.key)).data

    assert raw != b"top-secret-payload"
    assert is_envelope(raw)

    # The adapter transparently decrypts on download.
    downloaded = await storage_q.download(stored.key)
    assert downloaded.data == b"top-secret-payload"


# ....................... #


@pytest.mark.integration
@pytest.mark.asyncio
async def test_reencrypt_objects_reseals_in_place_preserving_metadata(
    s3_client, s3_bucket: str
) -> None:
    """The blob break-glass sweep: same key, same plaintext, FRESH envelope."""

    from forze.application.integrations.crypto import reencrypt_objects

    ctx = context_from_modules(
        CryptoDepsModule(
            kms=MockKeyManagement(),
            directory=StaticKeyDirectory(KeyRef(key_id="cmk")),
        ),
        S3DepsModule(
            client=s3_client,
            storages={s3_bucket: S3StorageConfig(bucket=s3_bucket, encrypt=True)},
        ),
    )
    spec = StorageSpec(name=s3_bucket)
    storage_q = ctx.storage.query(spec)
    storage_c = ctx.storage.command(spec)

    # Uploaded via `upload()`, which persists a metadata envelope (filename/description).
    stored = await storage_c.upload(
        UploadedObject(
            filename="secret.txt",
            data=b"top-secret-payload",
            description="a note",
        ),
    )

    async with s3_client.client():
        before = (await s3_client.download_bytes(bucket=s3_bucket, key=stored.key)).data

    report = await reencrypt_objects(storage_q, storage_c)
    assert report.rewritten == 1

    async with s3_client.client():
        after = (await s3_client.download_bytes(bucket=s3_bucket, key=stored.key)).data

    # Still sealed, but under a FRESH data key — the whole point of the sweep.
    assert is_envelope(after) or after.startswith(b"FZEc")
    assert after != before

    # Same key, same plaintext.
    assert (await storage_q.download(stored.key)).data == b"top-secret-payload"

    # ...and the metadata envelope survived the multipart round-trip: a streamed write
    # could not carry metadata before this, which would have silently dropped the
    # filename off every object the sweep touched.
    head = await storage_q.head(stored.key)
    assert head.metadata  # the filename/description envelope is still there
    assert (await storage_q.download_stream(stored.key)).filename == "secret.txt"

    # The write result must report the description the object still carries — a caller
    # refreshing an index or cache from it would otherwise wipe the description.
    rewritten = await storage_c.overwrite_stream(
        stored.key,
        (await storage_q.download_stream(stored.key)).chunks,
        content_type=head.content_type,
        metadata=head.metadata,
    )
    assert rewritten.filename == "secret.txt"
    assert rewritten.description == "a note"


# ....................... #


@pytest.mark.integration
@pytest.mark.asyncio
async def test_reencrypt_objects_skips_an_object_deleted_mid_sweep(
    s3_client, s3_bucket: str
) -> None:
    """A churning bucket still finishes a pass: a real-S3 miss is a skip, not an abort."""

    from forze.application.integrations.crypto import reencrypt_objects

    ctx = context_from_modules(
        CryptoDepsModule(
            kms=MockKeyManagement(),
            directory=StaticKeyDirectory(KeyRef(key_id="cmk")),
        ),
        S3DepsModule(
            client=s3_client,
            storages={s3_bucket: S3StorageConfig(bucket=s3_bucket, encrypt=True)},
        ),
    )
    spec = StorageSpec(name=s3_bucket)
    storage_q = ctx.storage.query(spec)
    storage_c = ctx.storage.command(spec)

    survivor = await storage_c.upload(
        UploadedObject(filename="keep.txt", data=b"keep-me"),
    )
    victim = await storage_c.upload(
        UploadedObject(filename="gone.txt", data=b"delete-me"),
    )

    class _DeletesVictimOnFirstTouch:
        """Query-port wrapper: the victim vanishes between listing and its rewrite."""

        def __getattr__(self, name: str):
            return getattr(storage_q, name)

        async def head(self, key: str, *args, **kwargs):
            if key == victim.key:
                async with s3_client.client():
                    await s3_client.delete_object(bucket=s3_bucket, key=victim.key)

            return await storage_q.head(key, *args, **kwargs)

    report = await reencrypt_objects(_DeletesVictimOnFirstTouch(), storage_c)  # type: ignore[arg-type]

    assert report.rewritten == 1
    assert report.skipped_missing == 1

    # The survivor round-trips and the victim stays deleted (no resurrection).
    assert (await storage_q.download(survivor.key)).data == b"keep-me"

    async with s3_client.client():
        assert not await s3_client.object_exists(bucket=s3_bucket, key=victim.key)


# ....................... #


@pytest.mark.integration
@pytest.mark.asyncio
async def test_reencrypt_objects_does_not_resurrect_a_delete_racing_the_write_back(
    s3_client, s3_bucket: str
) -> None:
    """The resurrection window: deleted AFTER the download, BEFORE the write-back.

    The earlier not-found skip cannot catch this — an unconditional overwrite
    *succeeds* and silently undoes the delete. The sweep's ETag-conditional
    write-back must fail not_found instead, count the object as skipped, and
    leave it deleted.
    """

    from forze.application.integrations.crypto import reencrypt_objects

    ctx = context_from_modules(
        CryptoDepsModule(
            kms=MockKeyManagement(),
            directory=StaticKeyDirectory(KeyRef(key_id="cmk")),
        ),
        S3DepsModule(
            client=s3_client,
            storages={s3_bucket: S3StorageConfig(bucket=s3_bucket, encrypt=True)},
        ),
    )
    spec = StorageSpec(name=s3_bucket)
    storage_q = ctx.storage.query(spec)
    storage_c = ctx.storage.command(spec)

    survivor = await storage_c.upload(
        UploadedObject(filename="keep.txt", data=b"keep-me"),
    )
    victim = await storage_c.upload(
        UploadedObject(filename="gone.txt", data=b"delete-me"),
    )

    class _DeletesVictimAfterItsDownload:
        """Command wrapper: drain the downloaded chunks (the download is done),
        then delete the object, then let the write-back proceed."""

        def __getattr__(self, name: str):
            return getattr(storage_c, name)

        async def overwrite_stream(self, key: str, chunks, **kwargs):
            if key != victim.key:
                return await storage_c.overwrite_stream(key, chunks, **kwargs)

            buffered = [piece async for piece in chunks]

            async with s3_client.client():
                await s3_client.delete_object(bucket=s3_bucket, key=victim.key)

            async def _replay():
                for piece in buffered:
                    yield piece

            return await storage_c.overwrite_stream(key, _replay(), **kwargs)

    report = await reencrypt_objects(storage_q, _DeletesVictimAfterItsDownload())  # type: ignore[arg-type]

    assert report.rewritten == 1
    assert report.skipped_missing == 1

    # The survivor round-trips; the victim STAYS deleted — the write-back did
    # not recreate it.
    assert (await storage_q.download(survivor.key)).data == b"keep-me"

    async with s3_client.client():
        assert not await s3_client.object_exists(bucket=s3_bucket, key=victim.key)


# ....................... #


@pytest.mark.integration
@pytest.mark.asyncio
async def test_reencrypt_objects_retries_once_when_the_object_changed_mid_rewrite(
    s3_client, s3_bucket: str
) -> None:
    """Replaced (not deleted) mid-rewrite: the conditional write-back answers 412,
    the sweep re-reads once — fresh bytes, fresh ETag — and the retry lands with
    the concurrent writer's content preserved."""

    from forze.application.integrations.crypto import reencrypt_objects

    ctx = context_from_modules(
        CryptoDepsModule(
            kms=MockKeyManagement(),
            directory=StaticKeyDirectory(KeyRef(key_id="cmk")),
        ),
        S3DepsModule(
            client=s3_client,
            storages={s3_bucket: S3StorageConfig(bucket=s3_bucket, encrypt=True)},
        ),
    )
    spec = StorageSpec(name=s3_bucket)
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
            # Drain the sweep's download first — the replacement below must race
            # the WRITE-BACK, not the download (a replaced object mid-download is
            # a different failure).
            buffered = [piece async for piece in chunks]

            if attempts == 1:
                # Concurrent traffic re-writes the object under the sweep.
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
    assert attempts == 2  # first write-back refused (412), the retry landed

    # The retry re-read the object, so the concurrent write's content survives.
    assert (await storage_q.download(victim.key)).data == b"concurrent"
