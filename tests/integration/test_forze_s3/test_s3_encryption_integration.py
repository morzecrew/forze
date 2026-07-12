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
async def test_s3_encrypted_upload_is_ciphertext_at_rest(
    s3_client, s3_bucket: str
) -> None:
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
