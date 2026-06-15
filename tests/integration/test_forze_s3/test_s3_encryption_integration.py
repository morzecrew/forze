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
        raw = await s3_client.download_bytes(bucket=s3_bucket, key=stored.key)

    assert raw != b"top-secret-payload"
    assert is_envelope(raw)

    # The adapter transparently decrypts on download.
    downloaded = await storage_q.download(stored.key)
    assert downloaded.data == b"top-secret-payload"
