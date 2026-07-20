"""Live MinIO round-trips for S3 server-side encryption (SSE-S3 / SSE-KMS).

SSE is the **at-rest** axis: the backend encrypts the stored bytes; the app
holds no keys, so it works on direct-upload flows (presigned, multipart, copy).
These tests assert that MinIO actually reports ``ServerSideEncryption`` on the
stored object for the upload, presigned-PUT, multipart, and copy paths.

The default MinIO image rejects **any** SSE (including SSE-S3 ``AES256``) unless
a KMS backend is configured. We enable MinIO's built-in single-key KMS via
``MINIO_KMS_SECRET_KEY=<name>:<base64-32-bytes>``, which unlocks both SSE-S3 and
SSE-KMS (the named key) without a full KES sidecar — so both modes are covered
live here. The unit suite (``tests/unit/test_forze_s3/test_sse.py``) covers the
exact ``ServerSideEncryption`` / ``SSEKMSKeyId`` params and presign headers.
"""

import base64
import shutil
import time
import urllib.error
import urllib.request
from datetime import timedelta
from uuid import uuid4

import httpx
import pytest
import pytest_asyncio

pytest.importorskip("aioboto3")
pytest.importorskip("testcontainers")

from testcontainers.minio import MinioContainer

from forze.application.contracts.storage import StorageSpec, UploadedObject
from forze.application.execution import ExecutionContext
from forze.application.integrations.storage.client import (
    ObjectStoragePartInfo,
    ObjectStorageSSE,
)
from forze_s3.execution.deps.configs import S3ServerSideEncryption, S3StorageConfig
from forze_s3.execution.deps.module import S3DepsModule
from forze_s3.kernel.client import S3Client, S3Config
from tests.support.execution_context import context_from_deps

# ----------------------- #

MINIO_ROOT_USER = "minioadmin"
MINIO_ROOT_PASSWORD = "minioadmin"

KMS_KEY_NAME = "forze-sse-key"
"""Name of the built-in MinIO KMS key (the SSE-KMS ``SSEKMSKeyId``)."""

_KMS_SECRET = base64.b64encode(b"forze-test-sse-master-key-32byte").decode()
"""32-byte master key (base64) for MinIO's built-in single-key KMS."""


@pytest.fixture(scope="session")
def sse_minio_container():
    """A MinIO container with the built-in KMS enabled (unlocks SSE-S3 + SSE-KMS)."""

    if shutil.which("docker") is None:
        pytest.skip("Docker is required for S3 SSE integration tests")

    container = MinioContainer(
        image="minio/minio:RELEASE.2025-09-07T16-13-09Z",
        port=9000,
        access_key=MINIO_ROOT_USER,
        secret_key=MINIO_ROOT_PASSWORD,
    ).with_env("MINIO_KMS_SECRET_KEY", f"{KMS_KEY_NAME}:{_KMS_SECRET}")

    with container as started:
        endpoint = (
            f"http://{started.get_container_host_ip()}:{started.get_exposed_port(9000)}"
        )

        health_url = f"{endpoint}/minio/health/live"
        deadline = time.time() + 60

        while time.time() < deadline:
            try:
                with urllib.request.urlopen(health_url, timeout=2) as resp:
                    if resp.status == 200:
                        break
            except (urllib.error.URLError, TimeoutError, OSError):
                time.sleep(0.5)
        else:
            raise RuntimeError("MinIO (SSE) container did not become healthy in time")

        yield endpoint


@pytest_asyncio.fixture(scope="function")
async def sse_s3_client(sse_minio_container):
    client = S3Client()
    config = S3Config(s3={"addressing_style": "path"})
    await client.initialize(
        endpoint=sse_minio_container,
        access_key_id=MINIO_ROOT_USER,
        secret_access_key=MINIO_ROOT_PASSWORD,
        config=config,
    )

    yield client

    await client.close()


@pytest_asyncio.fixture(scope="function")
async def sse_bucket(sse_s3_client: S3Client) -> str:
    bucket = f"forze-sse-{uuid4().hex[:16]}"

    async with sse_s3_client.client():
        await sse_s3_client.create_bucket(bucket)

    return bucket


# ----------------------- #


async def _sse_field(s3_client: S3Client, bucket: str, key: str) -> tuple[str, str]:
    """Return MinIO's ``(ServerSideEncryption, SSEKMSKeyId)`` head fields."""

    async with s3_client.client():
        api = s3_client._S3Client__require_client()  # type: ignore[attr-defined]
        resp = await api.head_object(Bucket=bucket, Key=key)

    return resp.get("ServerSideEncryption", ""), resp.get("SSEKMSKeyId", "")


def _context(
    s3_client: S3Client, bucket: str, sse: S3ServerSideEncryption
) -> ExecutionContext:
    return context_from_deps(
        S3DepsModule(
            client=s3_client,
            storages={bucket: S3StorageConfig(bucket=bucket, sse=sse)},
        )()
    )


# ----------------------- #
# SSE-S3 (AES256)


@pytest.mark.asyncio
async def test_sse_s3_upload_is_encrypted_at_rest(
    sse_s3_client: S3Client, sse_bucket: str
) -> None:
    ctx = _context(sse_s3_client, sse_bucket, S3ServerSideEncryption(mode="s3"))
    storage_c = ctx.storage.command(StorageSpec(name=sse_bucket))

    uploaded = await storage_c.upload(
        UploadedObject(filename="secret.txt", data=b"at-rest", prefix="sse"),
    )

    enc, _ = await _sse_field(sse_s3_client, sse_bucket, uploaded.key)
    assert enc == "AES256"


@pytest.mark.asyncio
async def test_sse_s3_presigned_put_stores_encrypted(
    sse_s3_client: S3Client, sse_bucket: str
) -> None:
    async with sse_s3_client.client():
        vo = await sse_s3_client.presign_upload_url(
            sse_bucket,
            "sse/presigned.bin",
            expires_in=timedelta(minutes=5),
            sse=ObjectStorageSSE(mode="s3"),
        )

    assert vo.headers["x-amz-server-side-encryption"] == "AES256"

    async with httpx.AsyncClient() as http:
        resp = await http.put(
            vo.url, content=b"presigned-encrypted", headers=dict(vo.headers)
        )

    assert resp.status_code == 200
    enc, _ = await _sse_field(sse_s3_client, sse_bucket, "sse/presigned.bin")
    assert enc == "AES256"


@pytest.mark.asyncio
async def test_sse_s3_multipart_completes_encrypted(
    sse_s3_client: S3Client, sse_bucket: str
) -> None:
    key = "sse/multipart.bin"
    sse = ObjectStorageSSE(mode="s3")
    part_bytes = b"x" * (5 * 1024 * 1024)  # 5 MiB (S3 part-size floor)

    async with sse_s3_client.client():
        upload_id = await sse_s3_client.create_multipart_upload(
            sse_bucket, key, sse=sse
        )
        part_url = await sse_s3_client.presign_multipart_part(
            sse_bucket,
            key,
            upload_id=upload_id,
            part_number=1,
            expires_in=timedelta(minutes=5),
        )

    # Parts inherit the upload's SSE (set on create); the part presign adds none.
    assert not any(h.startswith("x-amz-server-side") for h in part_url.headers)

    async with httpx.AsyncClient() as http:
        put = await http.put(
            part_url.url, content=part_bytes, headers=dict(part_url.headers)
        )

    assert put.status_code == 200
    etag = put.headers["ETag"].strip('"')

    async with sse_s3_client.client():
        await sse_s3_client.complete_multipart_upload(
            sse_bucket,
            key,
            upload_id=upload_id,
            parts=[
                ObjectStoragePartInfo(part_number=1, etag=etag, size=len(part_bytes))
            ],
            sse=sse,
        )

    enc, _ = await _sse_field(sse_s3_client, sse_bucket, key)
    assert enc == "AES256"


@pytest.mark.asyncio
async def test_sse_s3_copy_reencrypts_destination(
    sse_s3_client: S3Client, sse_bucket: str
) -> None:
    async with sse_s3_client.client():
        await sse_s3_client.upload_bytes(sse_bucket, "sse/copy-src.bin", b"copy-me")
        await sse_s3_client.copy_object(
            sse_bucket,
            "sse/copy-src.bin",
            "sse/copy-dst.bin",
            sse=ObjectStorageSSE(mode="s3"),
        )

    enc, _ = await _sse_field(sse_s3_client, sse_bucket, "sse/copy-dst.bin")
    assert enc == "AES256"


# ----------------------- #
# SSE-KMS (aws:kms with the built-in key)


@pytest.mark.asyncio
async def test_sse_kms_upload_is_encrypted_at_rest(
    sse_s3_client: S3Client, sse_bucket: str
) -> None:
    ctx = _context(
        sse_s3_client,
        sse_bucket,
        S3ServerSideEncryption(mode="kms", kms_key_id=KMS_KEY_NAME),
    )
    storage_c = ctx.storage.command(StorageSpec(name=sse_bucket))

    uploaded = await storage_c.upload(
        UploadedObject(filename="kms.txt", data=b"kms-at-rest", prefix="kms"),
    )

    enc, key_id = await _sse_field(sse_s3_client, sse_bucket, uploaded.key)
    assert enc == "aws:kms"
    assert KMS_KEY_NAME in key_id


@pytest.mark.asyncio
async def test_sse_kms_presigned_put_stores_encrypted(
    sse_s3_client: S3Client, sse_bucket: str
) -> None:
    async with sse_s3_client.client():
        vo = await sse_s3_client.presign_upload_url(
            sse_bucket,
            "kms/presigned.bin",
            expires_in=timedelta(minutes=5),
            sse=ObjectStorageSSE(mode="kms", key_id=KMS_KEY_NAME),
        )

    # SSE-KMS requires the client to send the bound SSE headers verbatim.
    assert vo.headers["x-amz-server-side-encryption"] == "aws:kms"
    assert vo.headers["x-amz-server-side-encryption-aws-kms-key-id"] == KMS_KEY_NAME

    async with httpx.AsyncClient() as http:
        resp = await http.put(
            vo.url, content=b"kms-presigned", headers=dict(vo.headers)
        )

    assert resp.status_code == 200
    enc, key_id = await _sse_field(sse_s3_client, sse_bucket, "kms/presigned.bin")
    assert enc == "aws:kms"
    assert KMS_KEY_NAME in key_id
