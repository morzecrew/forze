"""Unit tests for S3 server-side encryption (SSE-S3 / SSE-KMS) threading.

Exercises the S3 client (stubbed ``aiobotocore`` API, no I/O): upload, copy,
presigned PUT, and multipart-create pass the right ``ServerSideEncryption`` /
``SSEKMSKeyId`` params for the ``s3`` and ``kms`` modes, the presigned PUT
echoes the SSE request headers for KMS, and the route config validates the
``kms_key_id`` requirement. SSE is the *at-rest* axis (the backend encrypts),
independent of client-side ``encrypt``.
"""

from __future__ import annotations

from datetime import timedelta
from typing import Any

import pytest

from forze.application.integrations.storage.client import ObjectStorageSSE
from forze.base.exceptions import CoreException
from forze_s3.execution.deps.configs import S3ServerSideEncryption, S3StorageConfig
from forze_s3.kernel.client.client import S3Client

# ----------------------- #

EXPIRES = timedelta(minutes=10)


class _S3Exceptions:
    class ClientError(Exception):
        pass


class _FakeApi:
    """Records the SSE params seen by each backend call (no I/O)."""

    def __init__(self) -> None:
        self.exceptions = _S3Exceptions()
        self.upload_calls: list[dict[str, Any]] = []
        self.copy_calls: list[dict[str, Any]] = []
        self.create_calls: list[dict[str, Any]] = []
        self.presign_calls: list[dict[str, Any]] = []

    async def upload_fileobj(
        self,
        fileobj: Any,
        *,
        Bucket: str,
        Key: str,
        ExtraArgs: dict[str, Any] | None = None,
    ) -> None:
        self.upload_calls.append({"ExtraArgs": ExtraArgs})

    async def copy_object(self, **kwargs: Any) -> dict[str, Any]:
        self.copy_calls.append(kwargs)
        return {}

    async def create_multipart_upload(self, **kwargs: Any) -> dict[str, Any]:
        self.create_calls.append(kwargs)
        return {"UploadId": "UID-1"}

    async def generate_presigned_url(
        self,
        ClientMethod: str,
        *,
        Params: dict[str, Any],
        ExpiresIn: int,
    ) -> str:
        self.presign_calls.append({"ClientMethod": ClientMethod, "Params": Params})
        return f"https://s3.local/{Params['Bucket']}/{Params['Key']}?sig=x"


def _client_with(api: _FakeApi) -> tuple[S3Client, Any]:
    client = S3Client()
    tok = client._S3Client__ctx_client.set(api)  # type: ignore[arg-type]
    return client, tok


# ----------------------- #
# upload_bytes


@pytest.mark.asyncio
async def test_upload_no_sse_sends_no_sse_params() -> None:
    api = _FakeApi()
    client, tok = _client_with(api)
    try:
        await client.upload_bytes("b", "k", b"data")
    finally:
        client._S3Client__ctx_client.reset(tok)

    extra = api.upload_calls[0]["ExtraArgs"] or {}
    assert "ServerSideEncryption" not in extra


@pytest.mark.asyncio
async def test_upload_sse_s3_sets_aes256() -> None:
    api = _FakeApi()
    client, tok = _client_with(api)
    try:
        await client.upload_bytes("b", "k", b"data", sse=ObjectStorageSSE(mode="s3"))
    finally:
        client._S3Client__ctx_client.reset(tok)

    extra = api.upload_calls[0]["ExtraArgs"]
    assert extra["ServerSideEncryption"] == "AES256"
    assert "SSEKMSKeyId" not in extra


@pytest.mark.asyncio
async def test_upload_sse_kms_sets_aws_kms_and_key_id() -> None:
    api = _FakeApi()
    client, tok = _client_with(api)
    try:
        await client.upload_bytes(
            "b",
            "k",
            b"data",
            sse=ObjectStorageSSE(mode="kms", key_id="arn:aws:kms:key/abc"),
        )
    finally:
        client._S3Client__ctx_client.reset(tok)

    extra = api.upload_calls[0]["ExtraArgs"]
    assert extra["ServerSideEncryption"] == "aws:kms"
    assert extra["SSEKMSKeyId"] == "arn:aws:kms:key/abc"


# ----------------------- #
# copy_object


@pytest.mark.asyncio
async def test_copy_object_reencrypts_destination_with_sse_kms() -> None:
    api = _FakeApi()
    client, tok = _client_with(api)
    try:
        await client.copy_object(
            "b",
            "src/a",
            "dst/b",
            sse=ObjectStorageSSE(mode="kms", key_id="kid"),
        )
    finally:
        client._S3Client__ctx_client.reset(tok)

    call = api.copy_calls[0]
    assert call["ServerSideEncryption"] == "aws:kms"
    assert call["SSEKMSKeyId"] == "kid"
    assert call["CopySource"] == {"Bucket": "b", "Key": "src/a"}


@pytest.mark.asyncio
async def test_copy_object_no_sse_sends_no_sse_params() -> None:
    api = _FakeApi()
    client, tok = _client_with(api)
    try:
        await client.copy_object("b", "src/a", "dst/b")
    finally:
        client._S3Client__ctx_client.reset(tok)

    assert "ServerSideEncryption" not in api.copy_calls[0]


# ----------------------- #
# presign_upload_url


@pytest.mark.asyncio
async def test_presign_upload_binds_and_returns_kms_headers() -> None:
    api = _FakeApi()
    client, tok = _client_with(api)
    try:
        vo = await client.presign_upload_url(
            "b",
            "k",
            expires_in=EXPIRES,
            sse=ObjectStorageSSE(mode="kms", key_id="kid"),
        )
    finally:
        client._S3Client__ctx_client.reset(tok)

    # Signed params bind the SSE so S3 enforces it.
    params = api.presign_calls[0]["Params"]
    assert params["ServerSideEncryption"] == "aws:kms"
    assert params["SSEKMSKeyId"] == "kid"

    # Returned headers carry what the uploader must send verbatim.
    assert vo.headers["x-amz-server-side-encryption"] == "aws:kms"
    assert vo.headers["x-amz-server-side-encryption-aws-kms-key-id"] == "kid"


@pytest.mark.asyncio
async def test_presign_upload_sse_s3_binds_aes256_header() -> None:
    api = _FakeApi()
    client, tok = _client_with(api)
    try:
        vo = await client.presign_upload_url(
            "b",
            "k",
            expires_in=EXPIRES,
            sse=ObjectStorageSSE(mode="s3"),
        )
    finally:
        client._S3Client__ctx_client.reset(tok)

    assert api.presign_calls[0]["Params"]["ServerSideEncryption"] == "AES256"
    assert vo.headers["x-amz-server-side-encryption"] == "AES256"


@pytest.mark.asyncio
async def test_presign_upload_no_sse_has_no_sse_headers() -> None:
    api = _FakeApi()
    client, tok = _client_with(api)
    try:
        vo = await client.presign_upload_url("b", "k", expires_in=EXPIRES)
    finally:
        client._S3Client__ctx_client.reset(tok)

    assert "ServerSideEncryption" not in api.presign_calls[0]["Params"]
    assert not any(h.startswith("x-amz-server-side") for h in vo.headers)


# ----------------------- #
# create_multipart_upload (parts inherit; presign_part carries no SSE header)


@pytest.mark.asyncio
async def test_multipart_create_sets_sse_kms() -> None:
    api = _FakeApi()
    client, tok = _client_with(api)
    try:
        upload_id = await client.create_multipart_upload(
            "b",
            "k",
            sse=ObjectStorageSSE(mode="kms", key_id="kid"),
        )
    finally:
        client._S3Client__ctx_client.reset(tok)

    assert upload_id == "UID-1"
    call = api.create_calls[0]
    assert call["ServerSideEncryption"] == "aws:kms"
    assert call["SSEKMSKeyId"] == "kid"


@pytest.mark.asyncio
async def test_multipart_presign_part_carries_no_sse_header() -> None:
    api = _FakeApi()
    client, tok = _client_with(api)
    try:
        vo = await client.presign_multipart_part(
            "b",
            "k",
            upload_id="UID-1",
            part_number=1,
            expires_in=EXPIRES,
        )
    finally:
        client._S3Client__ctx_client.reset(tok)

    # Parts inherit the upload's SSE (set on create); UploadPart rejects
    # per-part SSE headers, so the presigned part URL adds none.
    assert not any(h.startswith("x-amz-server-side") for h in vo.headers)


# ----------------------- #
# config validation


def test_sse_kms_requires_key_id() -> None:
    with pytest.raises(CoreException):
        S3ServerSideEncryption(mode="kms")


def test_sse_none_forbids_key_id() -> None:
    with pytest.raises(CoreException):
        S3ServerSideEncryption(mode="none", kms_key_id="kid")


def test_sse_s3_forbids_key_id() -> None:
    with pytest.raises(CoreException):
        S3ServerSideEncryption(mode="s3", kms_key_id="kid")


def test_sse_kms_with_key_id_is_valid() -> None:
    sse = S3ServerSideEncryption(mode="kms", kms_key_id="kid")
    assert sse.mode == "kms"
    assert sse.kms_key_id == "kid"


def test_storage_config_defaults_sse_off() -> None:
    config = S3StorageConfig(bucket="b")
    assert config.sse.mode == "none"
    assert config.sse.kms_key_id is None
