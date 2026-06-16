"""Unit tests for the multipart upload-session port on the shared adapter.

Exercises the public ``StorageUploadSessionPort`` methods on the S3 adapter
against a fake client (no I/O): key validation, tenant-bucket resolution, VO
mapping, part_number validation, complete part-list forwarding, and the
encrypting-route refusal.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from typing import Any
from unittest.mock import MagicMock

import pytest

from forze.application.contracts.storage import (
    PresignedUrl,
    StorageSpec,
    UploadPart,
)
from forze.application.integrations.storage.client import (
    ObjectStorageHead,
    ObjectStoragePartInfo,
)
from forze.base.exceptions import CoreException
from forze.application.execution import CryptoDepsModule
from forze.application.contracts.crypto import KeyRef, StaticKeyDirectory
from forze_mock import MockKeyManagement
from forze_s3.adapters import S3StorageAdapter
from forze_s3.execution.deps import S3DepsModule
from forze_s3.execution.deps.configs import S3StorageConfig
from forze_s3.kernel.client import S3ClientPort
from tests.support.execution_context import context_from_modules

# ----------------------- #

EXPIRES = timedelta(minutes=10)


class _FakeClient:
    def __init__(self) -> None:
        self.create_calls: list[dict[str, Any]] = []
        self.presign_calls: list[dict[str, Any]] = []
        self.complete_calls: list[dict[str, Any]] = []
        self.abort_calls: list[dict[str, Any]] = []
        self.list_result: list[ObjectStoragePartInfo] = []
        self.upload_id = "UID-xyz"

    @asynccontextmanager
    async def client(self):
        yield self

    async def ensure_bucket(self, bucket: str) -> None:
        pass

    async def create_multipart_upload(
        self, *, bucket: str, key: str, content_type: str | None = None
    ) -> str:
        self.create_calls.append(
            {"bucket": bucket, "key": key, "content_type": content_type}
        )
        return self.upload_id

    async def presign_multipart_part(
        self,
        *,
        bucket: str,
        key: str,
        upload_id: str,
        part_number: int,
        expires_in: timedelta,
    ) -> PresignedUrl:
        self.presign_calls.append(
            {
                "bucket": bucket,
                "key": key,
                "upload_id": upload_id,
                "part_number": part_number,
            }
        )
        return PresignedUrl(
            url="https://s3/part",
            method="PUT",
            expires_at=datetime.now(timezone.utc) + expires_in,
        )

    async def list_multipart_parts(
        self, *, bucket: str, key: str, upload_id: str
    ) -> list[ObjectStoragePartInfo]:
        return self.list_result

    async def complete_multipart_upload(
        self, *, bucket: str, key: str, upload_id: str, parts: Any
    ) -> None:
        self.complete_calls.append(
            {"bucket": bucket, "key": key, "upload_id": upload_id, "parts": list(parts)}
        )

    async def abort_multipart_upload(
        self, *, bucket: str, key: str, upload_id: str
    ) -> None:
        self.abort_calls.append(
            {"bucket": bucket, "key": key, "upload_id": upload_id}
        )

    async def head_object(
        self, *, bucket: str, key: str, include_tags: bool = False
    ) -> ObjectStorageHead:
        return ObjectStorageHead(
            content_type="application/octet-stream",
            size=30,
            etag="assembled-etag",
        )


def _adapter(client: _FakeClient) -> S3StorageAdapter:
    return S3StorageAdapter(
        client=client,  # type: ignore[arg-type]
        bucket_spec="bkt",
        tenant_aware=False,
        tenant_provider=lambda: None,
    )


# ----------------------- #


@pytest.mark.asyncio
async def test_begin_upload_validates_key() -> None:
    adapter = _adapter(_FakeClient())

    with pytest.raises(CoreException):
        await adapter.begin_upload("../escape")


@pytest.mark.asyncio
async def test_begin_upload_opens_session() -> None:
    client = _FakeClient()
    adapter = _adapter(client)

    session = await adapter.begin_upload("files/big.bin", content_type="text/plain")

    assert session.key == "files/big.bin"
    assert session.upload_id == "UID-xyz"
    assert session.bucket == "bkt"
    assert session.content_type == "text/plain"
    assert client.create_calls[0]["content_type"] == "text/plain"


@pytest.mark.asyncio
async def test_presign_part_validates_part_number() -> None:
    client = _FakeClient()
    adapter = _adapter(client)
    session = await adapter.begin_upload("k")

    with pytest.raises(CoreException):
        await adapter.presign_part(session, 0, expires_in=EXPIRES)


@pytest.mark.asyncio
async def test_presign_part_forwards_args() -> None:
    client = _FakeClient()
    adapter = _adapter(client)
    session = await adapter.begin_upload("k")

    url = await adapter.presign_part(session, 3, expires_in=EXPIRES)

    assert url.method == "PUT"
    call = client.presign_calls[0]
    assert call["part_number"] == 3
    assert call["upload_id"] == "UID-xyz"
    assert call["bucket"] == "bkt"


@pytest.mark.asyncio
async def test_list_parts_maps_vos() -> None:
    client = _FakeClient()
    client.list_result = [
        ObjectStoragePartInfo(part_number=1, etag="e1", size=5),
        ObjectStoragePartInfo(part_number=2, etag="e2", size=5),
    ]
    adapter = _adapter(client)
    session = await adapter.begin_upload("k")

    parts = await adapter.list_parts(session)

    assert [p.part_number for p in parts] == [1, 2]
    assert all(isinstance(p, UploadPart) for p in parts)
    assert parts[0].etag == "e1"


@pytest.mark.asyncio
async def test_complete_sorts_and_forwards_parts_then_heads() -> None:
    client = _FakeClient()
    adapter = _adapter(client)
    session = await adapter.begin_upload("k")

    head = await adapter.complete_upload(
        session,
        [
            UploadPart(part_number=2, etag="e2"),
            UploadPart(part_number=1, etag="e1"),
        ],
    )

    assert head.size == 30
    assert head.etag == "assembled-etag"
    forwarded = client.complete_calls[0]["parts"]
    assert [p.part_number for p in forwarded] == [1, 2]


@pytest.mark.asyncio
async def test_complete_requires_parts() -> None:
    client = _FakeClient()
    adapter = _adapter(client)
    session = await adapter.begin_upload("k")

    with pytest.raises(CoreException, match="at least one"):
        await adapter.complete_upload(session, [])


@pytest.mark.asyncio
async def test_abort_forwards() -> None:
    client = _FakeClient()
    adapter = _adapter(client)
    session = await adapter.begin_upload("k")

    await adapter.abort_upload(session)

    assert client.abort_calls[0]["upload_id"] == "UID-xyz"


# ----------------------- #
# Encryption incompatibility


def _encrypting_ctx():
    client = MagicMock(spec=S3ClientPort)
    return context_from_modules(
        CryptoDepsModule(
            kms=MockKeyManagement(),
            directory=StaticKeyDirectory(KeyRef(key_id="cmk")),
        ),
        S3DepsModule(
            client=client,
            storages={"docs": S3StorageConfig(bucket="b", encrypt=True)},
        ),
    )


@pytest.mark.asyncio
async def test_begin_upload_refused_on_encrypting_route() -> None:
    ctx = _encrypting_ctx()
    uploads = ctx.storage.uploads(StorageSpec(name="docs"))

    with pytest.raises(CoreException) as ei:
        await uploads.begin_upload("k")

    assert "encryption" in str(ei.value).lower()


@pytest.mark.asyncio
async def test_uploads_port_resolvable_on_plain_route() -> None:
    client = MagicMock(spec=S3ClientPort)
    ctx = context_from_modules(
        S3DepsModule(
            client=client,
            storages={"docs": S3StorageConfig(bucket="b")},
        ),
    )

    uploads = ctx.storage.uploads(StorageSpec(name="docs"))
    assert uploads is not None
