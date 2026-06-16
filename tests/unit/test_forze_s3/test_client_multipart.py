"""Unit tests for the S3 native multipart client primitives (no I/O).

Drives the fake boto client via the same contextvar seam as
``test_client_new_ops``: checks create/presign/list/complete/abort send the
right ``UploadId``/``PartNumber``/``ETag`` args.
"""

from datetime import timedelta
from typing import Any

import pytest

from forze.application.integrations.storage.client import ObjectStoragePartInfo
from forze.base.exceptions import CoreException
from forze_s3.kernel.client import S3Client

# ----------------------- #


class _Paginator:
    def __init__(self, pages: list[dict[str, Any]]) -> None:
        self._pages = pages
        self.paginate_kwargs: dict[str, Any] | None = None

    def paginate(self, **kwargs: Any) -> "_Paginator":
        self.paginate_kwargs = kwargs
        return self

    def __aiter__(self) -> "_PageIter":
        return _PageIter(self._pages)


class _PageIter:
    def __init__(self, pages: list[dict[str, Any]]) -> None:
        self._it = iter(pages)

    def __aiter__(self) -> "_PageIter":
        return self

    async def __anext__(self) -> dict[str, Any]:
        try:
            return next(self._it)
        except StopIteration:
            raise StopAsyncIteration


class _FakeApi:
    def __init__(self) -> None:
        self.create_calls: list[dict[str, Any]] = []
        self.presign_calls: list[dict[str, Any]] = []
        self.complete_calls: list[dict[str, Any]] = []
        self.abort_calls: list[dict[str, Any]] = []
        self.list_pages: list[dict[str, Any]] = []
        self.paginator: _Paginator | None = None
        self.create_result: dict[str, Any] = {"UploadId": "UID-123"}

    async def create_multipart_upload(self, **kwargs: Any) -> dict[str, Any]:
        self.create_calls.append(kwargs)
        return self.create_result

    async def generate_presigned_url(self, op: str, **kwargs: Any) -> str:
        self.presign_calls.append({"op": op, **kwargs})
        return f"https://s3/{op}"

    def get_paginator(self, name: str) -> _Paginator:
        self.paginator = _Paginator(self.list_pages)
        return self.paginator

    async def complete_multipart_upload(self, **kwargs: Any) -> dict[str, Any]:
        self.complete_calls.append(kwargs)
        return {}

    async def abort_multipart_upload(self, **kwargs: Any) -> dict[str, Any]:
        self.abort_calls.append(kwargs)
        return {}


def _client_with(api: _FakeApi) -> tuple[S3Client, Any]:
    client = S3Client()
    tok = client._S3Client__ctx_client.set(api)  # type: ignore[arg-type]
    return client, tok


# ----------------------- #


@pytest.mark.asyncio
async def test_create_multipart_returns_upload_id() -> None:
    api = _FakeApi()
    client, tok = _client_with(api)

    try:
        upload_id = await client.create_multipart_upload(
            "b", "k", content_type="text/plain"
        )
    finally:
        client._S3Client__ctx_client.reset(tok)

    assert upload_id == "UID-123"
    call = api.create_calls[0]
    assert call["Bucket"] == "b"
    assert call["Key"] == "k"
    assert call["ContentType"] == "text/plain"


@pytest.mark.asyncio
async def test_create_multipart_missing_upload_id_raises() -> None:
    api = _FakeApi()
    api.create_result = {}
    client, tok = _client_with(api)

    try:
        with pytest.raises(CoreException):
            await client.create_multipart_upload("b", "k")
    finally:
        client._S3Client__ctx_client.reset(tok)


@pytest.mark.asyncio
async def test_presign_part_signs_upload_part() -> None:
    api = _FakeApi()
    client, tok = _client_with(api)

    try:
        url = await client.presign_multipart_part(
            "b",
            "k",
            upload_id="UID-123",
            part_number=2,
            expires_in=timedelta(minutes=10),
        )
    finally:
        client._S3Client__ctx_client.reset(tok)

    assert url.method == "PUT"
    call = api.presign_calls[0]
    assert call["op"] == "upload_part"
    assert call["Params"] == {
        "Bucket": "b",
        "Key": "k",
        "UploadId": "UID-123",
        "PartNumber": 2,
    }


@pytest.mark.asyncio
async def test_list_parts_parses_pages() -> None:
    api = _FakeApi()
    api.list_pages = [
        {"Parts": [{"PartNumber": 2, "ETag": '"e2"', "Size": 5}]},
        {"Parts": [{"PartNumber": 1, "ETag": '"e1"', "Size": 5}]},
    ]
    client, tok = _client_with(api)

    try:
        parts = await client.list_multipart_parts("b", "k", upload_id="UID")
    finally:
        client._S3Client__ctx_client.reset(tok)

    # Sorted ascending; ETags unquoted.
    assert [p.part_number for p in parts] == [1, 2]
    assert parts[0].etag == "e1"
    assert parts[1].size == 5
    assert api.paginator is not None
    assert api.paginator.paginate_kwargs == {
        "Bucket": "b",
        "Key": "k",
        "UploadId": "UID",
    }


@pytest.mark.asyncio
async def test_complete_sends_sorted_quoted_etags() -> None:
    api = _FakeApi()
    client, tok = _client_with(api)

    parts = [
        ObjectStoragePartInfo(part_number=2, etag="e2"),
        ObjectStoragePartInfo(part_number=1, etag='"e1"'),
    ]

    try:
        await client.complete_multipart_upload(
            "b", "k", upload_id="UID", parts=parts
        )
    finally:
        client._S3Client__ctx_client.reset(tok)

    call = api.complete_calls[0]
    assert call["UploadId"] == "UID"
    # Ascending part order, every ETag quoted exactly once.
    assert call["MultipartUpload"]["Parts"] == [
        {"PartNumber": 1, "ETag": '"e1"'},
        {"PartNumber": 2, "ETag": '"e2"'},
    ]


@pytest.mark.asyncio
async def test_abort_sends_upload_id() -> None:
    api = _FakeApi()
    client, tok = _client_with(api)

    try:
        await client.abort_multipart_upload("b", "k", upload_id="UID")
    finally:
        client._S3Client__ctx_client.reset(tok)

    assert api.abort_calls[0] == {"Bucket": "b", "Key": "k", "UploadId": "UID"}
