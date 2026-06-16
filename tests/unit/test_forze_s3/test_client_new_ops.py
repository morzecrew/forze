"""Unit tests for the new S3 client ops (no I/O; fake boto client via contextvar).

Covers download_range_bytes / download_bytes_conditional / copy_object /
put_object_tags: correct boto call args, Content-Range parsing, 304→None,
416→precondition.
"""

from datetime import datetime, timezone
from typing import Any

import pytest

from forze.base.exceptions import CoreException
from forze_s3.kernel.client import S3Client

# ----------------------- #


class _ClientError(Exception):
    def __init__(self, response: dict[str, Any]) -> None:
        super().__init__("client error")
        self.response = response


class _S3Exceptions:
    ClientError = _ClientError


class _Body:
    def __init__(self, data: bytes) -> None:
        self._data = data

    async def read(self) -> bytes:
        return self._data


class _FakeApi:
    def __init__(self) -> None:
        self.exceptions = _S3Exceptions()
        self.get_object_calls: list[dict[str, Any]] = []
        self.copy_calls: list[dict[str, Any]] = []
        self.tagging_calls: list[dict[str, Any]] = []
        self.get_object_result: dict[str, Any] | None = None
        self.get_object_error: Exception | None = None

    async def get_object(self, **kwargs: Any) -> dict[str, Any]:
        self.get_object_calls.append(kwargs)

        if self.get_object_error is not None:
            raise self.get_object_error

        assert self.get_object_result is not None
        return self.get_object_result

    async def copy_object(self, **kwargs: Any) -> dict[str, Any]:
        self.copy_calls.append(kwargs)
        return {}

    async def put_object_tagging(self, **kwargs: Any) -> dict[str, Any]:
        self.tagging_calls.append(kwargs)
        return {}


def _client_with(api: _FakeApi) -> tuple[S3Client, Any]:
    client = S3Client()
    tok = client._S3Client__ctx_client.set(api)  # type: ignore[arg-type]
    return client, tok


# ----------------------- #
# download_range_bytes


@pytest.mark.asyncio
async def test_download_range_sends_range_header_and_parses_total() -> None:
    api = _FakeApi()
    api.get_object_result = {
        "Body": _Body(b"01234"),
        "ContentRange": "bytes 0-4/10",
        "ContentType": "text/plain",
    }
    client, tok = _client_with(api)

    try:
        data, content_range, total = await client.download_range_bytes(
            "b", "k", start=0, end=4
        )
    finally:
        client._S3Client__ctx_client.reset(tok)

    assert data == b"01234"
    assert content_range == "bytes 0-4/10"
    assert total == 10
    assert api.get_object_calls[0]["Range"] == "bytes=0-4"


@pytest.mark.asyncio
async def test_download_range_open_ended_header() -> None:
    api = _FakeApi()
    api.get_object_result = {
        "Body": _Body(b"789"),
        "ContentRange": "bytes 7-9/10",
    }
    client, tok = _client_with(api)

    try:
        await client.download_range_bytes("b", "k", start=7)
    finally:
        client._S3Client__ctx_client.reset(tok)

    assert api.get_object_calls[0]["Range"] == "bytes=7-"


@pytest.mark.asyncio
async def test_download_range_invalid_range_maps_to_precondition() -> None:
    api = _FakeApi()
    api.get_object_error = _ClientError(
        {"Error": {"Code": "InvalidRange", "ActualObjectSize": "10"}}
    )
    client, tok = _client_with(api)

    try:
        with pytest.raises(CoreException) as ei:
            await client.download_range_bytes("b", "k", start=99)
    finally:
        client._S3Client__ctx_client.reset(tok)

    assert ei.value.code == "range_not_satisfiable"


@pytest.mark.asyncio
async def test_download_range_validates_window() -> None:
    api = _FakeApi()
    client, tok = _client_with(api)

    try:
        with pytest.raises(CoreException):
            await client.download_range_bytes("b", "k", start=-1)
    finally:
        client._S3Client__ctx_client.reset(tok)

    assert api.get_object_calls == []


# ----------------------- #
# download_bytes_conditional


@pytest.mark.asyncio
async def test_conditional_passes_headers_and_returns_body() -> None:
    api = _FakeApi()
    api.get_object_result = {"Body": _Body(b"hi"), "ContentType": "text/plain"}
    client, tok = _client_with(api)
    since = datetime(2025, 1, 1, tzinfo=timezone.utc)

    try:
        result = await client.download_bytes_conditional(
            "b", "k", if_none_match='"abc"', if_modified_since=since
        )
    finally:
        client._S3Client__ctx_client.reset(tok)

    assert result == (b"hi", "text/plain")
    call = api.get_object_calls[0]
    assert call["IfNoneMatch"] == '"abc"'
    assert call["IfModifiedSince"] == since


@pytest.mark.asyncio
async def test_conditional_304_returns_none() -> None:
    api = _FakeApi()
    api.get_object_error = _ClientError(
        {"Error": {"Code": "304"}, "ResponseMetadata": {"HTTPStatusCode": 304}}
    )
    client, tok = _client_with(api)

    try:
        result = await client.download_bytes_conditional(
            "b", "k", if_none_match='"abc"'
        )
    finally:
        client._S3Client__ctx_client.reset(tok)

    assert result is None


@pytest.mark.asyncio
async def test_conditional_other_error_propagates() -> None:
    api = _FakeApi()
    api.get_object_error = _ClientError(
        {"Error": {"Code": "AccessDenied"}, "ResponseMetadata": {"HTTPStatusCode": 403}}
    )
    client, tok = _client_with(api)

    try:
        with pytest.raises(Exception):
            await client.download_bytes_conditional("b", "k", if_none_match="x")
    finally:
        client._S3Client__ctx_client.reset(tok)


# ----------------------- #
# copy_object


@pytest.mark.asyncio
async def test_copy_object_uses_copy_source_same_bucket() -> None:
    api = _FakeApi()
    client, tok = _client_with(api)

    try:
        await client.copy_object("b", "src/a", "dst/b")
    finally:
        client._S3Client__ctx_client.reset(tok)

    call = api.copy_calls[0]
    assert call["Bucket"] == "b"
    assert call["Key"] == "dst/b"
    assert call["CopySource"] == {"Bucket": "b", "Key": "src/a"}


# ----------------------- #
# put_object_tags


@pytest.mark.asyncio
async def test_put_object_tags_builds_tagset() -> None:
    api = _FakeApi()
    client, tok = _client_with(api)

    try:
        await client.put_object_tags("b", "k", {"env": "prod", "team": "core"})
    finally:
        client._S3Client__ctx_client.reset(tok)

    call = api.tagging_calls[0]
    assert call["Bucket"] == "b"
    assert call["Key"] == "k"
    assert call["Tagging"]["TagSet"] == [
        {"Key": "env", "Value": "prod"},
        {"Key": "team", "Value": "core"},
    ]
