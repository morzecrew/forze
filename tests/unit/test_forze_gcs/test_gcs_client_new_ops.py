"""Unit tests for the new GCS client ops (no I/O; fake Storage via attribute).

Covers download_range_bytes / download_bytes_conditional / copy_object /
put_object_tags: correct gcloud-aio call args, range header, 304→None,
416→precondition, and namespaced tag replacement semantics.
"""

from datetime import datetime, timezone
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import aiohttp
import pytest

from forze.base.exceptions import CoreException
from forze_gcs.kernel.client import GCSClient
from forze_gcs.kernel.client.client import TAG_METADATA_PREFIX

# ----------------------- #


def _client(fake_storage: Any) -> GCSClient:
    client = GCSClient()
    client._GCSClient__storage = fake_storage  # type: ignore[attr-defined]
    return client


def _response_error(status: int) -> aiohttp.ClientResponseError:
    return aiohttp.ClientResponseError(
        request_info=MagicMock(),
        history=(),
        status=status,
    )


# ----------------------- #
# download_range_bytes


@pytest.mark.asyncio
async def test_download_range_sends_range_header_and_total() -> None:
    fake = MagicMock()
    fake.download_metadata = AsyncMock(
        return_value={"contentType": "text/plain", "size": 10},
    )
    fake.download = AsyncMock(return_value=b"2345")
    client = _client(fake)

    body, content_range, total = await client.download_range_bytes(
        "b", "k", start=2, end=5
    )

    assert body.data == b"2345"
    assert body.content_type == "text/plain"
    assert content_range == "bytes 2-5/10"
    assert total == 10
    headers = fake.download.await_args.kwargs["headers"]
    assert headers["Range"] == "bytes=2-5"


@pytest.mark.asyncio
async def test_download_range_unsatisfiable_start_beyond_size() -> None:
    fake = MagicMock()
    fake.download_metadata = AsyncMock(
        return_value={"contentType": "text/plain", "size": 10},
    )
    fake.download = AsyncMock()
    client = _client(fake)

    with pytest.raises(CoreException) as ei:
        await client.download_range_bytes("b", "k", start=99)

    assert ei.value.code == "range_not_satisfiable"
    fake.download.assert_not_called()


@pytest.mark.asyncio
async def test_download_range_416_maps_to_precondition() -> None:
    fake = MagicMock()
    fake.download_metadata = AsyncMock(
        return_value={"contentType": "text/plain", "size": 10},
    )
    fake.download = AsyncMock(side_effect=_response_error(416))
    client = _client(fake)

    with pytest.raises(CoreException) as ei:
        await client.download_range_bytes("b", "k", start=0, end=4)

    assert ei.value.code == "range_not_satisfiable"


# ----------------------- #
# download_bytes_conditional


@pytest.mark.asyncio
async def test_conditional_passes_headers_and_returns_body() -> None:
    fake = MagicMock()
    fake.download = AsyncMock(return_value=b"hello")
    fake.download_metadata = AsyncMock(
        return_value={"contentType": "text/plain", "size": 5},
    )
    client = _client(fake)
    since = datetime(2025, 1, 1, 12, 0, tzinfo=timezone.utc)

    result = await client.download_bytes_conditional(
        "b", "k", if_none_match='"abc"', if_modified_since=since
    )

    assert result is not None
    assert result.data == b"hello"
    assert result.content_type == "text/plain"
    headers = fake.download.await_args.kwargs["headers"]
    assert headers["If-None-Match"] == '"abc"'
    assert "GMT" in headers["If-Modified-Since"]


@pytest.mark.asyncio
async def test_conditional_304_returns_none() -> None:
    fake = MagicMock()
    fake.download = AsyncMock(side_effect=_response_error(304))
    client = _client(fake)

    result = await client.download_bytes_conditional("b", "k", if_none_match='"abc"')

    assert result is None


@pytest.mark.asyncio
async def test_conditional_other_error_propagates() -> None:
    fake = MagicMock()
    fake.download = AsyncMock(side_effect=_response_error(403))
    client = _client(fake)

    with pytest.raises(Exception):
        await client.download_bytes_conditional("b", "k", if_none_match="x")


# ----------------------- #
# copy_object


@pytest.mark.asyncio
async def test_copy_object_uses_rewrite_same_bucket() -> None:
    fake = MagicMock()
    fake.copy = AsyncMock(return_value={"done": True})
    client = _client(fake)

    await client.copy_object("b", "src/a", "dst/b")

    args, kwargs = fake.copy.await_args
    assert args[0] == "b"  # source bucket
    assert args[1] == "src/a"  # source key
    assert args[2] == "b"  # destination bucket (same)
    assert kwargs["new_name"] == "dst/b"


# ----------------------- #
# put_object_tags


@pytest.mark.asyncio
async def test_put_object_tags_clears_old_and_sets_new() -> None:
    fake = MagicMock()
    fake.download_metadata = AsyncMock(
        return_value={
            "metadata": {
                "filename": "Zm9v",
                f"{TAG_METADATA_PREFIX}old": "1",
            },
        },
    )
    fake.patch_metadata = AsyncMock(return_value={})
    client = _client(fake)

    await client.put_object_tags("b", "k", {"new": "2"})

    patched = fake.patch_metadata.await_args.args[2]["metadata"]
    # Old namespaced tag is nulled (deleted), new one set, user metadata untouched.
    assert patched[f"{TAG_METADATA_PREFIX}old"] is None
    assert patched[f"{TAG_METADATA_PREFIX}new"] == "2"
    assert "filename" not in patched
