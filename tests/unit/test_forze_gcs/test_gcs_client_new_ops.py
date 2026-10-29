"""Unit tests for the new GCS client ops (no I/O; fake Storage via attribute).

Covers download_range_bytes / download_bytes_conditional / copy_object /
put_object_tags: correct gcloud-aio call args, range header, 304→None,
416→precondition, and namespaced tag replacement semantics.
"""

from datetime import UTC, datetime, timezone
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import aiohttp
import pytest

from forze.base.exceptions import CoreException, ExceptionKind
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
    fake._download = AsyncMock(return_value=b"2345")
    client = _client(fake)

    body, content_range, total = await client.download_range_bytes(
        "b", "k", start=2, end=5
    )

    assert body.data == b"2345"
    assert body.content_type == "text/plain"
    assert content_range == "bytes 2-5/10"
    assert total == 10
    headers = fake._download.await_args.kwargs["headers"]
    assert headers["Range"] == "bytes=2-5"
    assert fake._download.await_args.kwargs["params"] == {"alt": "media"}


@pytest.mark.asyncio
async def test_download_range_unsatisfiable_start_beyond_size() -> None:
    fake = MagicMock()
    fake.download_metadata = AsyncMock(
        return_value={"contentType": "text/plain", "size": 10},
    )
    fake._download = AsyncMock()
    client = _client(fake)

    with pytest.raises(CoreException) as ei:
        await client.download_range_bytes("b", "k", start=99)

    assert ei.value.code == "range_not_satisfiable"
    fake._download.assert_not_called()


@pytest.mark.asyncio
async def test_download_range_416_maps_to_precondition() -> None:
    fake = MagicMock()
    fake.download_metadata = AsyncMock(
        return_value={"contentType": "text/plain", "size": 10},
    )
    fake._download = AsyncMock(side_effect=_response_error(416))
    client = _client(fake)

    with pytest.raises(CoreException) as ei:
        await client.download_range_bytes("b", "k", start=0, end=4)

    assert ei.value.code == "range_not_satisfiable"


@pytest.mark.asyncio
async def test_download_range_if_match_pins_the_generation() -> None:
    fake = MagicMock()
    fake.download_metadata = AsyncMock(
        return_value={"contentType": "text/plain", "size": 10, "etag": "E1", "generation": "7"},
    )
    fake._download = AsyncMock(return_value=b"2345")
    client = _client(fake)

    await client.download_range_bytes("b", "k", start=2, end=5, if_match='"E1"')

    params = fake._download.await_args.kwargs["params"]
    assert params == {"alt": "media", "ifGenerationMatch": "7"}


@pytest.mark.asyncio
async def test_download_range_if_match_refuses_a_changed_etag() -> None:
    fake = MagicMock()
    fake.download_metadata = AsyncMock(
        return_value={"contentType": "text/plain", "size": 10, "etag": "E2", "generation": "8"},
    )
    fake._download = AsyncMock()
    client = _client(fake)

    with pytest.raises(CoreException) as ei:
        await client.download_range_bytes("b", "k", start=0, end=4, if_match="E1")

    assert (ei.value.kind, ei.value.code) == (
        ExceptionKind.CONFLICT,
        "core.storage.read_precondition_failed",
    )
    fake._download.assert_not_called()


@pytest.mark.asyncio
async def test_download_range_if_match_refuses_an_object_with_no_generation() -> None:
    # Without a generation the media GET cannot be pinned: refuse rather than read unpinned.
    fake = MagicMock()
    fake.download_metadata = AsyncMock(
        return_value={"contentType": "text/plain", "size": 10, "etag": "E1"},
    )
    fake._download = AsyncMock()
    client = _client(fake)

    with pytest.raises(CoreException) as ei:
        await client.download_range_bytes("b", "k", start=0, end=4, if_match="E1")

    assert ei.value.code == "core.storage.read_unpinnable"
    fake._download.assert_not_called()


@pytest.mark.asyncio
async def test_download_range_if_match_412_maps_to_conflict() -> None:
    fake = MagicMock()
    fake.download_metadata = AsyncMock(
        return_value={"contentType": "text/plain", "size": 10, "etag": "E1", "generation": "7"},
    )
    fake._download = AsyncMock(side_effect=_response_error(412))
    client = _client(fake)

    with pytest.raises(CoreException) as ei:
        await client.download_range_bytes("b", "k", start=0, end=4, if_match="E1")

    assert ei.value.code == "core.storage.read_precondition_failed"


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
    since = datetime(2025, 1, 1, 12, 0, tzinfo=UTC)

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

    with pytest.raises(CoreException):
        await client.download_bytes_conditional("b", "k", if_none_match="x")


# ----------------------- #
# _http_date


def test_http_date_naive_treated_as_utc() -> None:
    from forze_gcs.kernel.client.client import _http_date

    dt = datetime(2026, 1, 2, 10, 0, 0)  # naive
    assert _http_date(dt) == "Fri, 02 Jan 2026 10:00:00 GMT"


def test_http_date_non_utc_aware_converted_to_gmt() -> None:
    from datetime import timedelta

    from forze_gcs.kernel.client.client import _http_date

    # +02:00 local → 08:00 GMT. ``format_datetime(usegmt=True)`` requires UTC,
    # so a non-UTC tz-aware value must be converted, not stamped.
    tz = timezone(timedelta(hours=2))
    dt = datetime(2026, 1, 2, 10, 0, 0, tzinfo=tz)
    assert _http_date(dt) == "Fri, 02 Jan 2026 08:00:00 GMT"


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


# ----------------------- #
# list_objects


@pytest.mark.asyncio
async def test_list_objects_filters_multipart_scaffolding() -> None:
    """Compose-based multipart temp parts must not surface as listed objects."""

    fake = MagicMock()
    fake.list_objects = AsyncMock(
        return_value={
            "items": [
                {"name": "docs/a.txt"},
                {"name": "docs/b.txt.__forze_mpu__/sess-1/1"},
                {"name": "docs/b.txt.__forze_mpu__/sess-1/2"},
                {"name": "docs/c.txt"},
            ],
            "nextPageToken": "",
        },
    )
    client = _client(fake)

    items, total = await client.list_objects("b", prefix="docs/")

    assert [i.key for i in items] == ["docs/a.txt", "docs/c.txt"]
    assert total == 2
