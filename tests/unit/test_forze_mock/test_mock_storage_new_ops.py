"""Unit tests for the metadata & access ops on the mock storage adapter."""

from datetime import datetime, timedelta, timezone

import pytest

from forze.base.exceptions import CoreException
from forze.base.primitives import FrozenTimeSource, bind_time_source
from forze_mock.adapters.storage import MockStorageAdapter
from forze_mock.state import MockState

# ----------------------- #

INSTANT = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)


@pytest.fixture
def state() -> MockState:
    return MockState()


@pytest.fixture
def adapter(state: MockState) -> MockStorageAdapter:
    return MockStorageAdapter(state=state, bucket="files")


async def _upload(adapter: MockStorageAdapter, data: bytes, **kw):
    from forze.application.contracts.storage import UploadedObject

    return await adapter.upload(UploadedObject(filename="a.txt", data=data, **kw))


# ----------------------- #
# head


@pytest.mark.asyncio
async def test_head_is_deterministic_etag_and_time_sourced(
    adapter: MockStorageAdapter,
) -> None:
    with bind_time_source(FrozenTimeSource(INSTANT)):
        stored = await _upload(adapter, b"hello world", tags={"env": "dev"})

        h1 = await adapter.head(stored.key)
        h2 = await adapter.head(stored.key)

    assert h1.size == 11
    assert h1.content_type == "text/plain"
    assert h1.last_modified == INSTANT
    assert h1.tags == {"env": "dev"}
    # ETag is a stable hash of the bytes.
    assert h1.etag == h2.etag
    assert h1.etag  # non-empty


@pytest.mark.asyncio
async def test_head_missing_raises(adapter: MockStorageAdapter) -> None:
    with pytest.raises(CoreException):
        await adapter.head("nope")


# ----------------------- #
# download_range


@pytest.mark.asyncio
async def test_download_range_slices_and_reports_content_range(
    adapter: MockStorageAdapter,
) -> None:
    stored = await _upload(adapter, b"0123456789")

    ranged = await adapter.download_range(stored.key, start=2, end=5)

    assert ranged.data == b"2345"
    assert ranged.content_range == "bytes 2-5/10"
    assert ranged.total_size == 10


@pytest.mark.asyncio
async def test_download_range_open_ended_reads_to_eof(
    adapter: MockStorageAdapter,
) -> None:
    stored = await _upload(adapter, b"0123456789")

    ranged = await adapter.download_range(stored.key, start=7)

    assert ranged.data == b"789"
    assert ranged.content_range == "bytes 7-9/10"


@pytest.mark.asyncio
async def test_download_range_unsatisfiable_raises(
    adapter: MockStorageAdapter,
) -> None:
    stored = await _upload(adapter, b"0123456789")

    with pytest.raises(CoreException) as ei:
        await adapter.download_range(stored.key, start=99)

    assert ei.value.code == "range_not_satisfiable"


@pytest.mark.asyncio
async def test_download_range_validates_window(adapter: MockStorageAdapter) -> None:
    stored = await _upload(adapter, b"0123456789")

    with pytest.raises(CoreException):
        await adapter.download_range(stored.key, start=-1)

    with pytest.raises(CoreException):
        await adapter.download_range(stored.key, start=5, end=1)


# ----------------------- #
# download_if_changed


@pytest.mark.asyncio
async def test_download_if_changed_matching_etag_returns_none(
    adapter: MockStorageAdapter,
) -> None:
    stored = await _upload(adapter, b"hello world")
    head = await adapter.head(stored.key)

    result = await adapter.download_if_changed(
        stored.key, if_none_match=head.etag
    )

    assert result is None


@pytest.mark.asyncio
async def test_download_if_changed_stale_etag_returns_body(
    adapter: MockStorageAdapter,
) -> None:
    stored = await _upload(adapter, b"hello world")

    result = await adapter.download_if_changed(
        stored.key, if_none_match='"deadbeef"'
    )

    assert result is not None
    assert result.data == b"hello world"


@pytest.mark.asyncio
async def test_download_if_changed_modified_since(
    adapter: MockStorageAdapter,
) -> None:
    with bind_time_source(FrozenTimeSource(INSTANT)):
        stored = await _upload(adapter, b"hello world")

    # Caller's copy is newer than upload time → not modified → None.
    fresh = await adapter.download_if_changed(
        stored.key, if_modified_since=INSTANT + timedelta(hours=1)
    )
    assert fresh is None

    # Caller's copy is older → changed → body.
    stale = await adapter.download_if_changed(
        stored.key, if_modified_since=INSTANT - timedelta(hours=1)
    )
    assert stale is not None
    assert stale.data == b"hello world"


@pytest.mark.asyncio
async def test_download_if_changed_etag_takes_precedence_over_date(
    adapter: MockStorageAdapter,
) -> None:
    # RFC 7232 §6: when If-None-Match is present, If-Modified-Since is ignored.
    # ETag mismatches (changed) but the date alone would say "not modified" —
    # the body must still be returned, matching a real S3/GCS GetObject.
    with bind_time_source(FrozenTimeSource(INSTANT)):
        stored = await _upload(adapter, b"hello world")

    result = await adapter.download_if_changed(
        stored.key,
        if_none_match='"deadbeef"',
        if_modified_since=INSTANT + timedelta(hours=1),
    )

    assert result is not None
    assert result.data == b"hello world"


@pytest.mark.asyncio
async def test_download_if_changed_requires_a_condition(
    adapter: MockStorageAdapter,
) -> None:
    stored = await _upload(adapter, b"x")

    with pytest.raises(CoreException, match="at least one"):
        await adapter.download_if_changed(stored.key)


# ----------------------- #
# copy / move


@pytest.mark.asyncio
async def test_copy_duplicates_bytes_metadata_tags(
    adapter: MockStorageAdapter,
) -> None:
    src = await _upload(adapter, b"payload", tags={"env": "dev"})

    head = await adapter.copy(src.key, "copies/dst")

    assert head.size == len(b"payload")
    assert head.tags == {"env": "dev"}

    # Both keys readable, same bytes.
    src_dl = await adapter.download(src.key)
    dst_dl = await adapter.download("copies/dst")
    assert src_dl.data == dst_dl.data == b"payload"


@pytest.mark.asyncio
async def test_move_deletes_source(adapter: MockStorageAdapter) -> None:
    src = await _upload(adapter, b"payload")

    await adapter.move(src.key, "moved/dst")

    moved = await adapter.download("moved/dst")
    assert moved.data == b"payload"

    with pytest.raises(CoreException):
        await adapter.download(src.key)


@pytest.mark.asyncio
async def test_self_move_is_noop_no_data_loss(adapter: MockStorageAdapter) -> None:
    # move(k, k) is copy-then-delete; without the guard the delete would destroy
    # the object. It must be a no-op that leaves the object intact.
    src = await _upload(adapter, b"payload")

    head = await adapter.move(src.key, src.key)

    intact = await adapter.download(src.key)
    assert intact.data == b"payload"
    assert head.size == len(b"payload")


@pytest.mark.asyncio
async def test_copy_missing_source_raises(adapter: MockStorageAdapter) -> None:
    with pytest.raises(CoreException):
        await adapter.copy("nope", "dst")


# ----------------------- #
# put_object_tags


@pytest.mark.asyncio
async def test_put_object_tags_replaces(adapter: MockStorageAdapter) -> None:
    stored = await _upload(adapter, b"x", tags={"old": "1"})

    await adapter.put_object_tags(stored.key, {"new": "2", "env": "prod"})

    head = await adapter.head(stored.key)
    assert head.tags == {"new": "2", "env": "prod"}


@pytest.mark.asyncio
async def test_put_object_tags_missing_raises(adapter: MockStorageAdapter) -> None:
    with pytest.raises(CoreException):
        await adapter.put_object_tags("nope", {"a": "b"})


# ----------------------- #
# streaming (upload_stream / download_stream)


async def _aiter(data: bytes, *, piece: int = 50):
    for i in range(0, len(data), piece):
        yield data[i : i + piece]


@pytest.mark.asyncio
async def test_upload_stream_round_trip(adapter: MockStorageAdapter) -> None:
    data = b"streamed payload" * 20

    stored = await adapter.upload_stream(_aiter(data), filename="big.bin", prefix="up")

    assert stored.size == len(data)
    assert stored.key.startswith("up/")

    dl = await adapter.download_stream(stored.key)
    out = bytearray()
    async for piece in dl.chunks:
        out += piece

    assert bytes(out) == data
    assert dl.filename == stored.filename
    assert dl.size == len(data)


@pytest.mark.asyncio
async def test_download_stream_missing_key_raises(adapter: MockStorageAdapter) -> None:
    with pytest.raises(CoreException):
        await adapter.download_stream("nope")
