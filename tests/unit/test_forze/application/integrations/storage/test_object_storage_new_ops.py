"""Unit tests for the metadata & access ops added to ``ObjectStorageAdapter``.

Covers head / copy / move / download_range / download_if_changed /
put_object_tags against a stubbed client: key validation, correct client args,
and value-object mapping.
"""

from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID

import pytest

from forze.application.contracts.crypto import BytesCipherPort
from forze.application.contracts.storage import ObjectHead, RangedDownload
from forze.application.integrations.storage import (
    ObjectStorageAdapter,
    ObjectStorageHead,
)
from forze.base.exceptions import CoreException

# ----------------------- #

_META = {
    "filename": "foo.txt",
    "size": "11",
    "created_at": "2025-01-15T12:00:00+00:00",
}


async def _resolve_static_bucket(_spec: str, _tenant_id: UUID | None) -> str:
    return "test-bucket"


def _adapter(**kw) -> ObjectStorageAdapter:
    client = MagicMock()
    client.client.return_value.__aenter__ = AsyncMock()
    client.client.return_value.__aexit__ = AsyncMock()
    return ObjectStorageAdapter(
        client=client,
        bucket_spec="test-bucket",
        resolve_bucket=_resolve_static_bucket,
        **kw,
    )


@pytest.fixture
def adapter() -> ObjectStorageAdapter:
    return _adapter()


# ----------------------- #
# head


@pytest.mark.asyncio
async def test_head_maps_client_head_to_object_head(
    adapter: ObjectStorageAdapter,
) -> None:
    lm = datetime(2025, 1, 15, 12, 0, tzinfo=timezone.utc)
    adapter.client.head_object = AsyncMock(
        return_value=ObjectStorageHead(
            content_type="text/plain",
            metadata={"filename": "Zm9v.txt"},
            size=11,
            etag="abc123",
            last_modified=lm,
            tags={"env": "dev"},
        ),
    )

    head = await adapter.head("docs/k1", include_tags=True)

    assert isinstance(head, ObjectHead)
    assert head.content_type == "text/plain"
    assert head.size == 11
    assert head.etag == "abc123"
    assert head.last_modified == lm
    assert head.tags == {"env": "dev"}
    assert head.metadata == {"filename": "Zm9v.txt"}

    kwargs = adapter.client.head_object.await_args.kwargs
    assert kwargs["include_tags"] is True
    assert kwargs["key"] == "docs/k1"


@pytest.mark.asyncio
async def test_head_after_raw_presigned_upload_returns_honest_shape(
    adapter: ObjectStorageAdapter,
) -> None:
    # A presigned PUT stores no envelope (no filename/created_at metadata), yet
    # head must still surface size/content_type/etag.
    adapter.client.head_object = AsyncMock(
        return_value=ObjectStorageHead(
            content_type="image/png",
            metadata={},
            size=2048,
            etag="deadbeef",
        ),
    )

    head = await adapter.head("raw/upload.png")

    assert head.size == 2048
    assert head.content_type == "image/png"
    assert head.etag == "deadbeef"
    assert head.metadata == {}


@pytest.mark.asyncio
async def test_head_rejects_traversal_key(adapter: ObjectStorageAdapter) -> None:
    adapter.client.head_object = AsyncMock()

    with pytest.raises(CoreException):
        await adapter.head("../../secret")

    adapter.client.head_object.assert_not_called()


# ----------------------- #
# copy / move


@pytest.mark.asyncio
async def test_copy_validates_both_keys_and_calls_client(
    adapter: ObjectStorageAdapter,
) -> None:
    adapter.client.copy_object = AsyncMock()
    adapter.client.head_object = AsyncMock(
        return_value=ObjectStorageHead(content_type="text/plain", size=3, etag="e"),
    )

    head = await adapter.copy("src/a", "dst/b")

    assert isinstance(head, ObjectHead)
    assert head.etag == "e"
    kwargs = adapter.client.copy_object.await_args.kwargs
    assert kwargs == {
        "bucket": "test-bucket",
        "src_key": "src/a",
        "dst_key": "dst/b",
        "sse": None,
    }


@pytest.mark.asyncio
async def test_copy_rejects_bad_src_or_dst(adapter: ObjectStorageAdapter) -> None:
    adapter.client.copy_object = AsyncMock()

    with pytest.raises(CoreException):
        await adapter.copy("../etc/passwd", "dst/b")

    with pytest.raises(CoreException):
        await adapter.copy("src/a", "../escape")

    adapter.client.copy_object.assert_not_called()


@pytest.mark.asyncio
async def test_move_copies_then_deletes_source(
    adapter: ObjectStorageAdapter,
) -> None:
    calls: list[str] = []

    async def _copy(**_kw):
        calls.append("copy")

    async def _delete(**_kw):
        calls.append("delete")

    adapter.client.copy_object = AsyncMock(side_effect=_copy)
    adapter.client.delete_object = AsyncMock(side_effect=_delete)
    adapter.client.head_object = AsyncMock(
        return_value=ObjectStorageHead(content_type="text/plain", size=3),
    )

    await adapter.move("src/a", "dst/b")

    # Copy precedes delete (non-atomic move).
    assert calls == ["copy", "delete"]
    del_kwargs = adapter.client.delete_object.await_args.kwargs
    assert del_kwargs == {"bucket": "test-bucket", "key": "src/a"}


# ----------------------- #
# download_range


@pytest.mark.asyncio
async def test_download_range_threads_args_and_maps(
    adapter: ObjectStorageAdapter,
) -> None:
    adapter.client.head_object = AsyncMock(
        return_value=ObjectStorageHead(content_type="text/plain", size=10),
    )
    adapter.client.download_range_bytes = AsyncMock(
        return_value=(b"01234", "bytes 0-4/10", 10),
    )

    ranged = await adapter.download_range("docs/k1", start=0, end=4)

    assert isinstance(ranged, RangedDownload)
    assert ranged.data == b"01234"
    assert ranged.content_range == "bytes 0-4/10"
    assert ranged.total_size == 10
    assert ranged.content_type == "text/plain"

    kwargs = adapter.client.download_range_bytes.await_args.kwargs
    assert kwargs["start"] == 0
    assert kwargs["end"] == 4


@pytest.mark.asyncio
async def test_download_range_validates_window(
    adapter: ObjectStorageAdapter,
) -> None:
    adapter.client.download_range_bytes = AsyncMock()

    with pytest.raises(CoreException):
        await adapter.download_range("docs/k1", start=-1)

    with pytest.raises(CoreException):
        await adapter.download_range("docs/k1", start=5, end=2)

    adapter.client.download_range_bytes.assert_not_called()


@pytest.mark.asyncio
async def test_download_range_rejects_traversal_key(
    adapter: ObjectStorageAdapter,
) -> None:
    adapter.client.download_range_bytes = AsyncMock()

    with pytest.raises(CoreException):
        await adapter.download_range("../../secret", start=0)

    adapter.client.download_range_bytes.assert_not_called()


@pytest.mark.asyncio
async def test_download_range_refused_when_encrypted() -> None:
    cipher = MagicMock(spec=BytesCipherPort)
    adapter = _adapter(cipher=cipher)
    adapter.client.download_range_bytes = AsyncMock()

    with pytest.raises(CoreException, match="encryption"):
        await adapter.download_range("docs/k1", start=0, end=4)

    adapter.client.download_range_bytes.assert_not_called()


# ----------------------- #
# download_if_changed


@pytest.mark.asyncio
async def test_download_if_changed_none_when_unchanged(
    adapter: ObjectStorageAdapter,
) -> None:
    adapter.client.download_bytes_conditional = AsyncMock(return_value=None)
    adapter.client.head_object = AsyncMock()

    result = await adapter.download_if_changed("docs/k1", if_none_match="etag")

    assert result is None
    # No head needed on the not-modified path.
    adapter.client.head_object.assert_not_called()


@pytest.mark.asyncio
async def test_download_if_changed_returns_body_when_changed(
    adapter: ObjectStorageAdapter,
) -> None:
    adapter.client.download_bytes_conditional = AsyncMock(
        return_value=(b"hello world", "text/plain"),
    )
    adapter.client.head_object = AsyncMock(
        return_value=ObjectStorageHead(content_type="text/plain", metadata=_META),
    )

    result = await adapter.download_if_changed("docs/k1", if_none_match="stale")

    assert result is not None
    assert result.data == b"hello world"
    assert result.content_type == "text/plain"
    assert result.filename == "foo.txt"

    kwargs = adapter.client.download_bytes_conditional.await_args.kwargs
    assert kwargs["if_none_match"] == "stale"


@pytest.mark.asyncio
async def test_download_if_changed_requires_a_condition(
    adapter: ObjectStorageAdapter,
) -> None:
    adapter.client.download_bytes_conditional = AsyncMock()

    with pytest.raises(CoreException, match="at least one"):
        await adapter.download_if_changed("docs/k1")

    adapter.client.download_bytes_conditional.assert_not_called()


# ----------------------- #
# put_object_tags


@pytest.mark.asyncio
async def test_put_object_tags_validates_and_calls_client(
    adapter: ObjectStorageAdapter,
) -> None:
    adapter.client.put_object_tags = AsyncMock()

    await adapter.put_object_tags("docs/k1", {"env": "prod"})

    kwargs = adapter.client.put_object_tags.await_args.kwargs
    assert kwargs == {
        "bucket": "test-bucket",
        "key": "docs/k1",
        "tags": {"env": "prod"},
    }


@pytest.mark.asyncio
async def test_put_object_tags_rejects_traversal_key(
    adapter: ObjectStorageAdapter,
) -> None:
    adapter.client.put_object_tags = AsyncMock()

    with pytest.raises(CoreException):
        await adapter.put_object_tags("../../secret", {"a": "b"})

    adapter.client.put_object_tags.assert_not_called()
