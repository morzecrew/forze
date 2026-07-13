"""Unit tests for the GCS compose-based multipart client primitives (no I/O).

Probes the temp part-key namespace layout, list-from-temp-objects resume,
compose ordering, the >32 chained-compose path, and temp cleanup.
"""

from datetime import timedelta
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from forze.application.integrations.storage.client import ObjectStoragePartInfo
from forze_gcs.kernel.client import GCSClient
from forze_gcs.kernel.client.client import COMPOSE_MAX_SOURCES, MPU_NAMESPACE

# ----------------------- #

EXPIRES = timedelta(minutes=10)


def _client(fake_storage: Any) -> GCSClient:
    client = GCSClient()
    client._GCSClient__storage = fake_storage  # type: ignore[attr-defined]
    return client


def _fake_storage(blob_keys: list[str] | None = None) -> Any:
    fake = MagicMock()
    bucket_ref = MagicMock()
    bucket_ref.list_blobs = AsyncMock(return_value=list(blob_keys or []))
    fake.get_bucket = MagicMock(return_value=bucket_ref)
    fake.compose = AsyncMock(return_value={})
    fake.copy = AsyncMock(return_value={})
    fake.delete = AsyncMock(return_value=None)
    return fake


# ----------------------- #


@pytest.mark.asyncio
async def test_create_multipart_returns_session_token() -> None:
    client = _client(_fake_storage())

    uid = await client.create_multipart_upload("b", "files/k", content_type="x")

    # Opaque, non-empty, no backend round-trip.
    assert uid


@pytest.mark.asyncio
async def test_part_key_layout() -> None:
    part_key = GCSClient._mpu_part_key("files/k", "SID", 3)
    assert part_key == f"files/k.{MPU_NAMESPACE}/SID/3"


@pytest.mark.asyncio
async def test_list_parts_reads_temp_objects() -> None:
    prefix = f"k.{MPU_NAMESPACE}/SID/"
    fake = _fake_storage([f"{prefix}2", f"{prefix}1", f"{prefix}notanint"])
    client = _client(fake)

    parts = await client.list_multipart_parts("b", "k", upload_id="SID")

    # Only numeric suffixes, ascending.
    assert [p.part_number for p in parts] == [1, 2]
    fake.get_bucket.return_value.list_blobs.assert_awaited_once_with(prefix=prefix)


@pytest.mark.asyncio
async def test_complete_composes_in_order_and_cleans_up() -> None:
    fake = _fake_storage()
    client = _client(fake)

    parts = [
        ObjectStoragePartInfo(part_number=2),
        ObjectStoragePartInfo(part_number=1),
        ObjectStoragePartInfo(part_number=3),
    ]

    await client.complete_multipart_upload("b", "k", upload_id="SID", parts=parts)

    # One compose call (<= 32 parts) with the temp keys in ascending order.
    fake.compose.assert_awaited_once()
    call = fake.compose.await_args
    assert call.args[0] == "b"
    assert call.args[1] == "k"
    sources = call.args[2]
    expected = [GCSClient._mpu_part_key("k", "SID", n) for n in (1, 2, 3)]
    assert sources == expected

    # Temps deleted.
    deleted = {c.args[1] for c in fake.delete.await_args_list}
    assert deleted == set(expected)


@pytest.mark.asyncio
async def test_complete_single_part_rewrites_via_copy() -> None:
    # compose rejects a single source; a 1-part upload must rewrite (copy) the
    # lone part to the destination instead. (fake-gcs accepts single-source
    # compose, masking this — real GCS returns 400.)
    fake = _fake_storage()
    client = _client(fake)

    await client.complete_multipart_upload(
        "b", "k", upload_id="SID", parts=[ObjectStoragePartInfo(part_number=1)]
    )

    # No compose at all; a copy from the lone part to the destination key.
    fake.compose.assert_not_called()
    fake.copy.assert_awaited_once()
    call = fake.copy.await_args
    lone = GCSClient._mpu_part_key("k", "SID", 1)
    assert call.args[0] == "b"
    assert call.args[1] == lone
    assert call.args[2] == "b"
    assert call.kwargs["new_name"] == "k"

    # The lone temp part is cleaned up.
    deleted = {c.args[1] for c in fake.delete.await_args_list}
    assert lone in deleted


@pytest.mark.parametrize("n_parts", [COMPOSE_MAX_SOURCES + 1, 65])
@pytest.mark.asyncio
async def test_complete_chains_composes_past_cap(n_parts: int) -> None:
    fake = _fake_storage()
    client = _client(fake)

    parts = [ObjectStoragePartInfo(part_number=i) for i in range(1, n_parts + 1)]

    await client.complete_multipart_upload("b", "k", upload_id="SID", parts=parts)

    # More than one compose call (chained); none exceeds the cap and — crucially
    # — none is single-source (compose rejects a lone source on real GCS).
    assert fake.compose.await_count >= 2
    for call in fake.compose.await_args_list:
        sources = call.args[2]
        assert 2 <= len(sources) <= COMPOSE_MAX_SOURCES

    # Final compose writes straight to the destination key (no extra 1-source
    # fold afterwards).
    final_call = fake.compose.await_args_list[-1]
    assert final_call.args[1] == "k"

    # The intermediate accumulator is cleaned up too.
    acc_key = f"k.{MPU_NAMESPACE}/SID/__compose__"
    deleted = {c.args[1] for c in fake.delete.await_args_list}
    assert acc_key in deleted


@pytest.mark.asyncio
async def test_complete_binds_content_type_on_destination() -> None:
    # The caller's content type (bound at begin_upload) must reach the final
    # destination object, not the octet-stream the bare part PUTs carry.
    fake = _fake_storage()
    client = _client(fake)

    # Multi-part: compose to the destination key gets the content type.
    parts = [ObjectStoragePartInfo(part_number=n) for n in (1, 2)]
    await client.complete_multipart_upload(
        "b", "k", upload_id="SID", parts=parts, content_type="video/mp4"
    )
    assert fake.compose.await_args.kwargs["content_type"] == "video/mp4"

    # Single-part: the copy/rewrite carries the type via the metadata body.
    fake.copy.reset_mock()
    await client.complete_multipart_upload(
        "b",
        "k2",
        upload_id="SID2",
        parts=[ObjectStoragePartInfo(part_number=1)],
        content_type="image/png",
    )
    assert fake.copy.await_args.kwargs["metadata"] == {"contentType": "image/png"}


@pytest.mark.asyncio
async def test_complete_chain_binds_content_type_only_on_final_dest() -> None:
    fake = _fake_storage()
    client = _client(fake)

    parts = [ObjectStoragePartInfo(part_number=i) for i in range(1, 65)]
    await client.complete_multipart_upload(
        "b", "k", upload_id="SID", parts=parts, content_type="application/pdf"
    )

    # Only the compose whose destination is the real key carries the type;
    # intermediate accumulators keep the default (they are temp + deleted).
    for call in fake.compose.await_args_list:
        expected = "application/pdf" if call.args[1] == "k" else None
        assert call.kwargs["content_type"] == expected


@pytest.mark.asyncio
async def test_complete_single_part_binds_metadata_in_rewrite() -> None:
    # The rewrite body is the destination resource: user metadata rides the
    # (conditional) final write itself — no follow-up patch that a concurrent
    # overwrite could absorb.
    fake = _fake_storage()
    fake.patch_metadata = AsyncMock(return_value={})
    client = _client(fake)

    await client.complete_multipart_upload(
        "b",
        "k",
        upload_id="SID",
        parts=[ObjectStoragePartInfo(part_number=1)],
        content_type="image/png",
        metadata={"owner": "me"},
    )

    body = fake.copy.await_args.kwargs["metadata"]
    assert body == {"contentType": "image/png", "metadata": {"owner": "me"}}
    fake.patch_metadata.assert_not_called()


@pytest.mark.asyncio
async def test_complete_compose_patch_pinned_to_composed_generation() -> None:
    # Under if_match the metadata patch must address the exact object the final
    # compose wrote (its new generation), not whatever sits at the key by then.
    fake = _fake_storage()
    fake.download_metadata = AsyncMock(return_value={"etag": '"E1"', "generation": "7"})
    fake.compose = AsyncMock(return_value={"generation": "8"})
    fake.patch_metadata = AsyncMock(return_value={})
    client = _client(fake)

    parts = [ObjectStoragePartInfo(part_number=n) for n in (1, 2)]
    await client.complete_multipart_upload(
        "b", "k", upload_id="SID", parts=parts, metadata={"owner": "me"}, if_match="E1"
    )

    # The compose carried the pre-read generation; the patch carries the new one.
    assert fake.compose.await_args.kwargs["params"] == {"ifGenerationMatch": "7"}
    patch_call = fake.patch_metadata.await_args
    assert patch_call.args[2] == {"metadata": {"owner": "me"}}
    assert patch_call.kwargs["params"] == {"ifGenerationMatch": "8"}


@pytest.mark.asyncio
async def test_complete_metadata_patch_412_maps_to_conflict() -> None:
    # A 412 on the pinned patch means the destination was replaced right after
    # the compose — the same conflict as the final-write precondition failure.
    from aiohttp import ClientResponseError, RequestInfo
    from yarl import URL

    from forze.base.exceptions import CoreException, ExceptionKind

    url = URL("http://example.com/storage/v1/b/b/o/k")
    request_info = RequestInfo(url=url, method="PATCH", headers={}, real_url=url)

    fake = _fake_storage()
    fake.download_metadata = AsyncMock(return_value={"etag": '"E1"', "generation": "7"})
    fake.compose = AsyncMock(return_value={"generation": "8"})
    fake.patch_metadata = AsyncMock(
        side_effect=ClientResponseError(
            request_info=request_info, history=(), status=412, message="", headers={}
        )
    )
    client = _client(fake)

    parts = [ObjectStoragePartInfo(part_number=n) for n in (1, 2)]

    with pytest.raises(CoreException) as e:
        await client.complete_multipart_upload(
            "b", "k", upload_id="SID", parts=parts, metadata={"owner": "me"}, if_match="E1"
        )

    assert e.value.kind == ExceptionKind.CONFLICT


@pytest.mark.asyncio
async def test_complete_requires_parts() -> None:
    from forze.base.exceptions import CoreException

    client = _client(_fake_storage())

    with pytest.raises(CoreException):
        await client.complete_multipart_upload("b", "k", upload_id="SID", parts=[])


@pytest.mark.asyncio
async def test_abort_deletes_all_temp_parts() -> None:
    prefix = f"k.{MPU_NAMESPACE}/SID/"
    fake = _fake_storage([f"{prefix}1", f"{prefix}2"])
    client = _client(fake)

    await client.abort_multipart_upload("b", "k", upload_id="SID")

    deleted = {c.args[1] for c in fake.delete.await_args_list}
    assert deleted == {f"{prefix}1", f"{prefix}2"}


@pytest.mark.asyncio
async def test_upload_multipart_part_writes_temp_part_object() -> None:
    fake = _fake_storage()
    fake.upload = AsyncMock(return_value=None)
    client = _client(fake)

    info = await client.upload_multipart_part(
        "b", "files/k", upload_id="SID", part_number=2, data=b"chunk-bytes"
    )

    assert isinstance(info, ObjectStoragePartInfo)
    assert info.part_number == 2
    assert info.size == len(b"chunk-bytes")

    part_key = GCSClient._mpu_part_key("files/k", "SID", 2)
    args = fake.upload.await_args
    assert args.args[0] == "b"
    assert args.args[1] == part_key
    assert args.args[2] == b"chunk-bytes"
