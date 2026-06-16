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
async def test_complete_chains_composes_past_cap() -> None:
    fake = _fake_storage()
    client = _client(fake)

    n_parts = COMPOSE_MAX_SOURCES + 5  # forces chaining
    parts = [ObjectStoragePartInfo(part_number=i) for i in range(1, n_parts + 1)]

    await client.complete_multipart_upload("b", "k", upload_id="SID", parts=parts)

    # More than one compose call (chained); none exceeds the source cap.
    assert fake.compose.await_count >= 2
    for call in fake.compose.await_args_list:
        sources = call.args[2]
        assert len(sources) <= COMPOSE_MAX_SOURCES

    # Final compose writes the destination key.
    final_call = fake.compose.await_args_list[-1]
    assert final_call.args[1] == "k"

    # The intermediate accumulator is cleaned up too.
    acc_key = f"k.{MPU_NAMESPACE}/SID/__compose__"
    deleted = {c.args[1] for c in fake.delete.await_args_list}
    assert acc_key in deleted


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
