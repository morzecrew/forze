"""Unit tests for presigned URLs on the in-memory mock storage adapter."""

from datetime import datetime, timedelta, timezone

import pytest

from forze.base.exceptions import CoreException, ExceptionKind
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


# ----------------------- #


@pytest.mark.asyncio
async def test_presign_download_is_deterministic_under_frozen_time(
    adapter: MockStorageAdapter,
) -> None:
    with bind_time_source(FrozenTimeSource(INSTANT)):
        first = await adapter.presign_download("docs/k1", expires_in=timedelta(hours=1))
        second = await adapter.presign_download(
            "docs/k1", expires_in=timedelta(hours=1)
        )

    expected_expiry = INSTANT + timedelta(hours=1)
    assert first.url == second.url
    assert (
        first.url
        == f"mock://files/docs/k1?op=get&expires={expected_expiry.isoformat()}"
    )
    assert first.method == "GET"
    assert first.expires_at == expected_expiry
    assert dict(first.headers) == {}


@pytest.mark.asyncio
async def test_presign_upload_url_shape_and_headers(
    adapter: MockStorageAdapter,
) -> None:
    with bind_time_source(FrozenTimeSource(INSTANT)):
        vo = await adapter.presign_upload(
            "docs/k1",
            expires_in=timedelta(minutes=30),
            content_type="text/plain",
        )

    expected_expiry = INSTANT + timedelta(minutes=30)
    assert (
        vo.url == f"mock://files/docs/k1?op=put&expires={expected_expiry.isoformat()}"
    )
    assert vo.method == "PUT"
    assert dict(vo.headers) == {"Content-Type": "text/plain"}


@pytest.mark.asyncio
async def test_presigns_are_recorded_on_the_state(
    adapter: MockStorageAdapter,
    state: MockState,
) -> None:
    with bind_time_source(FrozenTimeSource(INSTANT)):
        await adapter.presign_download("a", expires_in=timedelta(minutes=5))
        await adapter.presign_upload(
            "b",
            expires_in=timedelta(minutes=10),
            content_type="image/png",
        )

    assert state.storage_presigns == [
        {
            "bucket": "files",
            "key": "a",
            "method": "GET",
            "expires_at": INSTANT + timedelta(minutes=5),
            "content_type": None,
        },
        {
            "bucket": "files",
            "key": "b",
            "method": "PUT",
            "expires_at": INSTANT + timedelta(minutes=10),
            "content_type": "image/png",
            "sse": None,
        },
    ]


@pytest.mark.asyncio
async def test_presign_download_does_not_require_object_to_exist(
    adapter: MockStorageAdapter,
) -> None:
    # Production-faithful: signing is local, a missing object only fails the
    # later GET.
    vo = await adapter.presign_download("missing/key", expires_in=timedelta(minutes=5))

    assert vo.method == "GET"


@pytest.mark.asyncio
async def test_presign_enforces_the_seven_day_cap(
    adapter: MockStorageAdapter,
    state: MockState,
) -> None:
    # Production-faithful: both S3 and GCS reject expiries over 7 days.
    with pytest.raises(CoreException) as ei:
        await adapter.presign_download("k", expires_in=timedelta(days=8))

    with pytest.raises(CoreException) as ei_up:
        await adapter.presign_upload("k", expires_in=timedelta(days=8))

    assert ei.value.kind is ExceptionKind.VALIDATION
    assert ei_up.value.kind is ExceptionKind.VALIDATION
    assert state.storage_presigns == []  # nothing issued


@pytest.mark.asyncio
async def test_presign_rejects_non_positive_expiry(
    adapter: MockStorageAdapter,
) -> None:
    with pytest.raises(CoreException) as ei:
        await adapter.presign_upload("k", expires_in=timedelta(0))

    assert ei.value.kind is ExceptionKind.VALIDATION


@pytest.mark.asyncio
async def test_presign_bucket_is_tenant_partitioned(state: MockState) -> None:
    from uuid import UUID

    from forze.application.contracts.tenancy.value_objects import TenantIdentity

    tenant_id = UUID("00000000-0000-0000-0000-000000000001")
    adapter = MockStorageAdapter(
        state=state,
        bucket="files",
        tenant_aware=True,
        tenant_provider=lambda: TenantIdentity(tenant_id=tenant_id),
    )

    with bind_time_source(FrozenTimeSource(INSTANT)):
        vo = await adapter.presign_download("k", expires_in=timedelta(minutes=5))

    # The mock partitions namespaces as "{tenant_id}/{bucket}".
    assert state.storage_presigns[0]["bucket"] == f"{tenant_id}/files"
    assert vo.url.startswith(f"mock://{tenant_id}/files/")
