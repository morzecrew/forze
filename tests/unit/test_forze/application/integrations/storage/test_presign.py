"""Unit tests for presigned-URL support in the storage contracts and adapter."""

from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID

import pytest

from forze.application.contracts.storage import PresignedUrl
from forze.application.integrations.storage import (
    PRESIGN_MAX_EXPIRY,
    ObjectStorageAdapter,
    presign_expiry_seconds,
)
from forze.base.exceptions import CoreException, ExceptionKind

# ----------------------- #


async def _resolve_static_bucket(_spec: str, _tenant_id: UUID | None) -> str:
    return "test-bucket"


@pytest.fixture
def storage_adapter() -> ObjectStorageAdapter:
    client = MagicMock()
    return ObjectStorageAdapter(
        client=client,
        bucket_spec="test-bucket",
        resolve_bucket=_resolve_static_bucket,
    )


def _presigned(method: str = "GET") -> PresignedUrl:
    return PresignedUrl(
        url="https://signed.example/secret-token",
        method=method,  # type: ignore[arg-type]
        expires_at=datetime(2026, 1, 1, tzinfo=UTC),
    )


# ----------------------- #
# PresignedUrl value object


class TestPresignedUrlVO:
    def test_repr_hides_the_url(self) -> None:
        vo = _presigned()

        assert "secret-token" not in repr(vo)
        assert "secret-token" not in str(vo)
        # The non-secret fields stay visible for debugging.
        assert "GET" in repr(vo)

    def test_url_is_still_readable(self) -> None:
        vo = _presigned()

        assert vo.url == "https://signed.example/secret-token"

    def test_frozen(self) -> None:
        vo = _presigned()

        with pytest.raises(Exception):
            vo.url = "https://other"  # type: ignore[misc]

    def test_headers_default_to_empty(self) -> None:
        assert dict(_presigned().headers) == {}


# ----------------------- #
# presign_expiry_seconds helper


class TestPresignExpirySeconds:
    def test_whole_seconds(self) -> None:
        assert presign_expiry_seconds(timedelta(minutes=15)) == 900

    def test_subsecond_rounds_up(self) -> None:
        assert presign_expiry_seconds(timedelta(milliseconds=300)) == 1

    def test_zero_rejected(self) -> None:
        with pytest.raises(CoreException) as ei:
            presign_expiry_seconds(timedelta(0))

        assert ei.value.kind is ExceptionKind.VALIDATION

    def test_negative_rejected(self) -> None:
        with pytest.raises(CoreException) as ei:
            presign_expiry_seconds(timedelta(seconds=-1))

        assert ei.value.kind is ExceptionKind.VALIDATION

    def test_cap_enforced(self) -> None:
        with pytest.raises(CoreException) as ei:
            presign_expiry_seconds(timedelta(days=8))

        assert ei.value.kind is ExceptionKind.VALIDATION

    def test_cap_boundary_allowed(self) -> None:
        assert presign_expiry_seconds(PRESIGN_MAX_EXPIRY) == 7 * 24 * 3600

    def test_cap_can_be_disabled(self) -> None:
        assert presign_expiry_seconds(timedelta(days=30), max_expiry=None) == (
            30 * 24 * 3600
        )


# ----------------------- #
# ObjectStorageAdapter.presign_download / presign_upload


class TestAdapterPresignKeyValidation:
    """The security-round key validation guards both presign methods."""

    @pytest.mark.parametrize(
        "bad_key",
        ["../../secret", "a/../../b", "/absolute/key", "", "key with space"],
    )
    @pytest.mark.asyncio
    async def test_presign_download_rejects_unsafe_keys(
        self,
        storage_adapter: ObjectStorageAdapter,
        bad_key: str,
    ) -> None:
        storage_adapter.client.presign_download_url = AsyncMock()

        with pytest.raises(CoreException):
            await storage_adapter.presign_download(
                bad_key,
                expires_in=timedelta(minutes=5),
            )

        storage_adapter.client.presign_download_url.assert_not_called()

    @pytest.mark.parametrize(
        "bad_key",
        ["../../secret", "a/../../b", "/absolute/key", "", "key with space"],
    )
    @pytest.mark.asyncio
    async def test_presign_upload_rejects_unsafe_keys(
        self,
        storage_adapter: ObjectStorageAdapter,
        bad_key: str,
    ) -> None:
        storage_adapter.client.presign_upload_url = AsyncMock()

        with pytest.raises(CoreException):
            await storage_adapter.presign_upload(
                bad_key,
                expires_in=timedelta(minutes=5),
            )

        storage_adapter.client.presign_upload_url.assert_not_called()


class TestAdapterPresignDelegation:
    @pytest.mark.asyncio
    async def test_presign_download_delegates_with_resolved_bucket(
        self,
        storage_adapter: ObjectStorageAdapter,
    ) -> None:
        storage_adapter.client.client.return_value.__aenter__ = AsyncMock()
        storage_adapter.client.client.return_value.__aexit__ = AsyncMock()
        storage_adapter.client.presign_download_url = AsyncMock(
            return_value=_presigned("GET"),
        )

        vo = await storage_adapter.presign_download(
            "docs/key-1",
            expires_in=timedelta(minutes=15),
        )

        kwargs = storage_adapter.client.presign_download_url.await_args.kwargs
        assert kwargs == {
            "bucket": "test-bucket",
            "key": "docs/key-1",
            "expires_in": timedelta(minutes=15),
        }
        assert vo.method == "GET"

    @pytest.mark.asyncio
    async def test_presign_upload_delegates_and_ensures_bucket(
        self,
        storage_adapter: ObjectStorageAdapter,
    ) -> None:
        storage_adapter.client.client.return_value.__aenter__ = AsyncMock()
        storage_adapter.client.client.return_value.__aexit__ = AsyncMock()
        storage_adapter.client.ensure_bucket = AsyncMock()
        storage_adapter.client.presign_upload_url = AsyncMock(
            return_value=_presigned("PUT"),
        )

        vo = await storage_adapter.presign_upload(
            "docs/key-1",
            expires_in=timedelta(minutes=15),
            content_type="text/plain",
        )

        # Bucket parity with upload(): the handed-out URL targets a bucket
        # that exists.
        storage_adapter.client.ensure_bucket.assert_awaited_once_with("test-bucket")

        kwargs = storage_adapter.client.presign_upload_url.await_args.kwargs
        assert kwargs == {
            "bucket": "test-bucket",
            "key": "docs/key-1",
            "expires_in": timedelta(minutes=15),
            "content_type": "text/plain",
        }
        assert vo.method == "PUT"

    @pytest.mark.asyncio
    async def test_presign_upload_defaults_content_type_to_none(
        self,
        storage_adapter: ObjectStorageAdapter,
    ) -> None:
        storage_adapter.client.client.return_value.__aenter__ = AsyncMock()
        storage_adapter.client.client.return_value.__aexit__ = AsyncMock()
        storage_adapter.client.ensure_bucket = AsyncMock()
        storage_adapter.client.presign_upload_url = AsyncMock(
            return_value=_presigned("PUT"),
        )

        await storage_adapter.presign_upload(
            "docs/key-1",
            expires_in=timedelta(minutes=5),
        )

        kwargs = storage_adapter.client.presign_upload_url.await_args.kwargs
        assert kwargs["content_type"] is None
