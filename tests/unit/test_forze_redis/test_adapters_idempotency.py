from unittest.mock import AsyncMock, MagicMock
from uuid import UUID

import pytest
from datetime import timedelta

from forze.application.contracts.idempotency import IdempotencySnapshot
from forze.application.contracts.tenancy import TenantIdentity
from forze.base.errors import ConflictError
from forze_redis.adapters.codecs import RedisKeyCodec
from forze_redis.adapters.idempotency import RedisIdempotencyAdapter

_TID = UUID("12345678-1234-5678-1234-567812345678")
_NS = "test"


@pytest.fixture
def mock_redis_client() -> MagicMock:
    client = MagicMock()
    client.set = AsyncMock(return_value=True)
    client.get = AsyncMock(return_value=None)
    return client


@pytest.fixture
def adapter_with_tenant(mock_redis_client: MagicMock) -> RedisIdempotencyAdapter:
    return RedisIdempotencyAdapter(
        client=mock_redis_client,
        key_codec=RedisKeyCodec(namespace=_NS),
        ttl=timedelta(seconds=60),
        tenant_aware=True,
        tenant_provider=lambda: TenantIdentity(tenant_id=_TID),
    )


@pytest.fixture
def adapter_without_tenant(mock_redis_client: MagicMock) -> RedisIdempotencyAdapter:
    return RedisIdempotencyAdapter(
        client=mock_redis_client,
        key_codec=RedisKeyCodec(namespace=_NS),
        ttl=timedelta(seconds=60),
    )


def _expected_key_with_tenant() -> str:
    return f"tenant:{_TID}:idempotency:{_NS}:op:test-key"


def _expected_key_without_tenant() -> str:
    return f"idempotency:{_NS}:op:test-key"


@pytest.mark.asyncio
async def test_key_generation_with_tenant(adapter_with_tenant: RedisIdempotencyAdapter) -> None:
    key = adapter_with_tenant._RedisIdempotencyAdapter__key("op", "test-key")
    assert key == _expected_key_with_tenant()


@pytest.mark.asyncio
async def test_key_generation_without_tenant(adapter_without_tenant: RedisIdempotencyAdapter) -> None:
    key = adapter_without_tenant._RedisIdempotencyAdapter__key("op", "test-key")
    assert key == _expected_key_without_tenant()


@pytest.mark.asyncio
async def test_begin_success_with_tenant(
    adapter_with_tenant: RedisIdempotencyAdapter,
    mock_redis_client: MagicMock,
) -> None:
    mock_redis_client.set.return_value = True
    result = await adapter_with_tenant.begin("op", "test-key", "hash123")

    mock_redis_client.set.assert_called_once()
    args, _kwargs = mock_redis_client.set.call_args
    assert args[0] == _expected_key_with_tenant()
    assert result is None


@pytest.mark.asyncio
async def test_begin_no_key(adapter_with_tenant: RedisIdempotencyAdapter, mock_redis_client: MagicMock) -> None:
    result = await adapter_with_tenant.begin("op", None, "hash123")
    mock_redis_client.set.assert_not_called()
    assert result is None


@pytest.mark.asyncio
async def test_commit_success_with_tenant(
    adapter_with_tenant: RedisIdempotencyAdapter,
    mock_redis_client: MagicMock,
) -> None:
    mock_redis_client.set.return_value = True
    snapshot = IdempotencySnapshot(
        code=200, content_type="application/json", body=b"test-body"
    )

    await adapter_with_tenant.commit("op", "test-key", "hash123", snapshot)

    mock_redis_client.set.assert_called_once()
    args, _kwargs = mock_redis_client.set.call_args
    assert args[0] == _expected_key_with_tenant()


@pytest.mark.asyncio
async def test_commit_no_key(adapter_with_tenant: RedisIdempotencyAdapter, mock_redis_client: MagicMock) -> None:
    snapshot = IdempotencySnapshot(
        code=200, content_type="application/json", body=b"test-body"
    )
    await adapter_with_tenant.commit("op", None, "hash123", snapshot)
    mock_redis_client.set.assert_not_called()


@pytest.mark.asyncio
async def test_commit_failed_missing_or_expired(
    adapter_with_tenant: RedisIdempotencyAdapter,
    mock_redis_client: MagicMock,
) -> None:
    mock_redis_client.set.return_value = False
    snapshot = IdempotencySnapshot(
        code=200, content_type="application/json", body=b"test-body"
    )

    with pytest.raises(ConflictError, match="Idempotency commit failed"):
        await adapter_with_tenant.commit("op", "test-key", "hash123", snapshot)


@pytest.mark.asyncio
async def test_begin_conflict_not_readable(
    adapter_with_tenant: RedisIdempotencyAdapter,
    mock_redis_client: MagicMock,
) -> None:
    mock_redis_client.set.side_effect = [False, False]
    mock_redis_client.get.return_value = None

    with pytest.raises(ConflictError, match="not readable"):
        await adapter_with_tenant.begin("op", "test-key", "hash123")


@pytest.mark.asyncio
async def test_begin_conflict_hash_mismatch(
    adapter_with_tenant: RedisIdempotencyAdapter,
    mock_redis_client: MagicMock,
) -> None:
    mock_redis_client.set.return_value = False
    import json

    mock_redis_client.get.return_value = json.dumps({"st": "P", "ph": "wrong_hash"})

    with pytest.raises(ConflictError, match="Payload hash mismatch"):
        await adapter_with_tenant.begin("op", "test-key", "hash123")
