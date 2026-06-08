"""Integration tests for RedisIdempotencyAdapter."""

from forze.base.exceptions import CoreException
import pytest

from forze.application.contracts.idempotency import IdempotencyRecord
from forze_redis.adapters import RedisIdempotencyAdapter

@pytest.mark.asyncio
async def test_idempotency_begin_returns_none_when_new(
    redis_idempotency: RedisIdempotencyAdapter,
) -> None:
    """begin returns None when no prior operation exists."""
    result = await redis_idempotency.begin(
        op="test_op",
        key="key1",
        payload_hash="abc123",
    )
    assert result is None

@pytest.mark.asyncio
async def test_idempotency_begin_with_none_key_returns_none(
    redis_idempotency: RedisIdempotencyAdapter,
) -> None:
    """begin with key=None returns None without storing."""
    result = await redis_idempotency.begin(
        op="test_op",
        key=None,
        payload_hash="abc123",
    )
    assert result is None

@pytest.mark.asyncio
async def test_idempotency_commit_and_replay(
    redis_idempotency: RedisIdempotencyAdapter,
) -> None:
    """commit stores the record; duplicate begin returns the cached record."""
    record = IdempotencyRecord(result=b'{"id":"123"}')
    await redis_idempotency.begin(
        op="create",
        key="req-1",
        payload_hash="hash1",
    )
    await redis_idempotency.commit(
        op="create",
        key="req-1",
        payload_hash="hash1",
        record=record,
    )

    result = await redis_idempotency.begin(
        op="create",
        key="req-1",
        payload_hash="hash1",
    )
    assert result is not None
    assert result.result == b'{"id":"123"}'

@pytest.mark.asyncio
async def test_idempotency_payload_hash_mismatch_raises(
    redis_idempotency: RedisIdempotencyAdapter,
) -> None:
    """begin with different payload hash raises ConflictError."""
    await redis_idempotency.begin(op="op", key="k1", payload_hash="h1")
    record = IdempotencyRecord(result=b"ok")
    await redis_idempotency.commit(
        op="op",
        key="k1",
        payload_hash="h1",
        record=record,
    )

    with pytest.raises(CoreException, match="Payload hash mismatch"):
        await redis_idempotency.begin(op="op", key="k1", payload_hash="h2")

@pytest.mark.asyncio
async def test_idempotency_concurrent_begin_raises(
    redis_idempotency: RedisIdempotencyAdapter,
) -> None:
    """Second begin before commit raises ConflictError (pending)."""
    await redis_idempotency.begin(op="op", key="k2", payload_hash="same")

    with pytest.raises(CoreException, match="in progress"):
        await redis_idempotency.begin(op="op", key="k2", payload_hash="same")
