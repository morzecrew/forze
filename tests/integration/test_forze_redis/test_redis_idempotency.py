"""Integration tests for RedisIdempotencyAdapter."""

import pytest

from forze.application.contracts.idempotency import IdempotencySnapshot
from forze.base.errors import ConflictError
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
    """commit stores snapshot; duplicate begin returns cached snapshot."""
    snapshot = IdempotencySnapshot(
        code=201,
        content_type="application/json",
        body=b'{"id":"123"}',
    )
    await redis_idempotency.begin(
        op="create",
        key="req-1",
        payload_hash="hash1",
    )
    await redis_idempotency.commit(
        op="create",
        key="req-1",
        payload_hash="hash1",
        snapshot=snapshot,
    )

    result = await redis_idempotency.begin(
        op="create",
        key="req-1",
        payload_hash="hash1",
    )
    assert result is not None
    assert result.code == 201
    assert result.content_type == "application/json"
    assert result.body == b'{"id":"123"}'


@pytest.mark.asyncio
async def test_idempotency_payload_hash_mismatch_raises(
    redis_idempotency: RedisIdempotencyAdapter,
) -> None:
    """begin with different payload hash raises ConflictError."""
    await redis_idempotency.begin(op="op", key="k1", payload_hash="h1")
    snapshot = IdempotencySnapshot(
        code=200,
        content_type="application/json",
        body=b"ok",
    )
    await redis_idempotency.commit(
        op="op",
        key="k1",
        payload_hash="h1",
        snapshot=snapshot,
    )

    with pytest.raises(ConflictError, match="Payload hash mismatch"):
        await redis_idempotency.begin(op="op", key="k1", payload_hash="h2")


@pytest.mark.asyncio
async def test_idempotency_concurrent_begin_raises(
    redis_idempotency: RedisIdempotencyAdapter,
) -> None:
    """Second begin before commit raises ConflictError (pending)."""
    await redis_idempotency.begin(op="op", key="k2", payload_hash="same")

    with pytest.raises(ConflictError, match="in progress"):
        await redis_idempotency.begin(op="op", key="k2", payload_hash="same")
