"""Unit tests for :class:`~forze_sqs.kernel.platform.client.SQSClient` helpers (no I/O)."""

import asyncio
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock

import pytest
from pydantic import SecretStr

from forze.base.errors import CoreError, InfrastructureError
from forze_sqs.kernel.platform.client import SQSClient

# ----------------------- #


def test_is_queue_url() -> None:
    assert SQSClient._SQSClient__is_queue_url("https://sqs.local/1/q") is True
    assert SQSClient._SQSClient__is_queue_url("http://sqs.local/1/q") is True
    assert SQSClient._SQSClient__is_queue_url("my-queue") is False


def test_sanitize_queue_name_replaces_invalid_chars() -> None:
    s = SQSClient._SQSClient__sanitize_queue_name
    assert s("my@queue") == "my_queue"
    assert s("___only___bad___") == "only_bad"
    assert s("!@#") == "queue"
    assert s("a" * 100) == "a" * 80


def test_sanitize_fifo_truncates_base() -> None:
    s = SQSClient._SQSClient__sanitize_queue_name
    long_base = "x" * 90
    out = s(f"{long_base}.fifo")
    assert out.endswith(".fifo")
    assert len(out) == 80


def test_is_fifo_target() -> None:
    fifo = SQSClient._SQSClient__is_fifo_target
    assert fifo("tasks.fifo", "https://x/y/tasks.fifo") is True
    assert fifo("tasks", "https://x/y/tasks.fifo") is True
    assert fifo("tasks", "https://x/y/tasks") is False


def test_require_session_raises_when_uninitialized() -> None:
    client = SQSClient()
    with pytest.raises(CoreError, match="session is not initialized"):
        client._SQSClient__require_session()


def test_require_client_raises_when_no_context() -> None:
    client = SQSClient()
    with pytest.raises(CoreError, match="client is not initialized"):
        client._SQSClient__require_client()


@pytest.mark.asyncio
async def test_close_clears_state() -> None:
    client = SQSClient()
    client._SQSClient__queue_url_cache["k"] = "v"  # type: ignore[attr-defined]
    await client.close()
    assert client._SQSClient__session is None  # type: ignore[attr-defined]
    assert client._SQSClient__opts is None  # type: ignore[attr-defined]
    assert client._SQSClient__queue_url_cache == {}  # type: ignore[attr-defined]


@pytest.mark.asyncio
async def test_initialize_converts_timedelta_in_config() -> None:
    client = SQSClient()
    await client.initialize(
        endpoint="http://localhost:4566",
        access_key_id="k",
        secret_access_key=SecretStr("s"),
        region_name="us-east-1",
        config={
            "connect_timeout": timedelta(seconds=5),
            "read_timeout": timedelta(seconds=2),
        },
    )
    opts = client._SQSClient__opts  # type: ignore[attr-defined]
    assert opts is not None
    assert opts.config is not None
    assert opts.config.connect_timeout == 5
    assert opts.config.read_timeout == 2
    await client.close()


@pytest.mark.asyncio
async def test_initialize_is_idempotent() -> None:
    client = SQSClient()
    await client.initialize(
        endpoint="http://localhost:4566",
        access_key_id="k",
        secret_access_key="s",
        region_name="us-east-1",
    )
    first_session = client._SQSClient__session  # type: ignore[attr-defined]
    await client.initialize(
        endpoint="http://other",
        access_key_id="x",
        secret_access_key="y",
        region_name="eu-west-1",
    )
    assert client._SQSClient__session is first_session  # type: ignore[attr-defined]
    await client.close()


def test_build_message_attributes_partial() -> None:
    b = SQSClient._SQSClient__build_message_attributes(
        type="t",
        key=None,
        enqueued_at=None,
    )
    assert "forze_type" in b
    assert "forze_key" not in b


def test_encode_decode_body_roundtrip() -> None:
    raw = b"\x00\xff"
    enc = SQSClient._SQSClient__encode_body(raw)
    attrs = SQSClient._SQSClient__build_message_attributes(
        type=None,
        key=None,
        enqueued_at=None,
    )
    assert SQSClient._SQSClient__decode_body(enc, attrs) == raw


def test_decode_body_invalid_base64() -> None:
    attrs = SQSClient._SQSClient__build_message_attributes(
        type=None,
        key=None,
        enqueued_at=None,
    )
    with pytest.raises(InfrastructureError, match="base64"):
        SQSClient._SQSClient__decode_body("@@@", attrs)


def test_extract_enqueued_at_iso_and_sent_timestamp() -> None:
    dt = datetime(2024, 1, 2, 3, 4, 5, tzinfo=timezone.utc)
    attrs = {
        "forze_enqueued_at": {"StringValue": dt.isoformat(), "DataType": "String"},
    }
    got = SQSClient._SQSClient__extract_enqueued_at(attrs, None)
    assert got == dt

    bad_iso = SQSClient._SQSClient__extract_enqueued_at(
        {"forze_enqueued_at": {"StringValue": "nope", "DataType": "String"}},
        {"SentTimestamp": "1700000000000"},
    )
    assert bad_iso == datetime.fromtimestamp(1_700_000_000, tz=timezone.utc)


def test_chunked_ids() -> None:
    assert SQSClient._SQSClient__chunked_ids(["a", "b", "c"], size=2) == [
        ["a", "b"],
        ["c"],
    ]


@pytest.mark.asyncio
async def test_client_nested_reuses_bound_client() -> None:
    client = SQSClient()
    await client.initialize(
        endpoint="http://localhost:4566",
        access_key_id="k",
        secret_access_key="s",
        region_name="us-east-1",
    )

    class _FakeSqs:
        async def list_queues(self, **kwargs: object) -> dict[str, list[str]]:
            return {"QueueUrls": []}

    fake = _FakeSqs()
    tok_c = client._SQSClient__ctx_client.set(fake)  # type: ignore[attr-defined]
    tok_d = client._SQSClient__ctx_depth.set(1)
    try:
        async with client.client() as c:
            assert c is fake
            assert client._SQSClient__ctx_depth.get() == 2  # type: ignore[attr-defined]
    finally:
        client._SQSClient__ctx_depth.reset(tok_d)  # type: ignore[attr-defined]
        client._SQSClient__ctx_client.reset(tok_c)  # type: ignore[attr-defined]

    await client.close()


@pytest.mark.asyncio
async def test_health_returns_error_on_list_failure() -> None:
    client = SQSClient()
    await client.initialize(
        endpoint="http://localhost:4566",
        access_key_id="k",
        secret_access_key="s",
        region_name="us-east-1",
    )

    class _FakeSqs:
        async def list_queues(self, **kwargs: object) -> None:
            raise RuntimeError("down")

    tok = client._SQSClient__ctx_client.set(_FakeSqs())  # type: ignore[attr-defined]
    try:
        msg, ok = await client.health()
        assert ok is False
        assert "down" in msg
    finally:
        client._SQSClient__ctx_client.reset(tok)  # type: ignore[attr-defined]

    await client.close()


@pytest.mark.asyncio
async def test_queue_url_uses_in_memory_cache() -> None:
    client = SQSClient()
    await client.initialize(
        endpoint="http://localhost:4566",
        access_key_id="k",
        secret_access_key="s",
        region_name="us-east-1",
    )
    client._SQSClient__queue_url_cache["my-queue"] = "https://x/y/my-queue"  # type: ignore[attr-defined]
    url = await client.queue_url("my-queue")
    assert url == "https://x/y/my-queue"
    await client.close()


@pytest.mark.asyncio
async def test_initialize_without_config_leaves_opts_config_none() -> None:
    client = SQSClient()
    await client.initialize(
        endpoint="http://localhost:4566",
        access_key_id="k",
        secret_access_key="s",
        region_name="us-east-1",
    )
    opts = client._SQSClient__opts  # type: ignore[attr-defined]
    assert opts is not None
    assert opts.config is None
    assert client._SQSClient__enqueue_batch_concurrency == 10  # type: ignore[attr-defined]
    await client.close()


@pytest.mark.asyncio
async def test_initialize_sets_enqueue_concurrency_from_max_pool_connections() -> None:
    client = SQSClient()
    await client.initialize(
        endpoint="http://localhost:4566",
        access_key_id="k",
        secret_access_key="s",
        region_name="us-east-1",
        config={"max_pool_connections": 3},
    )
    assert client._SQSClient__enqueue_batch_concurrency == 3  # type: ignore[attr-defined]
    await client.close()


@pytest.mark.asyncio
async def test_enqueue_many_parallel_batches_respects_concurrency_cap() -> None:
    client = SQSClient()
    await client.initialize(
        endpoint="http://localhost:4566",
        access_key_id="k",
        secret_access_key="s",
        region_name="us-east-1",
        config={"max_pool_connections": 2},
    )

    in_flight = 0
    max_in_flight = 0
    lock = asyncio.Lock()

    async def send_message_batch(**kwargs: object) -> dict[str, list[object]]:
        nonlocal in_flight, max_in_flight
        async with lock:
            in_flight += 1
            max_in_flight = max(max_in_flight, in_flight)
        await asyncio.sleep(0.02)
        async with lock:
            in_flight -= 1
        return {"Failed": []}

    fake = AsyncMock()
    fake.send_message_batch = AsyncMock(side_effect=send_message_batch)

    client._SQSClient__queue_url_cache["q"] = "https://x/q"  # type: ignore[attr-defined]
    tok_c = client._SQSClient__ctx_client.set(fake)  # type: ignore[attr-defined]
    tok_d = client._SQSClient__ctx_depth.set(1)
    try:
        bodies = [b"x"] * 25
        ids = await client.enqueue_many("q", bodies, message_ids=[f"id{i}" for i in range(25)])
        assert len(ids) == 25
        assert fake.send_message_batch.await_count == 3
        assert max_in_flight == 2
    finally:
        client._SQSClient__ctx_depth.reset(tok_d)  # type: ignore[attr-defined]
        client._SQSClient__ctx_client.reset(tok_c)  # type: ignore[attr-defined]

    await client.close()


@pytest.mark.asyncio
async def test_enqueue_many_single_chunk_no_task_group_path() -> None:
    client = SQSClient()
    await client.initialize(
        endpoint="http://localhost:4566",
        access_key_id="k",
        secret_access_key="s",
        region_name="us-east-1",
    )

    fake = AsyncMock()
    fake.send_message_batch = AsyncMock(return_value={"Failed": []})

    client._SQSClient__queue_url_cache["q"] = "https://x/q"  # type: ignore[attr-defined]
    tok_c = client._SQSClient__ctx_client.set(fake)  # type: ignore[attr-defined]
    tok_d = client._SQSClient__ctx_depth.set(1)
    try:
        await client.enqueue_many("q", [b"a", b"b"])
        fake.send_message_batch.assert_awaited_once()
    finally:
        client._SQSClient__ctx_depth.reset(tok_d)  # type: ignore[attr-defined]
        client._SQSClient__ctx_client.reset(tok_c)  # type: ignore[attr-defined]

    await client.close()
