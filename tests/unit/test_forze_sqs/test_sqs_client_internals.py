"""Unit tests for :class:`~forze_sqs.kernel.platform.client.SQSClient` helpers (no I/O)."""

from datetime import datetime, timedelta, timezone

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


def test_close_clears_state() -> None:
    client = SQSClient()
    client._SQSClient__queue_url_cache["k"] = "v"  # type: ignore[attr-defined]
    client.close()
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
    client.close()


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
    client.close()


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
