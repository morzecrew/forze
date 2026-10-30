"""Redis cache adapter: pre-encoded bytes come back verbatim, JSON values parsed."""

from datetime import timedelta

import orjson
import pytest

from forze.base.codecs import JsonCodec
from forze_redis.adapters.cache import RedisCacheAdapter
from tests.support.cache_conformance import assert_value_contract

from .test_cache import FakeRedisClient

_MARKER = b"\x00\xffforze:bytes\x00"
"""The stored bytes marker, pinned: changing it breaks entries already written."""


def _adapter(client: FakeRedisClient) -> RedisCacheAdapter:
    return RedisCacheAdapter(
        client=client,  # type: ignore[arg-type]
        namespace="t",
    )


@pytest.mark.asyncio
async def test_value_contract() -> None:
    await assert_value_contract(_adapter(FakeRedisClient()))


@pytest.mark.asyncio
async def test_value_contract_large_batch_thread_path() -> None:
    """``get_many`` over 64 keys decodes on the CPU pool; bytes stay verbatim there too."""

    adapter = _adapter(FakeRedisClient())
    mapping = {f"k{i}": f"v{i}".encode() for i in range(70)}

    await adapter.set_many(mapping)
    await adapter.set_many_versioned({(k, "1"): v for k, v in mapping.items()})
    hits, misses = await adapter.get_many(list(mapping))

    assert misses == []
    assert hits == mapping


@pytest.mark.asyncio
async def test_mset_bodies_stores_bytes_without_json_wrap() -> None:
    client = FakeRedisClient()
    adapter = _adapter(client)
    raw = b'{"id":"1","rev":1}'

    await adapter._RedisCacheAdapter__mset_bodies(  # type: ignore[attr-defined]
        {("doc", "1"): raw},
        ttl=timedelta(seconds=60),
    )

    assert len(client.store) == 1
    stored = next(iter(client.store.values()))
    assert stored == _MARKER + raw


@pytest.mark.asyncio
async def test_json_values_are_stored_as_plain_json() -> None:
    """JSON values keep their wire format: no marker, so older readers still parse them."""

    client = FakeRedisClient()
    adapter = _adapter(client)
    value = {"b": [1, 2], "a": "text"}

    await adapter.set("plain", value)
    await adapter.set_versioned("versioned", "1", value)

    assert set(client.store.values()) == {JsonCodec().dumps(value), b"1"}


@pytest.mark.asyncio
async def test_legacy_unmarked_entries_read_as_json() -> None:
    """Entries written before the bytes marker existed keep reading as parsed JSON."""

    client = FakeRedisClient()
    adapter = _adapter(client)
    kv_key = adapter._RedisCacheAdapter__kv_key("plain")  # type: ignore[attr-defined]
    pointer_key = adapter._RedisCacheAdapter__pointer_key("versioned")  # type: ignore[attr-defined]
    body_key = adapter._RedisCacheAdapter__body_key("versioned", "1")  # type: ignore[attr-defined]
    client.store.update({kv_key: b'{"a":1}', pointer_key: b"1", body_key: b"[1,2]"})

    assert await adapter.get("plain") == {"a": 1}
    assert await adapter.get("versioned") == [1, 2]

    hits, misses = await adapter.get_many(["plain", "versioned"])

    assert hits == {"plain": {"a": 1}, "versioned": [1, 2]}
    assert misses == []


@pytest.mark.asyncio
@pytest.mark.parametrize("raw", [b"\x00binary", b"\x00\xffforze:bin"])
async def test_legacy_raw_bytes_with_a_leading_nul_are_not_taken_for_marked(raw: bytes) -> None:
    """A raw entry written before the marker existed that starts with a NUL, or with part of
    the marker, stays what it was — a decode miss — instead of coming back as wrong bytes."""

    client = FakeRedisClient()
    adapter = _adapter(client)
    client.store[adapter._RedisCacheAdapter__kv_key("legacy")] = raw  # type: ignore[attr-defined]

    assert await adapter.get("legacy") is None


def test_marked_entry_is_a_decode_miss_for_a_json_only_reader() -> None:
    """A reader that predates the marker parses every body as JSON; a marked body must
    fail that parse (the old adapter logs it and treats the key as a miss) rather than
    decode into a wrong value."""

    with pytest.raises(orjson.JSONDecodeError):
        JsonCodec().loads(_MARKER + b'{"a":1}')
