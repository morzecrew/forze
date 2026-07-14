"""`RedisCounterAdminAdapter` enumerates a namespace's counters against a real Redis.

Every hazard this adapter has to survive lives in Redis' ``SCAN`` and in the shape of the
counter key, and **none of them exist in the mock** (a dict scan cannot alias, cannot return a
key twice, and has no glob):

- ``MATCH`` is a *glob*, not a prefix — a namespace that merely starts like another one would
  otherwise have its counters exported under the wrong route's name.
- A glob metacharacter in the namespace makes the pattern match the wrong keys, or none at all
  — and "none" reads as "this application has no counters", which is exactly the silent
  incompleteness a portable export must never produce.
- The unsuffixed counter's key *is* the prefix that every suffixed one extends, so it is both
  the easiest entry to lose and the one most applications actually use.
"""

from __future__ import annotations

from uuid import uuid4

import pytest

from forze_redis.adapters import RedisCounterAdapter, RedisCounterAdminAdapter
from forze_redis.kernel.client import RedisClient

# ----------------------- #


def _pair(
    client: RedisClient, namespace: str
) -> tuple[RedisCounterAdapter, RedisCounterAdminAdapter]:
    """The allocation port and the admin port over the *same* namespace."""

    return (
        RedisCounterAdapter(client=client, namespace=namespace),
        RedisCounterAdminAdapter(client=client, namespace=namespace),
    )


def _pairs(entries) -> set[tuple[str | None, int]]:
    return {(e.suffix, e.value) for e in entries}


# ....................... #


@pytest.mark.asyncio
async def test_lists_every_suffix_and_the_unsuffixed_counter(
    redis_client: RedisClient,
) -> None:
    counter, admin = _pair(redis_client, f"it:counter:{uuid4().hex[:12]}")

    await counter.incr(by=2)  # the unsuffixed counter
    await counter.incr(suffix="2026")
    await counter.incr_batch(size=5, suffix="2027")

    assert _pairs(await admin.list_counters()) == {(None, 2), ("2026", 1), ("2027", 5)}


@pytest.mark.asyncio
async def test_an_unused_namespace_enumerates_empty(redis_client: RedisClient) -> None:
    _, admin = _pair(redis_client, f"it:counter:{uuid4().hex[:12]}")

    assert list(await admin.list_counters()) == []


@pytest.mark.asyncio
async def test_a_namespace_that_prefixes_another_does_not_swallow_it(
    redis_client: RedisClient,
) -> None:
    # ``SCAN MATCH orders*`` matches ``orders_archive`` too. Without the exact-boundary check
    # the ``orders`` route would export the archive's sequence numbers as its own.
    stem = f"it:counter:{uuid4().hex[:12]}"
    orders, orders_admin = _pair(redis_client, stem)
    archive, archive_admin = _pair(redis_client, f"{stem}_archive")

    await orders.incr(by=3)
    await orders.incr(by=4, suffix="eu")
    await archive.incr(by=99)
    await archive.incr(by=50, suffix="eu")

    assert _pairs(await orders_admin.list_counters()) == {(None, 3), ("eu", 4)}
    assert _pairs(await archive_admin.list_counters()) == {(None, 99), ("eu", 50)}


@pytest.mark.asyncio
async def test_a_glob_metacharacter_in_the_namespace_is_escaped(
    redis_client: RedisClient,
) -> None:
    # A ``[`` unescaped is an unterminated character class: Redis matches nothing, and the
    # counters come back *empty* rather than as an error — an export would carry no sequence
    # numbers and still look complete.
    namespace = f"it:counter:[{uuid4().hex[:8]}]*?"
    counter, admin = _pair(redis_client, namespace)

    await counter.incr(by=11)
    await counter.incr(by=7, suffix="2026")

    assert _pairs(await admin.list_counters()) == {(None, 11), ("2026", 7)}


@pytest.mark.asyncio
async def test_enumeration_survives_more_counters_than_one_scan_step_returns(
    redis_client: RedisClient,
) -> None:
    # ``SCAN``'s ``count`` bounds the work per step, not the keys returned: with many keys the
    # cursor takes several turns, and a step may hand back nothing at all. A loop that stopped
    # on an empty batch would report a subset as the whole set.
    counter, admin = _pair(redis_client, f"it:counter:{uuid4().hex[:12]}")
    expected = set()

    for i in range(250):
        await counter.incr(by=i + 1, suffix=f"s{i}")
        expected.add((f"s{i}", i + 1))

    assert _pairs(await admin.list_counters()) == expected


@pytest.mark.asyncio
async def test_enumerating_never_moves_a_counter(redis_client: RedisClient) -> None:
    counter, admin = _pair(redis_client, f"it:counter:{uuid4().hex[:12]}")
    await counter.incr_batch(size=3)

    assert _pairs(await admin.list_counters()) == {(None, 3)}
    assert _pairs(await admin.list_counters()) == {(None, 3)}
    assert await counter.incr() == 4  # the sequence continued; the reads did not consume


@pytest.mark.asyncio
async def test_export_then_import_continues_the_sequence(
    redis_client: RedisClient,
) -> None:
    """The plane's whole purpose, on real infrastructure: enumerate here, ``reset`` there."""

    source, source_admin = _pair(redis_client, f"it:counter:{uuid4().hex[:12]}")
    target, _ = _pair(redis_client, f"it:counter:{uuid4().hex[:12]}")

    await source.incr_batch(size=9)
    await source.incr_batch(size=3, suffix="2026")

    for entry in await source_admin.list_counters():
        await target.reset(value=entry.value, suffix=entry.suffix)

    # Neither sequence reissues a number the source has already handed out.
    assert await target.incr() == 10
    assert await target.incr(suffix="2026") == 4
