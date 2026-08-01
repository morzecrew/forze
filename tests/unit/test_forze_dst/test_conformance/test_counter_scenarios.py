"""The counter scenarios' own controls — each one must be able to fail.

The three counter scenarios pass against the mock and all four real backends, which is
exactly the situation in which a scenario that checks nothing is indistinguishable from one
that checks everything. These tests drive deliberately broken counters through them and
assert the scenario notices — and notices *specifically*, naming which part of the key was
dropped rather than merely reporting inequality.
"""

from __future__ import annotations

from uuid import UUID, uuid4

import pytest

from forze.application.contracts.counter import COUNTER_MAX_VALUE
from forze.base.exceptions import CoreException, exc
from forze_dst.conformance.counters import (
    CEILING_APPROACH,
    EXPECTED_ALLOCATION,
    EXPECTED_CEILING_PORTABLE,
    EXPECTED_PARTITIONS,
    run_counter_allocation,
    run_counter_ceiling,
    run_counter_partitions,
)

# ----------------------- #


class _InMemoryCounter:
    """A counter over a caller-supplied dict, so the KEY is the thing under test."""

    def __init__(self, store: dict[str, int], *, key: str) -> None:
        self._store = store
        self._key = key
        self._bounded = True

    def _cell(self, suffix: str | None) -> str:
        return f"{self._key}|{suffix}"

    async def incr(self, by: int = 1, *, suffix: str | None = None) -> int:
        cell = self._cell(suffix)
        value = self._store.get(cell, 0) + by

        if self._bounded and not -(2**63) <= value < 2**63:
            raise exc.precondition("out of range")

        self._store[cell] = value

        return value

    async def incr_batch(self, size: int = 2, *, suffix: str | None = None) -> list[int]:
        top = await self.incr(by=size, suffix=suffix)

        return [top - size + step + 1 for step in range(size)]

    async def decr(self, by: int = 1, *, suffix: str | None = None) -> int:
        return await self.incr(by=-by, suffix=suffix)

    async def reset(self, value: int = 1, *, suffix: str | None = None) -> int:
        self._store[self._cell(suffix)] = value

        return value


class _WrappingCounter(_InMemoryCounter):
    """The counter the design assumed some engine was: one that silently wraps at int64."""

    async def incr(self, by: int = 1, *, suffix: str | None = None) -> int:
        cell = self._cell(suffix)
        value = self._store.get(cell, 0) + by
        wrapped = ((value + 2**63) % 2**64) - 2**63
        self._store[cell] = wrapped

        return wrapped


# ....................... #


async def test_a_correct_counter_gives_the_expected_allocation() -> None:
    """The positive control: without it, the failures below could mean anything."""

    counter = _InMemoryCounter({}, key="k")

    assert await run_counter_allocation(counter, suffix="s") == EXPECTED_ALLOCATION


async def test_a_counter_whose_reset_is_additive_is_caught() -> None:
    counter = _InMemoryCounter({}, key="k")
    counter.reset = counter.incr  # type: ignore[method-assign]

    outcome = await run_counter_allocation(counter, suffix="s")

    assert outcome != EXPECTED_ALLOCATION
    assert outcome.after_reset != EXPECTED_ALLOCATION.after_reset


# ....................... #


def _factory(store: dict[str, int], *, keyed_by_tenant: bool):
    def build(tenant: UUID):
        return _InMemoryCounter(store, key=str(tenant) if keyed_by_tenant else "shared")

    return build


async def test_two_tenants_over_one_store_stay_apart() -> None:
    store: dict[str, int] = {}

    outcome = await run_counter_partitions(
        _factory(store, keyed_by_tenant=True),
        tenants=(uuid4(), uuid4()),
        suffixes=("one", "two"),
    )

    assert outcome == EXPECTED_PARTITIONS


async def test_a_key_that_drops_the_tenant_is_caught_and_named() -> None:
    """The failure has to be specific: dropping the tenant merges the two ``one`` cells.

    An assertion that only knew the outcome was "different" would leave a reader unable to
    tell a tenant leak from a suffix leak — which is the whole reason the four cells are
    driven a different number of times each.
    """

    store: dict[str, int] = {}

    outcome = await run_counter_partitions(
        _factory(store, keyed_by_tenant=False),
        tenants=(uuid4(), uuid4()),
        suffixes=("one", "two"),
    )

    assert outcome != EXPECTED_PARTITIONS
    assert dict(outcome.sequences) == {"a/one": 4, "a/two": 6, "b/one": 4, "b/two": 6}


async def test_a_key_that_drops_the_suffix_is_caught_and_named() -> None:
    store: dict[str, int] = {}

    def build(tenant: UUID):
        counter = _InMemoryCounter(store, key=str(tenant))
        counter._cell = lambda suffix: counter._key  # type: ignore[method-assign]

        return counter

    outcome = await run_counter_partitions(
        build,
        tenants=(uuid4(), uuid4()),
        suffixes=("one", "two"),
    )

    assert dict(outcome.sequences) == {"a/one": 3, "a/two": 3, "b/one": 7, "b/two": 7}


# ....................... #


async def test_the_ceiling_scenario_accepts_a_bounded_counter() -> None:
    counter = _InMemoryCounter({}, key="k")

    outcome = await run_counter_ceiling(counter, suffix="s")

    assert outcome.portable() == EXPECTED_CEILING_PORTABLE
    assert outcome.exact_landing == COUNTER_MAX_VALUE, "landing ON the ceiling must be allowed"


async def test_a_counter_that_wraps_is_caught() -> None:
    """No shipped engine does this — measured, not assumed — but the check must catch it."""

    counter = _WrappingCounter({}, key="k")

    outcome = await run_counter_ceiling(counter, suffix="s")

    assert outcome.portable() != EXPECTED_CEILING_PORTABLE
    assert outcome.refused is False
    assert outcome.value_after < 0, "the wrap is the finding: a counter that went negative"


class _RefusesNearTheTop(_InMemoryCounter):
    """Refuses every allocation once the counter is near the ceiling, crossing or not."""

    async def incr(self, by: int = 1, *, suffix: str | None = None) -> int:
        if by != 0 and self._store.get(self._cell(suffix), 0) >= CEILING_APPROACH:
            raise exc.precondition("too close to the ceiling")

        return await super().incr(by=by, suffix=suffix)


async def test_a_counter_that_refuses_everything_near_the_top_is_caught() -> None:
    """The positive control earns its place: blanket refusal must not read as bounded.

    Refusing the crossing AND the allocation that lands exactly on the ceiling is a
    different bug wearing the same passing test — the counter is not bounded, it is broken
    a whole allocation early. The scenario reaches the exact-landing step outside its
    try/except, so this surfaces as a failure rather than as a portable-looking outcome.
    """

    with pytest.raises(CoreException, match="too close to the ceiling"):
        await run_counter_ceiling(_RefusesNearTheTop({}, key="k"), suffix="s")
