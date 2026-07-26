"""Conformance battery for the counter plane — the mock and every real backend.

Counters looked like the safest plane in the framework: four verbs, no query surface, five
adapters that each had a passing test file. What none of those files did was run the *same*
assertions against more than one backend, and the contract said nothing about the value
domain — so the in-memory mock, which counts with unbounded Python integers, quietly
accepted allocations that no real store can hold. Code written against the mock was correct
right up until production.

That is the specific failure this battery exists to prevent, and the reason it runs against
the mock too. A mock more permissive than the systems it stands in for does not merely fail
to catch bugs; it certifies them.

The claims, in the order a reader should meet them:

1. **a fresh counter starts at zero** — the first ``incr()`` returns 1, and nothing has to
   create it first;
2. **``incr(by=0)`` reads without moving** — the only read the port offers, which the login
   lockout guard depends on;
3. **allocations are distinct and ascending**, singly and in batches;
4. **negative values are legal** — ``decr`` below zero, and a negative ``by``. Every backend
   already agreed here; nothing pinned it, so nothing stopped one from drifting;
5. **``reset`` is absolute**, and a reset outside the domain is refused *before* it is
   stored — the case where Redis silently accepted the write and broke the *next* caller;
6. **the value domain is int64 and leaving it changes nothing** — the divergence that
   started this: the mock accepted 2⁶³, Postgres refused as a precondition, Mongo and Redis
   refused as infrastructure, and Redis's ``reset`` did not refuse at all.

On (6) the battery asserts what every backend can honestly promise: the allocation is
refused and the stored value is untouched. Where the adapter knows the result up front — any
``reset``, and Firestore's read-modify-write — it must be the shared
``counter_value_out_of_range``. Where the store does the arithmetic atomically, the adapter
cannot know before asking, so the *kind* of refusal is still the backend's own; that residue
is deliberate and named here rather than papered over with error-message sniffing.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any, final

import attrs
import pytest

from forze.application.contracts.counter import (
    COUNTER_MAX_VALUE,
    COUNTER_MIN_VALUE,
    COUNTER_VALUE_OUT_OF_RANGE_CODE,
)
from forze.base.exceptions import CoreException

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True)
class CounterHarness:
    """One counter adapter under test, plus a fresh-suffix factory.

    Every check works on its own suffix so a battery run is order-independent and one
    check's leftovers cannot make another pass.
    """

    counter: Any
    """The :class:`CounterPort` implementation."""

    suffix: Callable[[str], str]
    """A suffix unique to this run, so checks never collide across a shared backend."""

    admin: Any = None
    """Optional :class:`CounterAdminPort`; checks that need it skip when absent."""


Check = Callable[[CounterHarness], Awaitable[None]]


# ....................... #


async def check_a_fresh_counter_starts_at_zero(h: CounterHarness) -> None:
    suffix = h.suffix("fresh")

    assert await h.counter.incr(suffix=suffix) == 1
    assert await h.counter.incr(suffix=suffix) == 2


# ....................... #


async def check_incr_by_zero_reads_without_moving(h: CounterHarness) -> None:
    """The port has no read verb, so ``incr(0)`` is the read — and the lockout guard uses it.

    It must not advance the counter. It *does* create it at zero if absent, which is a real
    side effect worth knowing: a pure-looking read materializes a partition that then shows
    up in enumeration and in portable exports.
    """

    suffix = h.suffix("read")
    await h.counter.incr(by=7, suffix=suffix)

    assert await h.counter.incr(by=0, suffix=suffix) == 7
    assert await h.counter.incr(by=0, suffix=suffix) == 7
    assert await h.counter.incr(suffix=suffix) == 8


# ....................... #


async def check_allocations_are_distinct_and_ascending(h: CounterHarness) -> None:
    suffix = h.suffix("alloc")

    first = await h.counter.incr(suffix=suffix)
    batch = await h.counter.incr_batch(3, suffix=suffix)
    after = await h.counter.incr(suffix=suffix)

    assert batch == [first + 1, first + 2, first + 3]
    assert after == batch[-1] + 1
    assert len({first, *batch, after}) == 5, "no value may be handed out twice"


# ....................... #


async def check_a_batch_of_one_is_legal_and_zero_is_not(h: CounterHarness) -> None:
    suffix = h.suffix("batch1")

    assert len(await h.counter.incr_batch(1, suffix=suffix)) == 1

    with pytest.raises(CoreException, match="at least 1"):
        await h.counter.incr_batch(0, suffix=suffix)


# ....................... #


async def check_counters_may_go_negative(h: CounterHarness) -> None:
    """Counters are not natural numbers, and every backend already agreed — but nothing
    pinned it, so nothing would have caught one drifting to a clamp or a refusal."""

    below = h.suffix("below")
    assert await h.counter.decr(suffix=below) == -1
    assert await h.counter.decr(by=4, suffix=below) == -5

    negative_by = h.suffix("negby")
    assert await h.counter.incr(by=-5, suffix=negative_by) == -5

    assert await h.counter.reset(-42, suffix=h.suffix("negreset")) == -42


# ....................... #


async def check_reset_is_absolute(h: CounterHarness) -> None:
    suffix = h.suffix("reset")
    await h.counter.incr(by=100, suffix=suffix)

    assert await h.counter.reset(4, suffix=suffix) == 4
    assert await h.counter.incr(suffix=suffix) == 5

    # Absolute, not additive — re-applying the same reset is idempotent, which is what lets
    # a portable import replay without inflating the sequence.
    assert await h.counter.reset(4, suffix=suffix) == 4
    assert await h.counter.incr(suffix=suffix) == 5


# ....................... #


async def check_a_reset_outside_the_domain_is_refused_before_it_is_stored(
    h: CounterHarness,
) -> None:
    """The value is known up front, so every backend must refuse it with the same error.

    Redis previously accepted it — ``GETSET`` is not bounds-checked — and the counter then
    broke on the *next* allocation, in a different call, for a different caller. A refusal
    that arrives late and elsewhere is worse than no refusal at all.
    """

    suffix = h.suffix("bigreset")
    await h.counter.incr(by=5, suffix=suffix)

    for value in (COUNTER_MAX_VALUE + 1, COUNTER_MIN_VALUE - 1, 2**70):
        with pytest.raises(CoreException) as refused:
            await h.counter.reset(value, suffix=suffix)

        assert refused.value.code == COUNTER_VALUE_OUT_OF_RANGE_CODE

    # Untouched, and still usable — the refusal did not poison it.
    assert await h.counter.incr(suffix=suffix) == 6


# ....................... #


async def check_the_value_domain_is_int64(h: CounterHarness) -> None:
    """The divergence this battery was written for.

    The mock counted past 2⁶³ happily; no real backend can. What every backend can promise
    is asserted here: the allocation is refused, and the counter is left exactly where it
    was — so a caller that retries or falls back is reasoning about a value that still means
    something.
    """

    high = h.suffix("hi")
    assert await h.counter.reset(COUNTER_MAX_VALUE, suffix=high) == COUNTER_MAX_VALUE

    with pytest.raises(CoreException):
        await h.counter.incr(suffix=high)

    assert await h.counter.incr(by=0, suffix=high) == COUNTER_MAX_VALUE, "refused, not applied"

    low = h.suffix("lo")
    assert await h.counter.reset(COUNTER_MIN_VALUE, suffix=low) == COUNTER_MIN_VALUE

    with pytest.raises(CoreException):
        await h.counter.decr(suffix=low)

    assert await h.counter.incr(by=0, suffix=low) == COUNTER_MIN_VALUE

    # A batch that would cross the ceiling is refused whole — no partial allocation.
    edge = h.suffix("edge")
    await h.counter.reset(COUNTER_MAX_VALUE - 1, suffix=edge)

    with pytest.raises(CoreException):
        await h.counter.incr_batch(5, suffix=edge)

    assert await h.counter.incr(by=0, suffix=edge) == COUNTER_MAX_VALUE - 1


# ....................... #


async def check_suffixes_are_independent_partitions(h: CounterHarness) -> None:
    one, two = h.suffix("part1"), h.suffix("part2")

    await h.counter.incr(by=10, suffix=one)
    await h.counter.incr(by=3, suffix=two)

    assert await h.counter.incr(by=0, suffix=one) == 10
    assert await h.counter.incr(by=0, suffix=two) == 3


COUNTER_BATTERY: tuple[Check, ...] = (
    check_a_fresh_counter_starts_at_zero,
    check_incr_by_zero_reads_without_moving,
    check_allocations_are_distinct_and_ascending,
    check_a_batch_of_one_is_legal_and_zero_is_not,
    check_counters_may_go_negative,
    check_reset_is_absolute,
    check_a_reset_outside_the_domain_is_refused_before_it_is_stored,
    check_the_value_domain_is_int64,
    check_suffixes_are_independent_partitions,
)
"""Every check. The mock runs them as a unit test; each real backend runs them live."""
