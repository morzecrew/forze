"""The counter differential — one frozen script, one comparable outcome, every engine.

The counter plane already has an agnostic battery of independent checks. This module adds
the other shape the harness supports: a **scenario** that drives a fixed script and returns
a frozen outcome, so "the mock behaves like Postgres" becomes a value comparison rather
than a set of separately-passing assertions. Three scripts, in the order their evidence
gets weaker:

- :func:`run_counter_allocation` — a fixed allocation sequence. Every engine must return
  the same numbers in the same order, and :data:`EXPECTED_ALLOCATION` is that answer
  written down. A constant is the strongest form of this comparison available: it does not
  need the two engines to be present at once, so the mock leg fails in the unit suite the
  moment it stops agreeing with a Postgres nobody started.

- :func:`run_counter_partitions` — two tenants × two suffixes, each driven a *different*
  number of times. Four counters that merge produce a different tuple than four that stay
  apart, so the outcome discriminates between "isolated", "tenant ignored", "suffix
  ignored" and "both ignored" rather than merely detecting inequality. This upgrades the
  tenant/route-in-key rule from a mock-only property to a cross-backend one.

- :func:`run_counter_ceiling` — the int64 boundary, and the one place the engines do not
  agree. What they *do* agree on is recorded in :meth:`CeilingOutcome.portable`: the
  allocation is refused whole and the stored value is untouched. What they disagree on —
  the exception kind — is deliberately left off that comparison and catalogued instead
  (see ``COUNTER_DIVERGENCES``), because the refusal happens inside the store for three of
  the four engines and recovering a uniform kind would mean matching on error text.
"""

from __future__ import annotations

from collections.abc import Callable
from uuid import UUID

import attrs

from forze.application.contracts.counter import (
    COUNTER_MAX_VALUE,
    CounterPort,
)
from forze.base.exceptions import CoreException

# ----------------------- #

CEILING_APPROACH = COUNTER_MAX_VALUE - 1
"""One below the ceiling — the value the crossing script starts from."""

CEILING_OVERSHOOT = 4
"""How far past the ceiling the crossing allocation reaches (a multi-step overshoot).

Deliberately not 1. An ``incr()`` from one below the ceiling *succeeds*, landing exactly on
it; only a step that overshoots can be refused. A boundary test that only ever moves by one
therefore never crosses anything, and the exact-landing control in
:attr:`CeilingOutcome.exact_landing` is what keeps this honest.
"""


# ....................... #


@attrs.frozen(kw_only=True)
class CounterOutcome:
    """The observable result of the fixed allocation script."""

    allocations: tuple[int, ...]
    """The values returned by the three single increments, in order."""

    batch: tuple[int, ...]
    """The values returned by ``incr_batch(4)``."""

    after_decrement: int
    after_reset: int

    final: int
    """The value read back afterwards — a reset that did not stick shows up here."""


# ....................... #


@attrs.frozen(kw_only=True)
class PartitionOutcome:
    """Where four independently-driven counters ended up, labelled by their partition."""

    sequences: tuple[tuple[str, int], ...]
    """``(label, final value)`` in a fixed order; labels are ``tenant/suffix``."""


# ....................... #


@attrs.frozen(kw_only=True)
class CeilingOutcome:
    """What happened when an allocation crossed the int64 ceiling."""

    refused: bool
    """Whether the crossing allocation raised rather than storing an out-of-domain value."""

    value_after: int
    """The value left in the counter — a refusal that poisoned it shows up here."""

    exact_landing: int
    """The result of an allocation that lands exactly ON the ceiling: the positive control.

    Without it, "refused" could equally mean the adapter refuses everything near the top,
    which would be a different bug wearing the same passing test.
    """

    refusal_kind: str
    """The exception kind the engine chose.

    **Not portable, and deliberately outside :meth:`portable`.** Postgres and the Firestore
    adapter classify the crossing as a caller-caused precondition; Redis and Mongo surface
    their store's own error as infrastructure, which the egress policy treats as retryable
    even though no retry can ever succeed. That difference is real, has a consequence, and
    is catalogued rather than papered over — see ``COUNTER_DIVERGENCES``.
    """

    def portable(self) -> tuple[bool, int, int]:
        """The part every engine can honestly promise, and the only part compared."""

        return (self.refused, self.value_after, self.exact_landing)


# ----------------------- #


EXPECTED_ALLOCATION = CounterOutcome(
    allocations=(1, 2, 3),
    batch=(4, 5, 6, 7),
    after_decrement=6,
    after_reset=4,
    final=4,
)
"""The one answer every counter implementation must give to the allocation script."""


EXPECTED_PARTITIONS = PartitionOutcome(
    sequences=(("a/one", 1), ("a/two", 2), ("b/one", 3), ("b/two", 4)),
)
"""Four counters, four different totals — chosen so any merge lands on a different tuple.

If the tenant is dropped from the key the two ``one`` cells share a sequence and both read
4; if the suffix is dropped, each tenant's cells share one and read 3 and 7; if both are
dropped all four read 10. None of those equals this, so the failure names the cause.
"""


EXPECTED_CEILING_PORTABLE = (True, CEILING_APPROACH, COUNTER_MAX_VALUE)
"""Refused, counter untouched, and an exactly-landing allocation still allowed."""


# ----------------------- #


async def run_counter_allocation(counter: CounterPort, *, suffix: str) -> CounterOutcome:
    """Drive the fixed allocation script and return what the engine did."""

    first = await counter.incr(suffix=suffix)
    second = await counter.incr(suffix=suffix)
    third = await counter.incr(suffix=suffix)

    allocations = (first, second, third)
    batch = tuple(await counter.incr_batch(4, suffix=suffix))
    after_decrement = await counter.decr(suffix=suffix)
    after_reset = await counter.reset(4, suffix=suffix)

    return CounterOutcome(
        allocations=allocations,
        batch=batch,
        after_decrement=after_decrement,
        after_reset=after_reset,
        final=await counter.incr(by=0, suffix=suffix),
    )


async def run_counter_partitions(
    for_tenant: Callable[[UUID], CounterPort],
    *,
    tenants: tuple[UUID, UUID],
    suffixes: tuple[str, str],
) -> PartitionOutcome:
    """Drive four counters — two tenants × two suffixes — a different number of times each."""

    first, second = tenants
    one, two = suffixes
    cells = (
        ("a/one", first, one, 1),
        ("a/two", first, two, 2),
        ("b/one", second, one, 3),
        ("b/two", second, two, 4),
    )
    sequences: list[tuple[str, int]] = []

    for _, tenant, suffix, times in cells:
        counter = for_tenant(tenant)

        for _step in range(times):
            await counter.incr(suffix=suffix)

    # Read every cell only after every write, so a merge cannot hide behind ordering:
    # reading each cell right after driving it would show the right number even if a
    # later cell were about to overwrite it.
    for label, tenant, suffix, _ in cells:
        sequences.append((label, await for_tenant(tenant).incr(by=0, suffix=suffix)))

    return PartitionOutcome(sequences=tuple(sequences))


async def run_counter_ceiling(counter: CounterPort, *, suffix: str) -> CeilingOutcome:
    """Take the counter to one below the ceiling, then try to step over it."""

    await counter.reset(CEILING_APPROACH, suffix=suffix)

    refused = False
    kind = "none"

    try:
        await counter.incr(by=CEILING_OVERSHOOT, suffix=suffix)
    except CoreException as error:
        refused = True
        kind = str(error.kind)

    value_after = await counter.incr(by=0, suffix=suffix)

    # The positive control: from the same starting point, a step that lands exactly on the
    # ceiling must still be allowed. A counter that refuses this too is not bounded, it is
    # broken, and the refusal above would have passed either way.
    exact_landing = await counter.incr(suffix=suffix)

    return CeilingOutcome(
        refused=refused,
        value_after=value_after,
        exact_landing=exact_landing,
        refusal_kind=kind,
    )
