"""`CounterAdminPort` enumerates a spec's allocated counters (mock).

The port exists because a counter is the one plane that is *durable state with no read verb*:
``CounterPort`` deliberately refuses to let a handler read a value (it would be stale the
instant it was read), and the consequence was that nothing could read one at all — so a
migration rebuilt every other plane and left the counters at zero, reissuing invoice numbers
that were already in customers' hands. These tests pin the two properties that has to keep at
once: operators can enumerate, handlers still cannot read.
"""

from __future__ import annotations

import pytest

from forze import build_runtime
from forze.application.contracts.counter import (
    CounterAdminPort,
    CounterEntry,
    CounterPort,
    CounterSpec,
)
from forze_mock import MockDepsModule

# ----------------------- #

INVOICES = CounterSpec(name="invoice_no")
ORDERS = CounterSpec(name="order_no")


def _pairs(entries) -> set[tuple[str | None, int]]:
    return {(e.suffix, e.value) for e in entries}


# ....................... #


class TestEnumeration:
    async def test_lists_every_suffix_and_the_unsuffixed_counter(self) -> None:
        runtime = build_runtime(MockDepsModule())

        async with runtime.scope():
            ctx = runtime.get_context()
            counter = ctx.counter(INVOICES)

            await counter.incr()
            await counter.incr()  # the unsuffixed counter is a real counter: 2
            await counter.incr(suffix="2026")
            await counter.incr_batch(5, suffix="2027")

            entries = await ctx.counter.admin(INVOICES).list_counters()

        # ``None`` is a partition, not "no counter" — dropping it would export every
        # partition of a sequence except the one most applications actually use.
        assert _pairs(entries) == {(None, 2), ("2026", 1), ("2027", 5)}

    async def test_an_unused_spec_enumerates_empty(self) -> None:
        runtime = build_runtime(MockDepsModule())

        async with runtime.scope():
            entries = await runtime.get_context().counter.admin(INVOICES).list_counters()

        assert list(entries) == []

    async def test_specs_do_not_bleed_into_each_other(self) -> None:
        runtime = build_runtime(MockDepsModule())

        async with runtime.scope():
            ctx = runtime.get_context()
            await ctx.counter(INVOICES).incr(by=7)
            await ctx.counter(ORDERS).incr(by=3, suffix="eu")

            invoices = await ctx.counter.admin(INVOICES).list_counters()
            orders = await ctx.counter.admin(ORDERS).list_counters()

        assert _pairs(invoices) == {(None, 7)}
        assert _pairs(orders) == {("eu", 3)}

    async def test_the_value_is_the_last_number_handed_out(self) -> None:
        # An import calls ``reset(value)``, and ``incr`` then returns ``value + 1`` — so the
        # exported number must be the last one *allocated*, never the next one free, or every
        # migration reissues exactly one number.
        runtime = build_runtime(MockDepsModule())

        async with runtime.scope():
            ctx = runtime.get_context()
            counter = ctx.counter(INVOICES)

            last = await counter.incr_batch(4)  # allocates 1..4
            entries = await ctx.counter.admin(INVOICES).list_counters()

            assert last[-1] == 4
            assert _pairs(entries) == {(None, 4)}

            # Round-trip: reset to the exported value, and the next allocation continues the
            # sequence rather than repeating it.
            await counter.reset(4)
            assert await counter.incr() == 5


# ....................... #


class TestRoundTrip:
    async def test_export_then_import_continues_the_sequence(self) -> None:
        """The whole point of the plane, end to end: enumerate here, `reset` there."""

        source = build_runtime(MockDepsModule())
        target = build_runtime(MockDepsModule())

        async with source.scope():
            ctx = source.get_context()
            await ctx.counter(INVOICES).incr_batch(9)
            await ctx.counter(INVOICES).incr_batch(3, suffix="2026")

            exported = await ctx.counter.admin(INVOICES).list_counters()

        async with target.scope():
            ctx = target.get_context()
            counter = ctx.counter(INVOICES)

            for entry in exported:
                await counter.reset(entry.value, suffix=entry.suffix)

            # Neither sequence reissues a number the source already handed out.
            assert await counter.incr() == 10
            assert await counter.incr(suffix="2026") == 4


# ....................... #


class TestDoctrine:
    def test_the_read_is_not_on_the_handler_facing_port(self) -> None:
        # The reason the read lives on a separate port at all. A handler holding a
        # ``CounterPort`` still cannot read a value — reading one is a race by construction —
        # while an operator and an export can.
        assert not hasattr(CounterPort, "list_counters")
        assert hasattr(CounterAdminPort, "list_counters")

    async def test_the_admin_port_cannot_allocate(self) -> None:
        runtime = build_runtime(MockDepsModule())

        async with runtime.scope():
            admin = runtime.get_context().counter.admin(INVOICES)

        # Read-only in the other direction too: enumerating counters must never move one.
        assert not hasattr(admin, "incr")
        assert not hasattr(admin, "reset")

    async def test_enumerating_does_not_move_a_counter(self) -> None:
        runtime = build_runtime(MockDepsModule())

        async with runtime.scope():
            ctx = runtime.get_context()
            await ctx.counter(INVOICES).incr_batch(3)

            admin = ctx.counter.admin(INVOICES)
            first = await admin.list_counters()
            second = await admin.list_counters()

            assert _pairs(first) == _pairs(second) == {(None, 3)}
            assert await ctx.counter(INVOICES).incr() == 4


# ....................... #


class TestEntry:
    def test_entry_is_frozen(self) -> None:
        entry = CounterEntry(suffix="2026", value=12)

        with pytest.raises(AttributeError):
            entry.value = 13  # type: ignore[misc]
