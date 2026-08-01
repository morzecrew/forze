"""Controls for the capped-replay boundary scenario — it must catch the bug it was built for.

Every store passes this scenario today, which is exactly when a scenario is indistinguishable
from one that checks nothing. These reconstruct the fault the fifth-edition audit found — a
replay that delivers an *incomplete* window while a live cumulative ack races it — and assert
the outcome names it, field by field.

The fault is injected at the mailbox rather than simulated in the assertions, so what is being
tested is the scenario's ability to observe a broken store, not its arithmetic.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Iterator
from contextlib import contextmanager
from typing import Any
from uuid import UUID

import attrs
import pytest
import pytest_asyncio

from forze.application.contracts.realtime import MailboxEntry
from forze.application.execution import DepsRegistry, ExecutionRuntime
from forze.base.exceptions import CoreException, exc
from forze_kits.integrations.realtime import realtime_cursor_spec, realtime_mailbox_spec
from forze_kits.integrations.realtime.conformance import (
    BACKLOG_OVERSHOOT,
    CURSOR_STALLED_CODE,
    EXPECTED_CURSOR_REPLAY,
    REPLAY_CAP,
    MailboxScope,
    Scoped,
    _is_complete_suffix,
    _overflowed,
    run_capped_replay_boundary,
    run_tenant_cursor_independence,
)
from forze_mock.execution import MockDepsModule, MockRouteConfig
from tests.support.realtime_cursor_conformance import tenant_scoped

# ----------------------- #


@attrs.frozen
class _Entry:
    """The one field :func:`_is_complete_suffix` reads."""

    event_id: str


@attrs.define(slots=True)
class _TruncatingMailbox:
    """A mailbox whose replay stops early — the pre-fix, oldest-first-limited read.

    Everything else delegates, so the only difference from a correct store is that the
    delivered window is a *prefix* rather than the newest-``cap`` suffix. That is precisely
    the shape that makes a cumulative ack a lie.
    """

    inner: Any
    keep: int

    async def store(self, **kwargs: Any) -> None:
        await self.inner.store(**kwargs)

    async def read_since(self, **kwargs: Any) -> list[MailboxEntry]:
        return await self.inner.read_since(**kwargs)

    async def trim(self, **kwargs: Any) -> None:
        await self.inner.trim(**kwargs)

    async def position_of(self, **kwargs: Any) -> Any:
        # Delegated so the wrapper still satisfies the whole RealtimeMailbox surface —
        # a partial stand-in would be testing the scenario against a shape no store has.
        return await self.inner.position_of(**kwargs)

    def stats(self) -> Any:
        return self.inner.stats()

    async def replay_since(self, **kwargs: Any) -> AsyncIterator[MailboxEntry]:
        sent = 0

        async for entry in self.inner.replay_since(**kwargs):
            if sent >= self.keep:
                return

            sent += 1

            yield entry


def _routes() -> dict[str, MockRouteConfig]:
    return {
        str(realtime_mailbox_spec().name): MockRouteConfig(tenant_aware=True),
        str(realtime_cursor_spec().name): MockRouteConfig(tenant_aware=True),
    }


@pytest_asyncio.fixture
async def scoped_factory() -> AsyncIterator[Any]:
    runtime = ExecutionRuntime(
        deps=DepsRegistry.from_modules(MockDepsModule(routes=_routes())).freeze()
    )

    async with runtime.scope():
        ctx = runtime.get_context()

        def _make(truncate_to: int | None) -> Scoped:
            return tenant_scoped(
                ctx,
                wrap_mailbox=(
                    None
                    if truncate_to is None
                    else lambda mailbox: _TruncatingMailbox(inner=mailbox, keep=truncate_to)
                ),
            )

        yield _make


# ....................... #


async def test_a_correct_store_matches_the_expected_outcome(scoped_factory: Any) -> None:
    """The positive control. Without it every failure below could mean anything."""

    assert await run_capped_replay_boundary(scoped_factory(None)) == EXPECTED_CURSOR_REPLAY


async def test_a_truncated_replay_loses_undelivered_entries(scoped_factory: Any) -> None:
    """The edition-5 fault, reconstructed: incomplete window + live cumulative ack.

    Each field is asserted separately rather than as one inequality, because they say
    different things and a reader has to be able to tell which guarantee broke: the window
    was incomplete, the cursor came to rest past entries never sent, and the trim then
    deleted them.
    """

    outcome = await run_capped_replay_boundary(scoped_factory(2))

    assert outcome.replayed_complete_suffix is False
    assert outcome.cursor_crossed_undelivered is True
    assert outcome.undelivered_deleted > 0, (
        "a cumulative ack past an incomplete window must be observed deleting real entries"
    )


async def test_the_loss_count_is_exact(scoped_factory: Any) -> None:
    """Not merely non-zero: the count is the number of entries actually skipped.

    A scenario that reported "some loss" for any truncation could be counting the wrong
    thing entirely — the cap's declared retention loss, say, which is not a fault.
    """

    keep = 2
    outcome = await run_capped_replay_boundary(scoped_factory(keep))

    assert outcome.undelivered_deleted == REPLAY_CAP - keep


async def test_entries_below_the_window_floor_are_not_counted_as_loss(
    scoped_factory: Any,
) -> None:
    """The cap's own retention loss is declared, not a fault, and must not inflate the count.

    The backlog is deliberately larger than the cap, so a correct run *does* delete entries
    that were never delivered — the ones below the window floor. Counting those would make
    the metric fire on every healthy overflow and the leg would be unusable.
    """

    outcome = await run_capped_replay_boundary(scoped_factory(None))

    assert outcome.overflowed == 1, "the cap must have engaged, or this proves nothing"
    assert outcome.undelivered_deleted == 0


@pytest.mark.parametrize("keep", [1, 3, 4])
async def test_any_truncation_is_caught(scoped_factory: Any, keep: int) -> None:
    """Not just the convenient one: every incomplete window is a fault, wherever it stops."""

    outcome = await run_capped_replay_boundary(scoped_factory(keep))

    assert outcome.undelivered_deleted == REPLAY_CAP - keep


async def test_a_replay_that_delivers_nothing_is_counted_as_total_loss(
    scoped_factory: Any,
) -> None:
    """The degenerate truncation, where the window floor has no replayed entry to come from.

    The worst store there is — it sends none of the backlog — and the one the arithmetic
    could most easily excuse: with no replayed entry the floor has to be *assumed*, and
    assuming the live frame's position would put it above every seeded entry and report a
    clean run while the trim deleted the whole backlog. The floor falls back to the bottom
    of the backlog instead, so every entry the cumulative ack covered counts.
    """

    outcome = await run_capped_replay_boundary(scoped_factory(0))

    assert outcome.replayed_count == 0
    assert outcome.replayed_complete_suffix is False
    assert outcome.cursor_crossed_undelivered is True
    assert outcome.undelivered_deleted == REPLAY_CAP + BACKLOG_OVERSHOOT, (
        "nothing was delivered, so every seeded entry the trim removed is a loss"
    )


# ....................... #
# The tenant probe's own failure branches — the paths that fire only when isolation breaks.


@attrs.define(slots=True)
class _SharedCursors:
    """Cursors that ignore the tenant — one row per (principal, client_key), globally.

    Exactly what a tenant-blind derived id produces on a shared table: the second tenant
    sees the first's read position, and its later ack drags the first tenant forward.
    """

    _positions: dict[tuple[str, str], Any] = attrs.field(factory=dict)

    async def get(self, *, principal: str, client_key: str) -> Any:
        return self._positions.get((principal, client_key))

    async def advance(self, *, principal: str, client_key: str, up_to: Any) -> None:
        current = self._positions.get((principal, client_key))

        if current is None or up_to > current:
            self._positions[(principal, client_key)] = up_to

    async def min_cursor(self, *, principal: str) -> Any:
        return min(self._positions.values(), default=None)


@attrs.define(slots=True)
class _ScriptedCursors:
    """Answers the probe's fixed call sequence, failing at one chosen step.

    The probe makes the same five calls every time — A.advance, A.get, B.get, B.advance,
    A.get — so a double can be precise about *which* tenant it is answering without seeing
    the ambient tenant at all. Being precise matters: an earlier double that simply failed
    every advance made the stall test pass through the setup-check branch instead of the
    stall branch, so it was covering the wrong thing while looking correct.

    The default script is a healthy, isolated store: the first tenant's ack lands, the
    second sees nothing of it, and the second's ack does not disturb the first.
    """

    on_second_advance: Any = None

    _advances: int = 0
    _gets: int = 0
    _position: Any = None

    async def get(self, *, principal: str, client_key: str) -> Any:
        self._gets += 1

        # The second get is tenant B's: an isolated store shows it nothing.
        return None if self._gets == 2 else self._position

    async def advance(self, *, principal: str, client_key: str, up_to: Any) -> None:
        self._advances += 1

        if self._advances == 1:
            self._position = up_to

            return

        if self.on_second_advance is not None:
            self.on_second_advance()

    async def min_cursor(self, *, principal: str) -> Any:
        return self._position


def _stalls() -> None:
    """How a colliding derived id presents: the retry budget runs out, no wrong value."""

    raise exc.internal("did not converge", code=CURSOR_STALLED_CODE)


def _falls_over() -> None:
    raise exc.infrastructure("the cursor store is down")


def _scope_with(cursors: Any, inner: Scoped) -> Scoped:
    """The real mailbox scope, with its cursors swapped for a broken pair."""

    @contextmanager
    def _scoped(tenant: UUID) -> Iterator[MailboxScope]:
        with inner(tenant) as scope:
            yield attrs.evolve(scope, cursors=cursors)

    return _scoped


async def test_a_tenant_blind_cursor_store_is_reported_as_not_independent(
    scoped_factory: Any,
) -> None:
    """The headline failure: one row shared by two tenants."""

    scoped = _scope_with(_SharedCursors(), scoped_factory(None))

    assert await run_tenant_cursor_independence(scoped) is False


async def test_the_scripted_double_reports_a_healthy_store_as_independent(
    scoped_factory: Any,
) -> None:
    """The positive control for the doubles below — otherwise every one could pass by luck."""

    scoped = _scope_with(_ScriptedCursors(), scoped_factory(None))

    assert await run_tenant_cursor_independence(scoped) is True


async def test_a_stalled_advance_is_reported_as_not_independent(scoped_factory: Any) -> None:
    """The indirect failure: the advance loop exhausts its budget instead of returning wrong data."""

    scoped = _scope_with(_ScriptedCursors(on_second_advance=_stalls), scoped_factory(None))

    assert await run_tenant_cursor_independence(scoped) is False


async def test_an_unrelated_failure_from_the_cursor_store_propagates(
    scoped_factory: Any,
) -> None:
    """Only the stall code is a verdict; any other fault must not be read as a clean "False".

    A cursor store failing for an unrelated reason would otherwise be reported as a tenancy
    finding — a wrong diagnosis pointing at code that is not broken.
    """

    scoped = _scope_with(_ScriptedCursors(on_second_advance=_falls_over), scoped_factory(None))

    with pytest.raises(CoreException) as propagated:
        await run_tenant_cursor_independence(scoped)

    assert propagated.value.code != CURSOR_STALLED_CODE


async def test_a_setup_that_does_not_take_is_not_read_as_independence(
    scoped_factory: Any,
) -> None:
    """A probe whose own setup silently failed must not report a pass.

    This is the bug the probe shipped with: it shared the replay's device, whose cursor the
    mid-replay ack had already pushed past the probe's position, so the monotonic guard made
    the setup a no-op. Reading that as a result is worse than failing.
    """

    @attrs.define(slots=True)
    class _IgnoresEveryAdvance(_ScriptedCursors):
        async def advance(self, *, principal: str, client_key: str, up_to: Any) -> None:
            return None

    scoped = _scope_with(_IgnoresEveryAdvance(), scoped_factory(None))

    assert await run_tenant_cursor_independence(scoped) is False


async def test_a_mailbox_without_counters_reports_no_overflows(scoped_factory: Any) -> None:
    """``stats()`` is optional: a store that keeps no counters still runs the scenario."""

    @attrs.define(slots=True)
    class _Countless:
        inner: Any

        def __getattr__(self, name: str) -> Any:
            return getattr(self.inner, name)

    assert _overflowed(_Countless(inner=object())) == 0


@pytest.mark.parametrize(
    ("replayed", "stored", "expected", "why"),
    [
        ((), (), True, "an empty store has nothing to deliver, so nothing IS the suffix"),
        ((), ("a", "b"), False, "delivering nothing from a full store is not a complete suffix"),
        (("b",), ("a", "b"), True, "the tail of one"),
        (("a",), ("a", "b"), False, "a prefix is not a suffix"),
        (("a", "b"), ("a", "b"), True, "the whole store"),
    ],
)
def test_the_suffix_test_refuses_the_degenerate_empty_case(
    replayed: tuple[str, ...],
    stored: tuple[str, ...],
    expected: bool,
    why: str,
) -> None:
    """The empty list is a tail of every sequence — which is the trap.

    Taking "the last zero entries" and comparing would call a replay that delivered nothing
    a complete suffix, and that is precisely the state a badly truncating store lands in.
    Covered directly because the scenario always seeds a backlog, so it can never produce
    the empty-store case itself.
    """

    entries = [_Entry(event_id=e) for e in replayed]
    corpus = [_Entry(event_id=e) for e in stored]

    assert _is_complete_suffix(entries, corpus) is expected, why
