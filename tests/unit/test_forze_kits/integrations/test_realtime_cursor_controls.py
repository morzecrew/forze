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
from forze.application.contracts.tenancy import TenantIdentity
from forze.application.execution import DepsRegistry, ExecutionRuntime
from forze_kits.integrations.realtime import (
    build_realtime_cursors,
    build_realtime_mailbox,
    realtime_cursor_spec,
    realtime_mailbox_spec,
)
from forze_kits.integrations.realtime.conformance import (
    EXPECTED_CURSOR_REPLAY,
    REPLAY_CAP,
    MailboxScope,
    Scoped,
    run_capped_replay_boundary,
)
from forze_mock.execution import MockDepsModule, MockRouteConfig

# ----------------------- #

UNCAPPED = 10**6


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
            @contextmanager
            def _scoped(tenant: UUID) -> Iterator[MailboxScope]:
                with ctx.inv_ctx.bind_identity(tenant=TenantIdentity(tenant_id=tenant)):
                    mailbox = build_realtime_mailbox(
                        ctx, cap=REPLAY_CAP, replay_page_size=2
                    )

                    yield MailboxScope(
                        mailbox=(
                            mailbox
                            if truncate_to is None
                            else _TruncatingMailbox(inner=mailbox, keep=truncate_to)
                        ),
                        cursors=build_realtime_cursors(ctx),
                        observer=build_realtime_mailbox(ctx, cap=UNCAPPED),
                    )

            return _scoped

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
