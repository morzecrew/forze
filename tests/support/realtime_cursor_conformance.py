"""The capped-replay boundary battery — one scenario, every document store behind the mailbox.

The scenario itself lives with the code it validates
(``forze_kits.integrations.realtime.conformance``); this is the thin test-side wrapper that
turns it into a battery the mock, Postgres and Mongo legs each run unchanged.

The replay itself is one check, deliberately: the scenario already reduces a run to a frozen
outcome, so splitting it into several assertions would just spread one comparison over
several failures. Tenant-cursor independence gets its own check anyway — it is a separate
guarantee with its own catalogued divergence, and a leg that lost it should say so rather
than report a mismatched replay outcome.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Iterator
from contextlib import contextmanager
from typing import Any, final
from uuid import UUID

import attrs

from forze.application.contracts.tenancy import TenantIdentity
from forze.application.execution import ExecutionContext
from forze_kits.integrations.realtime import build_realtime_cursors, build_realtime_mailbox
from forze_kits.integrations.realtime.conformance import (
    EXPECTED_CURSOR_REPLAY,
    REPLAY_CAP,
    MailboxScope,
    Scoped,
    run_capped_replay_boundary,
    run_tenant_cursor_independence,
)

# ----------------------- #

UNCAPPED = 10**6
"""The observer's cap: high enough that its window is always the whole store."""

REPLAY_PAGE_SIZE = 2
"""Small enough that the replay pages several times, so a page boundary is always crossed."""


def tenant_scoped(
    ctx: ExecutionContext,
    *,
    wrap_mailbox: Callable[[Any], Any] | None = None,
) -> Scoped:
    """The ``Scoped`` factory every leg needs, built once.

    Each leg differs only in how it wires *ctx* — a Postgres table, a Mongo collection, the
    mock store — while the scope itself is the same three builds every time: the capped
    mailbox under test, its cursors, and an uncapped observer over the same rows. Repeating
    that per leg meant four copies of a cap constant that has to agree with the scenario's.

    *wrap_mailbox* decorates the mailbox under test (never the observer), which is how the
    controls inject a deliberately broken replay without restating the wiring.
    """

    @contextmanager
    def _scoped(tenant: UUID) -> Iterator[MailboxScope]:
        with ctx.inv_ctx.bind_identity(tenant=TenantIdentity(tenant_id=tenant)):
            mailbox = build_realtime_mailbox(
                ctx,
                retention=UNSWEPT,
                cap=REPLAY_CAP,
                replay_page_size=REPLAY_PAGE_SIZE,
            )

            yield MailboxScope(
                mailbox=mailbox if wrap_mailbox is None else wrap_mailbox(mailbox),
                cursors=build_realtime_cursors(ctx),
                observer=build_realtime_mailbox(ctx, retention=UNSWEPT, cap=UNCAPPED),
            )

    return _scoped


@final
@attrs.define(slots=True, kw_only=True)
class CursorReplayHarness:
    """One document store behind the mailbox, as a tenant-scoping factory."""

    scoped: Scoped
    """Enter a tenant's scope and yield its mailbox, cursors and uncapped observer."""

    backend: str
    """Label used in failure messages, so a leg names itself."""


Check = Callable[[CursorReplayHarness], Awaitable[None]]


# ....................... #


async def check_a_capped_replay_survives_a_live_ack(h: CursorReplayHarness) -> None:
    """The whole leg: overflow the cap, replay, ack a live frame mid-stream, then trim.

    Compared against a constant rather than against another store, so the mock leg fails in
    the unit suite the moment it stops agreeing with a Postgres nobody started.
    """

    outcome = await run_capped_replay_boundary(h.scoped)

    assert outcome == EXPECTED_CURSOR_REPLAY, f"{h.backend}: {outcome}"


async def check_tenant_cursors_are_independent(h: CursorReplayHarness) -> None:
    """One principal on one device, present in two tenants, keeps two read positions.

    Separate from the replay check because it is a separate guarantee — the cursor id is
    derived so concurrent first-acks converge on one row, and leaving the tenant out of that
    derivation makes two tenants share it. The mock cannot demonstrate the collision (it
    hard-partitions per tenant), so only the real stores prove the guarantee holds; the mock
    leg pins that the probe itself still reports cleanly.
    """

    assert await run_tenant_cursor_independence(h.scoped) is True, h.backend


CURSOR_REPLAY_BATTERY: tuple[Check, ...] = (
    check_a_capped_replay_survives_a_live_ack,
    check_tenant_cursors_are_independent,
)
from tests.support.realtime_retention import UNSWEPT
