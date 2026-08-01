"""The capped-replay boundary battery — one scenario, every document store behind the mailbox.

The scenario itself lives with the code it validates
(``forze_kits.integrations.realtime.conformance``); this is the thin test-side wrapper that
turns it into a battery the mock, Postgres and Mongo legs each run unchanged.

There is only one check, and that is deliberate: the scenario already reduces a run to a
frozen outcome, so splitting it into several assertions would just spread one comparison
over several failures.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import final

import attrs

from forze_kits.integrations.realtime.conformance import (
    EXPECTED_CURSOR_REPLAY,
    Scoped,
    run_capped_replay_boundary,
)

# ----------------------- #


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


CURSOR_REPLAY_BATTERY: tuple[Check, ...] = (check_a_capped_replay_survives_a_live_ack,)
