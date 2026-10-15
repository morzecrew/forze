"""Port for fleet membership: which replicas of a service are alive, and in what state.

A member is alive exactly while its entry has not expired on the backend's clock — there is
no separate failure detector. A replica keeps its entry alive by beating within the fleet's
``ttl``; one that stops beating drops out of every view once its entry expires.

**Semantics every backend agrees on**, pinned by the shared conformance battery:

- one :meth:`FleetPort.beat` removes expired entries, writes this member's, and returns the
  view, atomically and on the backend's clock;
- ``since`` survives beats in the same state and resets on a change of state;
- a beat's ``ttl`` sets only that member's expiry, so a short final beat never shortens
  another member's life;
- an invalid member id or report is refused before anything is sent, and report numbers
  read back exactly as they were reported.
"""

from collections.abc import Awaitable
from datetime import timedelta
from typing import Protocol, runtime_checkable

from .value_objects import FleetView, MemberState, Report

# ----------------------- #


@runtime_checkable
class FleetPort(Protocol):
    """One fleet's membership."""

    def beat(
        self,
        member_id: str,
        state: MemberState,
        report: Report,
        *,
        ttl: timedelta | None = None,
    ) -> Awaitable[FleetView]:
        """Write this member's entry and return the fleet's view, in one atomic call.

        :param member_id: This member's id.
        :param state: This member's state; ``since`` is kept while it is unchanged.
        :param report: What this member publishes with its entry.
        :param ttl: How long this entry lives, or ``None`` for the spec's ``ttl``.
        :returns: The live members, this one included.
        """
        ...  # pragma: no cover

    def view(self) -> Awaitable[FleetView]:
        """Read the live members without writing anything."""
        ...  # pragma: no cover

    def leave(self, member_id: str) -> Awaitable[None]:
        """Remove this member's entry at once, so peers stop counting it immediately."""
        ...  # pragma: no cover
