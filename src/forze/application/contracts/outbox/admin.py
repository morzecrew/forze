"""Read-only depth and age probes over an outbox route (ops / quiesce surfaces).

Kept **separate** from the operational :class:`~forze.application.contracts.outbox.OutboxQueryPort`,
mirroring the framework's management/data split. Emptiness has, until now, only been
observable through ``claim_pending`` — which *mutates*: it claims. A caller that merely wants
to know whether a route is drained had to race the relay for rows it did not want, so the
question could not be asked safely at all. These reads never claim and never mark, and are
acquirable from a read-only (``QUERY``) operation.
"""

from collections.abc import Awaitable
from datetime import timedelta
from typing import Protocol, final, runtime_checkable

import attrs

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class OutboxDepth:
    """How much undrained work a route is holding.

    Counts the **undrained** buckets only. ``published`` is deliberately absent: no adapter
    deletes a published row and the framework never prunes, so that bucket grows with every
    event the application has ever emitted. Counting it would turn a cheap index seek into a
    scan of the entire history — and it answers nothing, since a published row is gone.
    """

    pending: int = 0
    """Rows waiting to be claimed, **including** rows parked for a future retry.

    Deliberately unlike ``claim_pending``, which only returns rows whose ``available_at``
    has passed: a row backing off is still undelivered work, so a quiesce that ignored it
    would attest an empty outbox while events were still queued behind a retry."""

    processing: int = 0
    """Rows claimed by a relay and not yet marked — in flight, or stranded by a process that
    died mid-batch (``reclaim_stale_processing`` recovers those after a lease)."""

    failed: int = 0
    """Rows parked terminally. They never drain on their own — an operator has to
    ``requeue_failed`` them — so they are reported apart from the deliverable work and must
    never be waited on, or a quiesce would hang forever on a poison row."""

    # ....................... #

    @property
    def undrained(self) -> int:
        """Rows still on their way out: ``pending`` + ``processing``. Never ``failed``."""

        return self.pending + self.processing

    # ....................... #

    @property
    def is_empty(self) -> bool:
        """Whether the route holds no deliverable work."""

        return self.undrained == 0


# ....................... #


@runtime_checkable
class OutboxAdminPort(Protocol):
    """Read-only observability over one outbox route.

    Every probe requires the route's claim index (``(outbox_route, status, …)``) to stay
    cheap. The outbox table is application-owned and the framework creates no indexes — see
    the outbox schema reference. Without that index these degrade to a sequential scan over
    the table's whole published history.
    """

    def has_undrained(self) -> Awaitable[bool]:
        """Whether any row is still ``pending`` or ``processing``.

        An existence check, not a count: polling it (a quiesce loop, a readiness gate) costs
        one index seek no matter how much history the table holds. Prefer it whenever the
        answer wanted is a yes/no.
        """
        ...  # pragma: no cover

    def depth(self) -> Awaitable[OutboxDepth]:
        """Count each undrained bucket."""
        ...  # pragma: no cover

    def oldest_pending_age(self) -> Awaitable[timedelta | None]:
        """Age of the oldest ``pending`` row, or ``None`` when none is pending.

        Distinguishes a busy relay from a wedged or absent one, which a depth alone cannot:
        a backlog that will not fall *and* an age that keeps climbing means nothing is
        relaying. Quiesce reports it when it gives up, so the failure names its cause.
        """
        ...  # pragma: no cover
