"""Port for persisting and recovering a node's HLC high-water mark."""

from typing import Awaitable, Protocol, runtime_checkable

from forze.base.primitives import HlcTimestamp

# ----------------------- #


@runtime_checkable
class HlcCheckpointPort(Protocol):
    """Durable high-water mark of the timestamps a node's HLC has issued.

    A :class:`~forze.base.primitives.HybridLogicalClock` keeps its last-issued timestamp
    only in memory, so a process restart resets it to ``(0, 0)``. After that reset the
    clock re-floors at wall time — which silently loses any physical component a peer
    merge, or a since-corrected wall clock, had carried it to, so a restart can re-issue a
    timestamp at or below one the node already emitted (and possibly relayed), breaking the
    clock's monotonicity guarantee exactly when it matters. This port persists the mark so
    a restarted clock resumes above its prior emissions
    (:meth:`~forze.base.primitives.HybridLogicalClock.resume`).

    :meth:`advance` participates in the caller's *business* transaction, so the mark
    commits atomically with the HLC-stamped writes it guards — a committed stamp is never
    durable without a mark that covers it, and a rolled-back one does not advance the mark.
    :meth:`load` reads the recovered floor once at startup.

    The mark only advances (monotonic max), so out-of-order or concurrent writers are safe:
    a lower value never lowers it. Delivery guarantee: resuming from the mark is a *floor*,
    not an exact restore — a mark ahead of the node's true last emission is harmless
    (issuing higher never violates monotonicity), so the store errs toward over-advancing.
    """

    def load(self) -> Awaitable[HlcTimestamp | None]:
        """Return the persisted high-water mark, or ``None`` when none was ever written."""
        ...  # pragma: no cover

    def advance(self, mark: HlcTimestamp) -> Awaitable[None]:
        """Raise the persisted mark to at least *mark* (monotonic max; never lowers).

        Runs inside the business transaction so the mark and the stamped writes commit
        atomically. A *mark* at or below the stored value is a no-op.
        """
        ...  # pragma: no cover
