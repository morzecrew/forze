from datetime import timedelta
from typing import Awaitable, Protocol, runtime_checkable

from .specs import DistributedLockSpec
from .value_objects import AcquiredLock

# ----------------------- #


@runtime_checkable
class DistributedLockQueryPort(Protocol):  # pragma: no cover
    """Contract for querying distributed lock state."""

    spec: DistributedLockSpec
    """Specification for the distributed lock."""

    # ....................... #

    def is_locked(self, key: str) -> Awaitable[bool]:
        """Check if the lock is held by any process."""

        ...

    def get_owner(self, key: str) -> Awaitable[str | None]:
        """Get the owner of the lock."""

        ...

    def get_ttl(self, key: str) -> Awaitable[timedelta | None]:
        """Get the remaining time-to-live of a lock."""

        ...


# ....................... #


@runtime_checkable
class DistributedLockCommandPort(Protocol):  # pragma: no cover
    """Contract for commanding distributed locks.

    **Fencing model.** A lock alone is best-effort exclusion: a holder paused by
    GC or a network partition can resume after its lease expired while a new
    holder runs. To close that gap, :meth:`acquire` issues a fencing token —
    monotonically increasing per key **across lock generations** (every fresh
    acquisition gets a strictly higher token; extending a live lease via
    :meth:`reset` keeps the same token). Consumers protect downstream writes by
    sending the token with the write and rejecting, storage-side, any token
    lower than the highest one observed for that resource. The framework cannot
    enforce that check — it is the consumer's storage-side responsibility.
    """

    spec: DistributedLockSpec
    """Specification for the distributed lock."""

    # ....................... #

    def acquire(self, key: str, owner: str) -> Awaitable[AcquiredLock | None]:
        """Acquire the lock on the given key for the given owner.

        Returns an :class:`~.AcquiredLock` carrying the fencing token for the new
        lock generation, or ``None`` when the lock is already held. Backends that
        cannot issue monotonic tokens return ``AcquiredLock(token=None)``.
        """

        ...

    def release(self, key: str, owner: str) -> Awaitable[bool]:
        """Release the lock on the given key held by the given owner.

        Releasing must not reset the per-key fencing counter — tokens stay
        monotonic across generations.
        """

        ...

    def reset(self, key: str, owner: str) -> Awaitable[bool]:
        """Reset the time-to-live of the lock held by the given owner.

        Extends the current lock generation: the fencing token does not change.
        """

        ...
