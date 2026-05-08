from datetime import timedelta
from typing import Awaitable, Protocol, runtime_checkable

from .specs import DistributedLockSpec

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
    """Contract for commanding distributed locks."""

    spec: DistributedLockSpec
    """Specification for the distributed lock."""

    # ....................... #

    def acquire(self, key: str, owner: str) -> Awaitable[bool]:
        """Acquire the lock on the given key for the given owner."""

        ...

    def release(self, key: str, owner: str) -> Awaitable[bool]:
        """Release the lock on the given key held by the given owner."""

        ...

    def reset(self, key: str, owner: str) -> Awaitable[bool]:
        """Reset the time-to-live of the lock held by the given owner."""

        ...
