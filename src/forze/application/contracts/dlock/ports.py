from collections.abc import Awaitable
from datetime import timedelta
from typing import Final, Protocol, runtime_checkable

from forze.base.exceptions import exc

from .specs import DistributedLockSpec
from .value_objects import AcquiredLock, DistributedLockCapabilities

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
    monotonically increasing per key **across lock generations** while the backend
    keeps its counter (every fresh acquisition gets a strictly higher token;
    extending a live lease via :meth:`reset` keeps the same token).

    Consumers protect downstream writes by sending the token with the write, and the
    store keeps the highest fence per resource:

    - a token's **first** write claims it: the stored fence must be strictly lower
      (``fence < :token``). Zero rows means another holder has the token — end the
      term without writing;
    - **later** writes under the same token accept an equal fence
      (``fence <= :token``), since the holder's own claim set it;
    - a **lower** token is always refused.

    The claim is what keeps a repeated token harmless. A backend that loses its data
    restarts its counter, and a holder may have drawn a token it never wrote, so after
    a loss two holders can draw the same token (see :class:`FenceAdvancing`); the store
    serializes their claims and exactly one wins. The framework cannot enforce these
    checks — they are the consumer's storage-side responsibility.
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


# ....................... #


@runtime_checkable
class FencingAware(Protocol):
    """Opt-in extension for lock command ports that report their fencing capability.

    Kept separate from :class:`DistributedLockCommandPort` so a backend opts in only when
    it can report — the kernel checks a spec's ``requires_fencing_token`` against
    :meth:`capabilities` at first resolve (fail-closed). Mirrors transaction
    :class:`~forze.application.contracts.transaction.IsolationAware`.
    """

    def capabilities(self) -> DistributedLockCapabilities:
        """Report the lock features (fencing tokens, ...) this backend supports."""
        ...


# ....................... #

FENCE_MAX_VALUE: Final[int] = 2**53 - 1
"""Largest ``past`` :meth:`FenceAdvancing.advance_fence` accepts.

A double holds every integer up to here exactly, and Redis scripts compare numbers as
doubles, so a larger value could compare equal to its neighbour."""

FENCE_OUT_OF_RANGE_CODE: Final[str] = "dlock.fence_out_of_range"
"""Error code for a ``past`` outside ``[0, FENCE_MAX_VALUE]``, raised by every backend."""


def validate_fence_past(past: int) -> None:
    """Refuse a ``past`` that is not an integer in ``[0, FENCE_MAX_VALUE]``.

    Backends call it before touching their store, so a bad value is refused the same way
    everywhere and never reaches the counter. ``bool`` is refused though it is an ``int``.

    :param past: The fence the caller wants the counter moved to.
    :raises CoreException: ``precondition`` with code :data:`FENCE_OUT_OF_RANGE_CODE`.
    """

    if isinstance(past, bool) or not isinstance(past, int) or not 0 <= past <= FENCE_MAX_VALUE:
        raise exc.precondition(
            f"Fence advance needs an integer in [0, {FENCE_MAX_VALUE}], got {past!r}.",
            code=FENCE_OUT_OF_RANGE_CODE,
        )


# ....................... #


@runtime_checkable
class FenceAdvancing(Protocol):
    """Opt-in extension for lock command ports that can move a key's fencing counter up.

    A backend that loses its data restarts its counter, and a store that refuses tokens
    lower than its highest fence then refuses every new holder until the counter climbs
    back. Before acquiring, a fenced writer reads the highest fence its store holds for the
    key and passes it here, so the next token is above it. With the counter intact the call
    changes nothing.

    The next token can still repeat one a holder drew before the loss and never wrote; the
    claim rule on :class:`DistributedLockCommandPort` makes that harmless.

    Reported by :attr:`DistributedLockCapabilities.fence_advance`, and required by a spec
    with ``requires_fence_advance``. The port a command accessor returns is a guarded
    stand-in that a protocol ``isinstance`` check never matches, so set the spec flag and
    narrow with ``cast(FenceAdvancing, port)`` rather than probing.
    """

    async def advance_fence(self, key: str, *, past: int) -> int:
        """Move the key's fence counter to at least ``past``.

        The next token :meth:`~DistributedLockCommandPort.acquire` draws for the key is
        greater than ``past``. Never lowers the counter, so racing calls settle on the
        largest value, and needs no lock ownership.

        :param key: Logical lock key, as passed to ``acquire``.
        :param past: The highest fence the store holds for this key; an integer in
            ``[0, FENCE_MAX_VALUE]``, checked before anything is sent.
        :returns: The counter after the call.
        :raises CoreException: ``precondition`` (``dlock.fence_out_of_range``) for a bad
            ``past``.
        """
        ...  # pragma: no cover
