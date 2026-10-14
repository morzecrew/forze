"""Value objects for distributed lock contracts."""

from typing import final

import attrs

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class DistributedLockCapabilities:
    """What a distributed-lock backend supports — its reported, fail-closed contract.

    A spec that sets ``requires_fencing_token`` (or ``requires_fence_advance``) is rejected
    at first resolve against a backend that reports the matching flag ``False`` or does not
    report capabilities at all (not :class:`~forze.application.contracts.dlock.FencingAware`).
    Mirrors transaction :class:`~forze.application.contracts.transaction.TxCapabilities`.
    """

    fencing_tokens: bool
    """Whether ``acquire`` issues monotonic fencing tokens (vs best-effort exclusion with
    ``AcquiredLock.token=None``)."""

    fence_advance: bool = False
    """Whether the backend implements
    :class:`~forze.application.contracts.dlock.FenceAdvancing`, which moves a key's fencing
    counter up to a store's fence after the backend lost it."""


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class AcquiredLock:
    """Result of a successful distributed lock acquisition.

    Carries the fencing token issued by the backend for this lock generation.
    Tokens are **monotonically increasing per key across lock generations** while
    the backend keeps its counter: every fresh acquisition of the same key (after a
    release, expiry, or steal) yields a strictly higher token, while extending the
    lease of a live lock (``reset``) keeps the token unchanged.

    Consumers protect downstream writes by sending the token along with the write.
    The token's **first** write claims it: the stored fence must be strictly lower
    (``fence < :token``). **Later** writes under the same token accept an equal
    fence (``fence <= :token``). A **lower** token is always refused. **Zero rows**
    on any fenced write, the claim or a later one, means the token is no longer
    current, so the term ends. The claim matters because tokens can repeat after the
    backend loses its data; the store serializes the claims and one holder wins. The
    framework cannot enforce these checks; without them the lock remains best-effort
    mutual exclusion (a paused holder can resume after expiry while a new holder
    runs).
    """

    key: str
    """Logical lock key the token was issued for."""

    owner: str
    """Owner the lock was acquired for (ties extend/release to this generation)."""

    token: int | None
    """Fencing token for this lock generation.

    ``None`` means the backend cannot issue fencing tokens; exclusion is then
    best-effort only — downstream writes have no way to reject a stale holder.
    """
