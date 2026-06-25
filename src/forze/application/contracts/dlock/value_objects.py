"""Value objects for distributed lock contracts."""

from typing import final

import attrs

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class DistributedLockCapabilities:
    """What a distributed-lock backend supports — its reported, fail-closed contract.

    A spec that sets ``requires_fencing_token`` is rejected at first resolve against a
    backend that reports ``fencing_tokens=False`` or does not report capabilities at all
    (not :class:`~forze.application.contracts.dlock.FencingAware`). Mirrors transaction
    :class:`~forze.application.contracts.transaction.TxCapabilities`.
    """

    fencing_tokens: bool
    """Whether ``acquire`` issues monotonic fencing tokens (vs best-effort exclusion with
    ``AcquiredLock.token=None``)."""


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class AcquiredLock:
    """Result of a successful distributed lock acquisition.

    Carries the fencing token issued by the backend for this lock generation.
    Tokens are **monotonically increasing per key across lock generations**:
    every fresh acquisition of the same key (after a release, expiry, or steal)
    yields a strictly higher token, while extending the lease of a live lock
    (``reset``) keeps the token unchanged.

    Consumers protect downstream writes by sending the token along with the
    write and rejecting — storage-side — any token lower than the highest one
    observed for that resource. The framework cannot enforce this check; without
    it the lock remains best-effort mutual exclusion (a paused holder can resume
    after expiry while a new holder runs).
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
