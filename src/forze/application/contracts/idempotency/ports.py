"""Port for engine-level idempotency handling."""

from collections.abc import Awaitable
from typing import Protocol, runtime_checkable

from .value_objects import IdempotencyRecord

# ----------------------- #


@runtime_checkable
class IdempotencyPort(Protocol):
    """Contract for storing and replaying the result of an idempotent operation.

    Implementations store a result record keyed by an operation identifier, an
    idempotency key, and a payload hash, and replay it when a duplicate request
    is detected.

    Guarantee: **at-least-once with a dedup window**, not exactly-once. A duplicate
    within the record's TTL replays the stored result without re-executing; one that
    arrives after the TTL re-executes — so ``IdempotencySpec.ttl`` must be at least the
    operation's maximum retry / redelivery horizon. A co-located store
    (:attr:`commits_in_transaction`) closes the crash window; an out-of-transaction store
    (the Redis / mock adapters) leaves a crash between the business commit and
    :meth:`commit` uncached, whose retry re-executes (see :meth:`commit`).

    Guarantee: **a claim reclaimed by someone else cannot be completed or released by its
    previous holder — where the store carries an owner.** ``op`` / key / ``payload_hash``
    are identical across duplicates of one request, so an operation that overruns its
    window and finds the key reclaimed would otherwise overwrite the reclaimer's live
    claim. Every shipped store fences on the invocation that took the claim
    (:class:`~forze.application.contracts.idempotency.ClaimOwnerMixin`), which the wiring
    supplies; the condition is real rather than decorative, because a store built without
    a provider, a call made outside an invocation, and a claim written before the owner
    existed all degrade to the unfenced behaviour instead of refusing work. Postgres adds
    one condition of its own — the table needs the ``owner`` column, detected at runtime —
    documented on that store.
    """

    @property
    def commits_in_transaction(self) -> bool:
        """Whether :meth:`commit` writes on the caller's *business* transaction connection.

        ``True`` for a co-located store (e.g. Postgres) whose ``commit`` runs inside the
        business transaction, so the result record and the business writes commit
        atomically — the record write is then driven from an in-transaction ``on_success``
        hook and the middleware skips its out-of-transaction commit. ``False`` (Redis, the
        mock default) keeps the out-of-transaction commit with its documented at-least-once
        crash gap.
        """
        ...  # pragma: no cover

    def begin(
        self,
        op: str,
        key: str | None,
        payload_hash: str,
    ) -> Awaitable[IdempotencyRecord | None]:
        """Claim an idempotent operation, returning a stored record if complete.

        :param op: Operation name.
        :param key: Idempotency key supplied by the boundary (``None`` skips idempotency).
        :param payload_hash: Hash of the normalized operation arguments.
        :returns: A stored :class:`IdempotencyRecord` when the operation already
            completed, else ``None`` after a fresh claim.
        :raises CoreException: ``conflict`` when the key is already bound to a different
            ``payload_hash``, and ``conflict`` when a claim for it is still in progress.
            The kind is part of the contract rather than each store's choice: it is what
            the boundary renders (409, not 400), so a client telling "you reused a key with
            different arguments" from "your request was malformed" must not get a different
            answer depending on which store the deployment wired.
        """
        ...  # pragma: no cover

    def commit(
        self,
        op: str,
        key: str | None,
        payload_hash: str,
        record: IdempotencyRecord,
    ) -> Awaitable[None]:
        """Persist the result record for a completed idempotent operation.

        Runs *outside* the business transaction: a crash between the transaction
        commit and this call leaves a committed effect with a stuck in-progress
        claim until its TTL expires (an at-least-once gap, by design).

        :raises CoreException: ``conflict`` when ``key`` is given and no claim of the
            caller's own is pending — never taken, already released, already completed, or
            (with an owner wired) reclaimed by another invocation. Failing closed is what
            lets a co-located store roll the business transaction back rather than commit
            an effect whose record went to someone else's operation. ``key=None`` skips
            idempotency entirely and never raises.
        """
        ...  # pragma: no cover

    def fail(
        self,
        op: str,
        key: str | None,
        payload_hash: str,
    ) -> Awaitable[None]:
        """Release the in-progress claim for a failed idempotent operation.

        Clears the pending claim taken by :meth:`begin` for this ``op`` /
        ``key`` / ``payload_hash``, so a legitimate retry of the failed request
        can re-execute instead of waiting for the claim TTL. A missing or
        non-matching claim is a no-op — including one reclaimed by another
        invocation, where an owner is wired: releasing that would hand a third
        duplicate permission to run alongside the operation now holding the key.

        :param op: Operation name.
        :param key: Idempotency key supplied by the boundary (``None`` skips idempotency).
        :param payload_hash: Hash of the normalized operation arguments.
        """
        ...  # pragma: no cover
