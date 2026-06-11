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
    """

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
            completed, else ``None`` after a fresh claim. Raises on a payload-hash
            mismatch or an in-progress duplicate.
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
        non-matching claim is a no-op.

        :param op: Operation name.
        :param key: Idempotency key supplied by the boundary (``None`` skips idempotency).
        :param payload_hash: Hash of the normalized operation arguments.
        """
        ...  # pragma: no cover
