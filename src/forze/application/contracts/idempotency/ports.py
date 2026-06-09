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
        """Persist the result record for a completed idempotent operation."""
        ...  # pragma: no cover
