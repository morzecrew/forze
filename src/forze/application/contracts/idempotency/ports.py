"""Port for HTTP-style idempotency handling."""

from typing import Awaitable, Protocol, runtime_checkable

from .value_objects import IdempotencySnapshot

# ----------------------- #


@runtime_checkable
class IdempotencyPort(Protocol):
    """Contract for implementing idempotent request handling.

    Implementations are responsible for storing and retrieving response
    snapshots keyed by an operation identifier, optional key, and payload
    hash.
    """

    def begin(
        self,
        op: str,
        key: str | None,
        payload_hash: str,
    ) -> Awaitable[IdempotencySnapshot | None]:
        """Start an idempotent operation and return a cached snapshot if any.

        :param op: Operation name.
        :param key: Optional idempotency key provided by the caller.
        :param payload_hash: Hash of the normalized request payload.
        :returns: A previously stored :class:`IdempotencySnapshot` or ``None``.
        """
        ...  # pragma: no cover

    def commit(
        self,
        op: str,
        key: str | None,
        payload_hash: str,
        snapshot: IdempotencySnapshot,
    ) -> Awaitable[None]:
        """Persist the snapshot for the given idempotent operation."""
        ...  # pragma: no cover
