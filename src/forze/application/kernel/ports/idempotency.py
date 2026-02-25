"""Port for HTTP-style idempotency handling."""

from typing import Optional, Protocol, TypedDict, runtime_checkable

# ----------------------- #


class IdempotencySnapshot(TypedDict):
    """Serialized response snapshot stored for idempotent operations."""

    code: int
    content_type: str
    body: bytes


# ....................... #
#! TODO: use contextvar or so for idempotency snapshot and async context manager
#! to wrap the scope


@runtime_checkable
class IdempotencyPort(Protocol):
    """Contract for implementing idempotent request handling.

    Implementations are responsible for storing and retrieving response
    snapshots keyed by an operation identifier, optional key, and payload
    hash.
    """

    async def begin(
        self,
        op: str,
        key: Optional[str],
        payload_hash: str,
    ) -> Optional[IdempotencySnapshot]:
        """Start an idempotent operation and return a cached snapshot if any.

        :param op: Operation name or route identifier.
        :param key: Optional idempotency key provided by the caller.
        :param payload_hash: Hash of the normalized request payload.
        :returns: A previously stored :class:`IdempotencySnapshot` or ``None``.
        """

    async def commit(
        self,
        op: str,
        key: Optional[str],
        payload_hash: str,
        snapshot: IdempotencySnapshot,
    ) -> None:
        """Persist the snapshot for the given idempotent operation."""
