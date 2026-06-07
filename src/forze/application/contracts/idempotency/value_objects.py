import attrs

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class IdempotencyRecord:
    """Stored result of a completed idempotent operation.

    Replayed when a duplicate request with the same idempotency key is detected.
    :attr:`result` is the operation's return value encoded by the operation's
    result codec; the boundary is responsible for any protocol-specific framing.
    """

    result: bytes
    """Serialized operation result."""
