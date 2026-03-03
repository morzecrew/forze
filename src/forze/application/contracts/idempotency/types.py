"""Port for HTTP-style idempotency handling."""

from typing import TypedDict

# ----------------------- #


class IdempotencySnapshot(TypedDict):
    """Serialized response snapshot stored for idempotent operations.

    Used to replay a previous response when a duplicate request is detected.
    """

    code: int
    """HTTP status code."""

    content_type: str
    """Response content type."""

    body: bytes
    """Response body bytes."""
