from typing import Mapping

import attrs

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class IdempotencySnapshot:
    """Serialized response snapshot stored for idempotent operations.

    Used to replay a previous response when a duplicate request is detected.
    """

    code: int
    """HTTP status code."""

    content_type: str
    """Response content type."""

    body: bytes
    """Response body bytes."""

    headers: Mapping[str, str] | None = attrs.field(default=None)
    """Response headers."""
