from typing import Final

# ----------------------- #

IDEMPOTENCY_KEY_HEADER: Final[str] = "Idempotency-Key"
"""Key of the header used for idempotent operations."""

ERROR_CODE_HEADER: Final[str] = "X-Error-Code"
"""Key of the header used for error code."""
