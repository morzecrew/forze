"""Idempotency contracts for HTTP-style request deduplication.

Provides :class:`IdempotencyPort`, :class:`IdempotencySnapshot`, and
dependency keys for building idempotency handlers.
"""

from .deps import IdempotencyDepKey, IdempotencyDepPort
from .ports import IdempotencyPort
from .types import IdempotencySnapshot

# ----------------------- #

__all__ = [
    "IdempotencyPort",
    "IdempotencyDepPort",
    "IdempotencyDepKey",
    "IdempotencySnapshot",
]
