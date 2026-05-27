"""Idempotency dependency keys."""

from ..deps import ConfigurableDepPort, DepKey
from .ports import IdempotencyPort
from .specs import IdempotencySpec

# ----------------------- #

IdempotencyDepPort = ConfigurableDepPort[IdempotencySpec, IdempotencyPort]
"""Idempotency dependency port."""

IdempotencyDepKey = DepKey[IdempotencyDepPort]("idempotency")
"""Key used to register the :class:`IdempotencyDepPort` implementation."""
