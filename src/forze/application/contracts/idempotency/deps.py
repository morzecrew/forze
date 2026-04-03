"""Idempotency dependency keys."""

from ..base import BaseDepPort, DepKey
from .ports import IdempotencyPort
from .specs import IdempotencySpec

# ----------------------- #

IdempotencyDepPort = BaseDepPort[IdempotencySpec, IdempotencyPort]
"""Idempotency dependency port."""

IdempotencyDepKey = DepKey[IdempotencyDepPort]("idempotency")
"""Key used to register the :class:`IdempotencyDepPort` implementation."""
