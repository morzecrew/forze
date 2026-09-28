from ..deps import DepKey, SimpleDepPort
from .ports import AuditPort

# ----------------------- #

AuditDepPort = SimpleDepPort[AuditPort]
"""Audit dependency port: builds the :class:`AuditPort` for an execution scope."""

AuditDepKey = DepKey[AuditDepPort]("audit")
"""Key used to register the :data:`AuditDepPort` implementation."""
