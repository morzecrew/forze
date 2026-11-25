"""Audit trail — a document collection and the port that writes it.

- :func:`audit_record_spec` — the collection, one :class:`AuditRecord` per audited operation.
- :class:`AuditDepsModule` — registers a :class:`DocumentAuditPort` over it, so the
  :class:`~forze.application.hooks.audit.Audited` hooks have somewhere to write.
- :func:`audit_spec_contributions` — what to merge into the application's inventory.
"""

from .inventory import audit_spec_contributions
from .port import AuditDepsModule, DocumentAuditPort
from .record import (
    DEFAULT_AUDIT_COLLECTION,
    AuditDocumentSpec,
    AuditRecord,
    audit_record_spec,
)

# ----------------------- #

__all__ = [
    "DEFAULT_AUDIT_COLLECTION",
    "AuditDepsModule",
    "AuditDocumentSpec",
    "AuditRecord",
    "DocumentAuditPort",
    "audit_record_spec",
    "audit_spec_contributions",
]
