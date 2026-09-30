"""The audit collection — one row per audited operation, and its spec factory.

The collection is the framework's, the table is the application's (the ``job_record_spec``
pattern): :func:`audit_record_spec` hands over the spec and the DDL is documented rather than
migrated for you. Wire it **tenant-aware** and the adapter injects and scopes ``tenant_id``
like every other collection. Rows are written once and never updated, so the spec has no
update command and no history.
"""

from datetime import datetime
from typing import Final
from uuid import UUID

from pydantic import Field

from forze.application.contracts.audit import AuditOutcome, AuditScalar
from forze.application.contracts.crypto import FieldEncryption
from forze.application.contracts.document import DocumentSpec, DocumentWriteTypes
from forze.base.exceptions import exc
from forze.domain.models import BaseDTO, Document, ReadDocument

# ----------------------- #

DEFAULT_AUDIT_COLLECTION: Final[str] = "audit_events"
"""Default collection name for the audit trail."""


class AuditDoc(Document):
    action: str
    outcome: AuditOutcome
    actor_id: UUID | None = None
    subject_id: UUID | None = None
    actor_ids: list[UUID] = Field(default_factory=list)
    object_type: str | None = None
    object_id: str | None = None
    metadata: dict[str, AuditScalar] = Field(default_factory=dict)
    at: datetime


class AuditCreate(BaseDTO):
    action: str
    outcome: AuditOutcome
    actor_id: UUID | None = None
    subject_id: UUID | None = None
    actor_ids: list[UUID] = Field(default_factory=list)
    object_type: str | None = None
    object_id: str | None = None
    metadata: dict[str, AuditScalar] = Field(default_factory=dict)
    at: datetime


class AuditRecord(ReadDocument):
    """One audited operation, as the trail stores it."""

    action: str
    """The audited action."""

    outcome: AuditOutcome
    """``allowed``, ``denied`` or ``failed``."""

    actor_id: UUID | None = None
    """Who performed it — the nearest actor of the chain, for a delegated call."""

    subject_id: UUID | None = None
    """On whose behalf it ran."""

    actor_ids: list[UUID] = Field(default_factory=list)
    """The whole delegation chain, nearest actor first (``actor_id`` is the first); empty for a
    direct call."""

    object_type: str | None = None
    """The kind of object it acted on, when the operation said."""

    object_id: str | None = None
    """That object's identifier."""

    metadata: dict[str, AuditScalar] = Field(default_factory=dict)
    """The declared metadata — only keys the action's allowlist names."""

    at: datetime
    """When it was recorded."""


# ....................... #

AuditDocumentSpec = DocumentSpec[AuditRecord, AuditDoc, AuditCreate, BaseDTO]
"""The audit collection's spec type (only the read model is public)."""

_QUERIED_FIELDS: Final = frozenset(
    {"action", "outcome", "actor_id", "subject_id", "actor_ids", "object_type", "object_id", "at"}
)
"""What a trail is read by: "every disclosure of this record", "everything this actor did"."""


def audit_record_spec(
    name: str = DEFAULT_AUDIT_COLLECTION,
    *,
    encryption: FieldEncryption | None = None,
) -> AuditDocumentSpec:
    """The document collection holding audit rows (wire it tenant-aware).

    ``metadata`` is the field that can carry business meaning, so *encryption* may seal it;
    the others are how the trail is queried, and sealed they would compare ciphertext and
    answer wrongly rather than fail — that is refused here.
    """

    if encryption is not None and (forbidden := encryption.sealed & _QUERIED_FIELDS):
        raise exc.configuration(
            f"audit_record_spec cannot seal {sorted(forbidden)}: the trail is queried by them, "
            "and a query over ciphertext answers wrongly rather than fails. Seal 'metadata'.",
            code="audit_record_sealed_index",
        )

    return DocumentSpec(
        name=name,
        read=AuditRecord,
        write=DocumentWriteTypes(domain=AuditDoc, create_cmd=AuditCreate),
        encryption=encryption,
    )
